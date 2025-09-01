package joshua.green.dqn;

import joshua.green.Datacenters.DatacenterGreenAware;
import lombok.NonNull;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Local Broker with Reinforcement Learning control over VM selection.
 */

public class LocalBrokerRL extends DatacenterBrokerSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalBrokerRL.class);

    public Map<Long, double[]> getStateMap() {
        return stateMap;
    }

    public Map<Long, Integer> getActionMap() {
        return actionMap;
    }

    private final RLClient rlClient;
    private final List<Host> hosts = new ArrayList<>();
    private final Map<Long, double[]> stateMap = new HashMap<>();
    private final Map<Long, Integer> actionMap = new HashMap<>();
    private final Set<Long> finishedCloudlets = new HashSet<>();

    public LocalBrokerRL(CloudSimPlus simulation, RLClient rlClient, List<Host> hosts) {
        super(simulation);
        this.rlClient = rlClient;
        this.hosts.addAll(hosts);
//        setupCloudletFinishListener();
    }

    private void setupCloudletFinishListener() {
        getSimulation().addOnClockTickListener(info -> {
            for (Cloudlet cl : getCloudletFinishedList()) {
                long id = cl.getId();
                if (!finishedCloudlets.contains(id)) {
                    finishedCloudlets.add(id);

                    double[] state = stateMap.get(id);
                    int action = actionMap.get(id);

                    double reward = computeReward(cl);
                    double[] nextState = buildState(cl);

                    rlClient.storeExperienceLocal(state, action, reward, nextState);
                    LOGGER.info("LocalBrokerRL: Cloudlet {} finished. Reward: {}", id, reward);
                }
            }
        });
    }

    @Override
    public DatacenterBroker submitCloudlet(@NonNull Cloudlet cloudlet) {
        double[] state = buildState(cloudlet);
        int action = rlClient.selectActionLocal(state, hosts.size());

        Host selectedHost = hosts.get(action);
        if (selectedHost != null) {
            Vm vm = findBestVm(selectedHost);
            if (vm != null) {
                cloudlet.setVm(vm);
            }
        }

        long cloudletId = cloudlet.getId();
        stateMap.put(cloudletId, state);
        actionMap.put(cloudletId, action);

        return super.submitCloudlet(cloudlet);
    }

    public double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();
        for (Host host : hosts) {
            state.add(host.getCpuPercentUtilization());
            state.add((double) (host.getRam().getCapacity() - host.getRamUtilization()));
            state.add((double) (host.getBw().getCapacity() - host.getBwUtilization()));
        }
        state.add(cl.getUtilizationOfCpu());
        state.add(cl.getUtilizationOfRam());
        state.add(cl.getUtilizationOfBw());
        return state.stream().mapToDouble(Double::doubleValue).toArray();
    }

    private Vm findBestVm(Host host) {
        for (Vm vm : getVmCreatedList()) {
            if (vm.getHost().equals(host) && vm.isIdle()) {
                return vm;
            }
        }
        return null;
    }

    private double computeReward(Cloudlet cl) {
        double greenEnergyRatio = 0.0;
        double cpuUtil = 0.0;
        double ramUtil = 0.0;
        double bwUtil = 0.0;

        if (cl.getVm() != null && cl.getVm().getHost() != null) {
            Host host = cl.getVm().getHost();
            Datacenter dc = host.getDatacenter();

            if (dc instanceof DatacenterGreenAware greenDc) {
                double greenUsed = greenDc.getTotalGreenUsed();
                double totalUsed = greenDc.getTotalBrownUsed() + greenUsed;
                greenEnergyRatio = (totalUsed > 0) ? (greenUsed / totalUsed) : 0.0;
            }

            cpuUtil = host.getCpuPercentUtilization();
            ramUtil = (double) host.getRamUtilization() / host.getRam().getCapacity();
            bwUtil = (double) host.getBwUtilization() / host.getBw().getCapacity();
        }

        double avgUtil = (cpuUtil + ramUtil + bwUtil) / 3.0;

        double makespan = cl.getFinishTime() - cl.getStartTime();
        makespan = Math.max(makespan, 0.1);

        double reward = greenEnergyRatio * 5.0 + avgUtil * 3.0 - 0.1 * makespan;

        return reward;
    }

}
