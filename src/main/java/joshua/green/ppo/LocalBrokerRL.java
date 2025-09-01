package joshua.green.ppo;

import joshua.green.Datacenters.DatacenterGreenAware;

import lombok.Getter;
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

    private final RLClient rlClient;
    private final List<Host> hosts = new ArrayList<>();
    @Getter
    private final Map<Long, double[]> stateMap = new HashMap<>();
    @Getter
    private final Map<Long, Integer> actionMap = new HashMap<>();
    @Getter
    private final Map<Long, Double> probLogMap = new HashMap<>();
    @Getter
    private final Map<Long, Double> valueMap = new HashMap<>();
    private final Set<Long> finishedCloudlets = new HashSet<>();

    public LocalBrokerRL(CloudSimPlus simulation, RLClient rlClient, List<Host> hosts) {
        super(simulation);
        this.rlClient = rlClient;
        this.hosts.addAll(hosts);
    }


    @Override
    public DatacenterBroker submitCloudlet(@NonNull Cloudlet cloudlet) {
        double[] state = buildState(cloudlet);
        RLClient.ActionResponse result = rlClient.selectActionLocal(state, hosts.size());
        int action = result.action;
        double LogProb = result.log_prob;
        double value = result.value;


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
        probLogMap.put(cloudletId, LogProb);
        valueMap.put(cloudletId, value);

        return super.submitCloudlet(cloudlet);
    }

    protected double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        double maxRam = hosts.stream()
                .mapToDouble(h -> h.getRam().getCapacity())
                .max().orElse(1.0);

        double maxBw = hosts.stream()
                .mapToDouble(h -> h.getBw().getCapacity())
                .max().orElse(1.0);

        for (Host host : hosts) {
            double cpuUtil = host.getCpuPercentUtilization();
            double ramFree = (host.getRam().getCapacity() - host.getRamUtilization()) / maxRam;
            double bwFree = (host.getBw().getCapacity() - host.getBwUtilization()) / maxBw;

            state.add(cpuUtil);
            state.add(ramFree);
            state.add(bwFree);
        }


        double clRamUtil = cl.getUtilizationOfRam();
        double clBwUtil = cl.getUtilizationOfBw();

        state.add(cl.getUtilizationOfCpu()); // 一般为 0~1
        state.add(clRamUtil > 1.0 ? clRamUtil / maxRam : clRamUtil);
        state.add(clBwUtil > 1.0 ? clBwUtil / maxBw : clBwUtil);

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

//    private double computeReward(Cloudlet cl) {
//        double greenEnergyRatio = 0.0;
//        double cpuUtil = 0.0;
//        double ramUtil = 0.0;
//        double bwUtil = 0.0;
//
//        if (cl.getVm() != null && cl.getVm().getHost() != null) {
//            Host host = cl.getVm().getHost();
//            Datacenter dc = host.getDatacenter();
//
//            if (dc instanceof DatacenterGreenAware greenDc) {
//                double greenUsed = greenDc.getTotalGreenUsed();
//                double totalUsed = greenDc.getTotalBrownUsed() + greenUsed;
//                greenEnergyRatio = (totalUsed > 0) ? (greenUsed / totalUsed) : 0.0;
//            }
//
//            cpuUtil = host.getCpuPercentUtilization();
//            ramUtil = (double) host.getRamUtilization() / host.getRam().getCapacity();
//            bwUtil = (double) host.getBwUtilization() / host.getBw().getCapacity();
//        }
//
//        double avgUtil = (cpuUtil + ramUtil + bwUtil) / 3.0;
//
//        double makespan = cl.getFinishTime() - cl.getStartTime();
//        makespan = Math.max(makespan, 0.1);
//
//        double reward = greenEnergyRatio * 5.0 + avgUtil * 3.0 - 0.1 * makespan;
//
//        return reward;
//    }

}
