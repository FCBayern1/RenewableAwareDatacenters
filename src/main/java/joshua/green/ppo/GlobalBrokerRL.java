package joshua.green.ppo;

import joshua.green.Datacenters.DatacenterGreenAware;

import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimEntity;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.core.events.SimEvent;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


/**
 * Global Broker using Reinforcement Learning to distribute Cloudlets to Local Brokers.
 */
public class GlobalBrokerRL extends CloudSimEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalBrokerRL.class);

    private final List<LocalBrokerRL> localBrokers = new ArrayList<>();
    private final RLClient rlClient;
    private final Queue<Cloudlet> cloudletQueue = new LinkedList<>();
    private final List<Vm> vmList = new ArrayList<>();

    public GlobalBrokerRL(CloudSimPlus simulation, RLClient rlClient) {
        super(simulation);
        this.rlClient = rlClient;
    }

    public void addLocalBroker(LocalBrokerRL broker) {
        localBrokers.add(broker);
    }

    public void addLocalBrokers(List<LocalBrokerRL> brokers) {
        localBrokers.addAll(brokers);
    }

    public List<LocalBrokerRL> getLocalBrokers() {
        return localBrokers;
    }

    public void setVmList(List<Vm> list) {
        vmList.addAll(list);
    }

    public void setCloudletList(List<Cloudlet> list) {
        cloudletQueue.addAll(list);
    }

    @Override
    protected void startInternal() {
        LOGGER.info("{} starting...", getName());
        distributeVMs();
//        scheduleTickDrivenSubmission();
    }

    private void distributeVMs() {
        int count = localBrokers.size();
        for (int i = 0; i < vmList.size(); i++) {
            localBrokers.get(i % count).submitVm(vmList.get(i));
        }
        LOGGER.info("Distributed {} VMs to {} Local Brokers", vmList.size(), count);
    }

//    public void scheduleTickDrivenSubmission() {
//        getSimulation().addOnClockTickListener(info -> {
//            if (cloudletQueue.isEmpty()) {
//                return;
//            }
//
//            Cloudlet cl = cloudletQueue.poll();
//            if (cl != null) {
//                int action = chooseBestBroker(cl);
//                submitCloudlet(cl, action);
//            }
//        });
//    }

//    public int chooseBestBroker(Cloudlet cl) {
//        double[] state = buildState(cl);
//        return rlClient.selectAction(state, localBrokers.size()).action;
//    }

    public double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        double maxGreen = 1.0;
        double maxProc = 1.0;

        for (LocalBrokerRL broker : localBrokers) {
            Datacenter dc = broker.getLastSelectedDc();
            if (dc instanceof DatacenterGreenAware greenDC) {
                maxGreen = Math.max(maxGreen, greenDC.getGreenEnergy());
                maxProc = Math.max(maxProc, greenDC.getAverageProcessingAbility());
            }
        }

        for (LocalBrokerRL broker : localBrokers) {
            Datacenter dc = broker.getLastSelectedDc();
            if (dc instanceof DatacenterGreenAware greenDC) {
                state.add(greenDC.getGreenEnergy() / maxGreen); // normalised green energy
                state.add(greenDC.getAverageProcessingAbility() / maxProc); // normalised average processing ability
                state.add(greenDC.getCurrentCpuUtilization());
                state.add(greenDC.getRamUtilization());
                state.add(greenDC.getBwUtilization());
            } else {
                for (int i = 0; i < 5; i++) {
                    state.add(0.0);
                }
            }
        }

        state.add(cl.getUtilizationOfCpu());
        state.add(cl.getUtilizationOfRam());
        state.add(cl.getUtilizationOfBw());

        return state.stream().mapToDouble(Double::doubleValue).toArray();
    }


    public void submitCloudlet(Cloudlet cl, int action) {
        if (action < 0 || action >= localBrokers.size()) {
            LOGGER.error("Invalid Local Broker index {} for Cloudlet {}", action, cl.getId());
            return;
        }
        LocalBrokerRL broker = localBrokers.get(action);
        broker.submitCloudlet(cl);
        LOGGER.info("GlobalBrokerRL: Cloudlet {} submitted to LocalBroker {} at time {}", cl.getId(), broker.getId(), getSimulation().clock());
    }

    @Override
    public void processEvent(SimEvent simEvent) {
        // No internal events needed for now
    }
}
