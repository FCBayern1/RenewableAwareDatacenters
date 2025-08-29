package joshua.green.random;

import joshua.green.data.TimedCloudlet;
import joshua.green.data.new_read;
import joshua.green.Datacenters.DatacenterGreenAware;

import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimEntity;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.core.events.SimEvent;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Global Broker using Random Scheduling to distribute Cloudlets to Local Brokers,
 * with support for time-driven Cloudlet submissions.
 */
public class GlobalBrokerRandom extends CloudSimEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalBrokerRandom.class);

    private final List<DatacenterBrokerSimple> localBrokers = new ArrayList<>();
    private final Queue<TimedCloudlet> cloudletQueue = new LinkedList<>();
    private final List<TimedCloudlet> submittedCloudlets = new ArrayList<>();
    private final List<Vm> vmList = new ArrayList<>();
    private final Random random = new Random();

    public GlobalBrokerRandom(CloudSimPlus simulation) {
        super(simulation);
    }

    public void addLocalBroker(DatacenterBrokerSimple broker) {
        localBrokers.add(broker);
    }

    public void addLocalBrokers(List<DatacenterBrokerSimple> brokers) {
        localBrokers.addAll(brokers);
    }

    public List<DatacenterBrokerSimple> getLocalBrokers() {
        return localBrokers;
    }

    public void setVmList(List<Vm> list) {
        vmList.addAll(list);
    }

    public void setCloudletList(List<TimedCloudlet> list) {
        cloudletQueue.addAll(list);
    }

    @Override
    protected void startInternal() {
        LOGGER.info("{} starting...", getName());
        distributeVMs();

        // Register tick listener for time-driven Cloudlet submissions
        getSimulation().addOnClockTickListener(info -> {
            if (cloudletQueue.isEmpty()) return;

            double now = getSimulation().clock();
            List<TimedCloudlet> toSubmit = new ArrayList<>();

            for (TimedCloudlet cl : cloudletQueue) {
                if (cl.getSubmissionTime() <= now && !submittedCloudlets.contains(cl)) {
                    toSubmit.add(cl);
                }
            }

            for (TimedCloudlet cl : toSubmit) {
                submitCloudlet(cl.getCloudlet());
                submittedCloudlets.add(cl);
                cloudletQueue.remove(cl);
            }
        });
    }

    private void distributeVMs() {
        int count = localBrokers.size();
        for (int i = 0; i < vmList.size(); i++) {
            localBrokers.get(i % count).submitVm(vmList.get(i));
        }
        LOGGER.info("Distributed {} VMs to {} Local Brokers", vmList.size(), count);
    }

    public void submitCloudlet(Cloudlet cl) {
        if (localBrokers.isEmpty()) {
            LOGGER.error("No Local Brokers available to submit Cloudlet {}", cl.getId());
            return;
        }

        int randomIndex = random.nextInt(localBrokers.size());
        DatacenterBrokerSimple broker = localBrokers.get(randomIndex);

        broker.submitCloudlet(cl);
        LOGGER.info("GlobalBrokerRandom: Cloudlet {} submitted to LocalBroker {} at time {}",
                cl.getId(), broker.getId(), getSimulation().clock());
    }

    @Override
    public void processEvent(SimEvent simEvent) {
        // No internal events
    }
}
