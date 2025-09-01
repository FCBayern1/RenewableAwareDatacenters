package joshua.green.dqn;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.data.TimedCloudlet;
import joshua.green.data.read;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;

import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.power.models.PowerModelHostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;




import java.util.*;

public class rltest {
    private static final Logger logger = LoggerFactory.getLogger(rltest.class);
    private final Map<Long, Double> cloudletStartGreenEnergy = new HashMap<>();

    private static final int EPISODES = 10;
    private static final int DC_NUM = 10, HOSTS = 4, HOST_PES = 8, HOST_MIPS = 1000;
    private static final long HOST_RAM = 1024*16*16, HOST_BW = 10_000, HOST_STORAGE = 1_000_000;
    private static final int VMS = 40, VM_PES = 2, CLOUDLETS = 1000;

    private final RLClient globalRlClient = new RLClient("global");
    private final List<RLClient> localRlClients = new ArrayList<>();

    public static void main(String[] args) {
        new rltest().run();
    }

    public void run() {
        for (int episode = 0; episode < EPISODES; episode++) {
            double[] globalRewardSum = new double[1];
            globalRlClient.startEpisode();
            System.out.println("Episode " + episode);

            CloudSimPlus simulation = new CloudSimPlus(0.001);
            GlobalBrokerRL globalBroker = new GlobalBrokerRL(simulation, globalRlClient);
            List<LocalBrokerRL> brokers = new ArrayList<>();
            List<Datacenter> dcs = new ArrayList<>();

            // Create Datacenters and Brokers
            for (int i = 0; i < DC_NUM; i++) {
                Datacenter dc = createDatacenter(simulation);
                dcs.add(dc);
                RLClient localRlClient = new RLClient("local_" + i);
                localRlClients.add(localRlClient);
                List<Host> hosts = dc.getHostList();
                var broker = new LocalBrokerRL(simulation, localRlClient, hosts);
                broker.setVmDestructionDelayFunction(vm -> 5000.0);
                broker.setDatacenterMapper((last, vm) -> dc);
                brokers.add(broker);
            }
            globalBroker.addLocalBrokers(brokers);

            // Create VMs and Cloudlets
            List<Vm> vmList = createVms();
            globalBroker.setVmList(vmList);

            List<TimedCloudlet> cloudletList = read.loadTimedCloudletsFromCSV(
                    "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/data/cloudsim_cloudlets.csv",
                    CLOUDLETS);

            // Dummy cloudlet
            Cloudlet dummy = new CloudletSimple(1, 1, new UtilizationModelDynamic(0.01));
            dummy.setFileSize(1).setOutputSize(1).setId(0);
            globalBroker.submitCloudlet(dummy, 0);

            // Cloudlet schedule
            scheduleCloudlets(simulation, globalBroker, cloudletList, dcs, brokers, globalRewardSum);

            simulation.start();

            printResults(brokers);

            double totalGreenEnergy = 0.0;
            double totalEnergy = 0.0;
            for (Datacenter dc : dcs) {
                if (dc instanceof DatacenterGreenAware greenDc) {
                    totalGreenEnergy += greenDc.getTotalGreenUsed();
                    totalEnergy += greenDc.getTotalGreenUsed() + greenDc.getTotalBrownUsed();
                }
                logger.info("anchor1: Total generation for Datacenter id: {} is {}", dc.getId(), totalGreenEnergy);
            }
            logger.info("anchor2: Episode {} Energy Stats: Green Used = {}, Total Used = {}, Green Ratio = {} \n",
                    episode + 1, totalGreenEnergy, totalEnergy, (totalEnergy > 0 ? totalGreenEnergy / totalEnergy * 100.0 : 0.0));
            logger.info("anchor3: Episode {} Total Global Reward is {} \n", episode+1, globalRewardSum[0] );
//            globalRlClient.triggerTrain();

            globalRlClient.endEpisode();
            System.out.printf("Episode %d completed\n", episode + 1);
        }
    }

    private Datacenter createDatacenter(CloudSimPlus sim) {
        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < HOST_PES; i++) peList.add(new PeSimple(HOST_MIPS));
        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < HOSTS; i++) {
            Host host = new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList);
            host.setPowerModel(new PowerModelHostSimple(50, 35));
            hostList.add(host);
        }
        return new DatacenterGreenAware(sim, hostList, new VmAllocationPolicySimple(), Math.random() * 100_000);
    }

    private List<Vm> createVms() {
        List<Vm> list = new ArrayList<>();
        for (int i = 0; i < VMS; i++) {
            Vm vm = new VmSimple(HOST_MIPS, VM_PES);
            vm.setRam(1024).setBw(1000).setSize(10_000);
            list.add(vm);
        }
        return list;
    }

    private void scheduleCloudlets(CloudSimPlus sim, GlobalBrokerRL globalBroker,
                                   List<TimedCloudlet> cloudlets, List<Datacenter> dcs, List<LocalBrokerRL> brokers, double[] globalRewardSum) {
        List<TimedCloudlet> submitted = new ArrayList<>();
        Map<Long, double[]> globalStateMap = new HashMap<>();
        Map<Long, Integer> globalActionMap = new HashMap<>();
        Set<Long> finishedCloudletIds = new HashSet<>();


        sim.addOnClockTickListener(info -> {
            double now = sim.clock();

            // Submit cloudlets
            List<TimedCloudlet> toSubmit = cloudlets.stream()
                    .filter(tc -> !submitted.contains(tc) && tc.getSubmissionTime() <= now)
                    .toList();

            for (TimedCloudlet tc : toSubmit) {
                Cloudlet cl = tc.getCloudlet();
                double[] globalState = globalBroker.buildState(cl);
                int globalAction = globalRlClient.selectAction(globalState, globalBroker.getLocalBrokers().size());

                globalBroker.submitCloudlet(cl, globalAction);
                submitted.add(tc);

                long id = cl.getId();
                globalStateMap.put(id, globalState);
                globalActionMap.put(id, globalAction);

                double startGreenUsed = calculateTotalGreenEnergy(dcs);
                cloudletStartGreenEnergy.put(id, startGreenUsed);

                System.out.printf("Cloudlet %d submitted at %.2f to broker %d%n", id, now, globalAction);
            }

            // When cloudlet finishes, send experience to both global and local
            for (int i = 0; i < brokers.size(); i++) {
                LocalBrokerRL broker = brokers.get(i);
                RLClient localClient = localRlClients.get(i);

                for (Cloudlet cl : broker.getCloudletFinishedList()) {
                    long id = cl.getId();
                    if (!finishedCloudletIds.contains(id)) {
                        finishedCloudletIds.add(id);

                        // Global reward
                        double globalReward = computeGlobalReward(cl, dcs);
                        globalRewardSum[0] += globalReward;
                        double[] globalNextState = globalBroker.buildState(cl);

                        double[] globalState = globalStateMap.get(id);
                        int globalAction = globalActionMap.get(id);
                        globalRlClient.storeExperience(globalState, globalAction, globalReward, globalNextState);

                        // Local reward
                        double[] localState = broker.getStateMap().get(id);
                        int localAction = broker.getActionMap().get(id);
                        double[] localNextState = broker.buildState(cl);

                        double localReward = computeLocalCloudletReward(cl);

                        localClient.storeExperienceLocal(localState, localAction, localReward, localNextState);

                        System.out.printf("Cloudlet %d finished. GlobalReward: %.3f LocalReward: %.3f%n", id, globalReward, localReward);
                    }
                }
            }
        });
    }


    private double computeGlobalReward(Cloudlet cl, List<Datacenter> dcs) {
        double startGreen = cloudletStartGreenEnergy.getOrDefault(cl.getId(), 0.0);
        double endGreen = calculateTotalGreenEnergy(dcs);
        double reward = endGreen - startGreen;
        reward += computeLocalCloudletReward(cl);
        return reward;
    }
    
    private double computeLocalCloudletReward(Cloudlet cl) {
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

    private void printResults(List<LocalBrokerRL> brokers) {
        for (LocalBrokerRL b : brokers) {
            List<Cloudlet> finishedList = b.getCloudletFinishedList();
            System.out.println("\n Cloudlets finished for broker " + b.getId() + ":");

            new CloudletsTableBuilder(finishedList).build();

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("\nCloudlets finished for broker %d:\n", b.getId()));
            for (Cloudlet cl : finishedList) {
                sb.append(String.format("Cloudlet %d | Status: %s | Start: %.2f | Finish: %.2f | Vm: %d\n",
                        cl.getId(), cl.getStatus(), cl.getStartTime(), cl.getFinishTime(), cl.getVm().getId()));
            }

            logger.info(sb.toString());
        }
    }

    private double calculateTotalGreenEnergy(List<Datacenter> dcs) {
        double total = 0.0;
        for (Datacenter dc : dcs) {
            if (dc instanceof DatacenterGreenAware green) {
                total += green.getTotalGreenUsed();
            }
        }
        return total;
    }

}
