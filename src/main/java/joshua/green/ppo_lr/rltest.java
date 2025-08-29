package joshua.green.ppo_lr;

/**
 * Description:
 * Author: joshua
 * Date: 2025/5/1
 */

import joshua.green.data.TimedCloudlet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class rltest {
    private static final Logger logger = LoggerFactory.getLogger(rltest.class);
    private static final int EPISODES = 20;
    private static final int DC_NUM = 10, HOSTS = 4, HOST_PES = 8, HOST_MIPS = 1000;
    private static final long HOST_RAM = 1024*1024*16, HOST_BW = 10_000, HOST_STORAGE = 1_000_000;
    private static final int VMS = 40, VM_PES = 2, CLOUDLETS = 2000;

    private static final RLClient globalRlClient = new RLClient("global");
    private final List<RLClient> localRlClients = new ArrayList<>();

    private double lastGreenRatio = 0.0;


    public static void main(String[] args) {
        globalRlClient.clearLogFile();
        new rltest().run();
    }

    public void run() {
        for (int episode = 0; episode < EPISODES; episode++) {
            double[] globalRewardSum = new double[1];

//            globalRlClient.loadModel();
            globalRlClient.startEpisode();
            System.out.println("Episode " + episode);

            CloudSimPlus simulation = new CloudSimPlus(0.001);
            GlobalBrokerRL globalBroker = new GlobalBrokerRL(simulation, globalRlClient);
            List<LocalBrokerRL> brokers = new ArrayList<>();
            List<Datacenter> dcs = new ArrayList<>();
            double totalGreenInitial = 0;

            // Create Datacenters and Brokers
            for (int i = 0; i < DC_NUM; i++) {

                DatacenterGreenAware dc = (DatacenterGreenAware) createDatacenter(simulation, i);
                dc.setGenerationScalingFactor(Math.random()*i*5);
                totalGreenInitial += dc.getInitalGreenEnergy();
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

            List<TimedCloudlet> cloudletList = new_read.loadTimedCloudletsFromCSV(
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

            double totalGreenEnergyConsumption = 0.0;
            double totalEnergyConsumption = 0.0;
            double totalGreenGeneration = 0.0;
            double totalSurplus = 0.0;

            for (Datacenter dc : dcs) {
                if (dc instanceof DatacenterGreenAware greenDc) {
                    totalGreenEnergyConsumption += greenDc.getTotalGreenUsed();
                    totalEnergyConsumption += greenDc.getTotalGreenUsed() + greenDc.getTotalBrownUsed();
                    totalGreenGeneration += greenDc.getGeneration();
                    totalSurplus += greenDc.getCumulativeSurplus();
                }
            }
            logger.info("anchor1: Total green generation is {} and total intial is {}, SO TOTAL GREEN ENERGY IS {}", totalGreenGeneration, totalGreenInitial, totalGreenGeneration+totalGreenInitial);
            logger.info("anchor2: Episode {} Energy Stats: Green Used = {}, Total Used = {}, Green Ratio = {} and GreenUtil Ratio is {} \n",
                    episode + 1, totalGreenEnergyConsumption, totalEnergyConsumption,(totalEnergyConsumption >0? (totalGreenEnergyConsumption / totalEnergyConsumption)*100: 0 ), ((totalGreenGeneration+totalGreenInitial) > 0 ? totalGreenEnergyConsumption / (totalGreenGeneration+totalGreenInitial) * 100.0 : 0.0));
            logger.info("anchor3: Episode {} Total Global Reward is {} \n", episode+1, globalRewardSum[0] );
            double makespan = brokers.stream()
                    .flatMap(b -> b.getCloudletFinishedList().stream())
                    .mapToDouble(Cloudlet::getFinishTime)
                    .max()
                    .orElse(0);

            globalRlClient.logEpisodeMetrics(
                    episode + 1,
                    totalGreenInitial,
                    totalGreenEnergyConsumption,
                    totalEnergyConsumption,
                    ((totalGreenGeneration+totalGreenInitial) > 0 ? totalGreenEnergyConsumption / (totalGreenGeneration+totalGreenInitial) * 100.0 : 0.0),
                    (totalEnergyConsumption > 0? (totalGreenEnergyConsumption / totalEnergyConsumption)*100: 0 ),
                    globalRewardSum[0],
                    totalGreenGeneration+totalGreenInitial,
                    totalSurplus,
                    makespan
            );

            globalRlClient.endEpisode();
//            globalRlClient.triggerTrain();
            System.out.printf("Episode %d completed\n", episode + 1);
        }
    }


    private Datacenter createDatacenter(CloudSimPlus sim, int dcIndex) {
        List<Pe> peList = new ArrayList<>();
        int mips = HOST_MIPS + dcIndex * 100; //
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(mips));
        }

        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < HOSTS; i++) {
            long ram = (long)(HOST_RAM * (1 + (dcIndex % 3) * 0.25));
            long bw = (long)(HOST_BW * (1 + (dcIndex % 2) * 0.5));
            long storage = HOST_STORAGE;

            Host host = new HostSimple(ram, bw, storage, peList);

            double maxPower = 50 + dcIndex * 5;
            double staticPower = 35 + dcIndex * 2;
            host.setPowerModel(new PowerModelHostSimple(maxPower, staticPower));

            hostList.add(host);
        }

        double initialGreen = Math.random() * 100_000;

        return new DatacenterGreenAware(sim, hostList, new VmAllocationPolicySimple(), initialGreen);
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
        Map<Long, Double> cloudletStartGreenEnergy = new HashMap<>(); //  green energy sum of all dcs when tc was executed
        Map<Long, Double> cloudletStartDCGreenEnergy = new HashMap<>(); // green energy of the dcs which this tc is on when the tc was executed

        Map<Long, double[]> globalStateMap = new HashMap<>();
        Map<Long, Integer> globalActionMap = new HashMap<>();
        Map<Long, Double> globalLogProb = new HashMap<>();
        Map<Long, Double> globalValue = new HashMap<>();
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
                RLClient.ActionResponse result = globalRlClient.selectAction(globalState, globalBroker.getLocalBrokers().size());
                int globalAction = result.action;
                double logProb = result.log_prob;
                double value = result.value;

                globalBroker.submitCloudlet(cl, globalAction);
                submitted.add(tc);

                long id = cl.getId();
                globalStateMap.put(id, globalState);
                globalActionMap.put(id, globalAction);
                globalLogProb.put(id, logProb);
                globalValue.put(id, value);

                double startGreenUsed = calculateTotalGreenEnergy(dcs);
                if (dcs.get(globalAction) instanceof DatacenterGreenAware green) {
                    cloudletStartDCGreenEnergy.put(id, green.getGreenEnergy());
                }

                cloudletStartGreenEnergy.put(id, startGreenUsed);

                System.out.printf("Cloudlet %d submitted at %.2f to broker %d%n", id, now, globalAction);
            }

            // When cloudlet finishes, send experience to both global and local
            for (int i = 0; i < brokers.size(); i++) {

                LocalBrokerRL localBroker = brokers.get(i);
                RLClient localClient = localRlClients.get(i);

                for (Cloudlet cl : localBroker.getCloudletFinishedList()) {
                    long id = cl.getId();
                    if (!finishedCloudletIds.contains(id)) {
                        finishedCloudletIds.add(id);

                        // Global reward
                        double globalReward = computeGlobalReward(cl);
                        globalRewardSum[0] += globalReward;
                        double[] globalNextState = globalBroker.buildState(cl);

                        double[] globalState = globalStateMap.get(id);
                        int globalAction = globalActionMap.get(id);
                        boolean isDone = submitted.size() >= cloudlets.size();
                        double logProb = globalLogProb.get(id);
                        double value = globalValue.get(id);
                        globalRlClient.storeExperience(globalState, globalAction, globalReward, globalNextState, isDone, logProb, value);

                        // Local reward
                        double[] localState = localBroker.getStateMap().get(id);
                        int localAction = localBroker.getActionMap().get(id);
                        double[] localNextState = localBroker.buildState(cl);
                        double localProb = localBroker.getProbLogMap().get(id);
                        double localValue = localBroker.getValueMap().get(id);

                        double localReward = computeLocalCloudletReward(cl);

                        localClient.storeExperienceLocal(localState, localAction, localReward, localNextState, isDone, localProb, localValue);

                        System.out.printf("Cloudlet %d finished. GlobalReward: %.3f LocalReward: %.3f%n", id, globalReward, localReward);
                    }
                }
            }
        });
    }


    private double computeGlobalReward(Cloudlet cl) {
        if (cl.getVm() == null || cl.getVm().getHost() == null) return 0.0;

        double totalGreen = 0.0, totalEnergy = 0.0;
        for (Datacenter dc : cl.getVm().getHost().getSimulation().getCis().getDatacenterList()) {
            if (dc instanceof DatacenterGreenAware greenDc) {
                totalGreen += greenDc.getTotalGreenUsed();
                totalEnergy += greenDc.getTotalGreenUsed() + greenDc.getTotalBrownUsed();
            }
        }

        double currentRatio = (totalEnergy > 0) ? totalGreen / totalEnergy : 0.0;
        double delta = currentRatio - lastGreenRatio;
        lastGreenRatio = currentRatio;

        // 稳定 reward 设计
        double rewardGrowth = Math.max(0.0, delta * 10);       // 成长激励
        double rewardLevel = currentRatio;                     // 当前水平激励

        return 0.5 * rewardGrowth + 0.5 * rewardLevel;
    }


    private double computeLocalCloudletReward(Cloudlet cl) {
        if (cl.getVm() == null || cl.getVm().getHost() == null) return 0.0;

        Host host = cl.getVm().getHost();

        double makespan = cl.getFinishTime() - cl.getStartTime();
        double execTime = cl.getFinishTime() - cl.getStartWaitTime();
        makespan = Math.max(makespan, 0.1);
        execTime = Math.max(execTime, 0.1);

        double util = host.getCpuPercentUtilization();
        double power = host.getPowerModel().getPower(util);
        double energy = execTime * power;

        // === Normalization（落在 0~1 之间）===
        double normMakespan = Math.min(makespan / 2000.0, 1.0); // 2000s cap
        double normEnergy = Math.min(energy / 5000.0, 1.0);     // 5000J cap

        double alpha = 0.5;

        return 1.0 - (alpha * normMakespan + (1 - alpha) * normEnergy);
    }



//    private double computeLocalCloudletReward(Cloudlet cl) {
//        if (cl.getVm() == null || cl.getVm().getHost() == null) return 0.0;
//
//        Host host = cl.getVm().getHost();
//        Datacenter dc = host.getDatacenter();
//
//        double makespan = cl.getFinishTime() - cl.getStartTime();
//        double execTime = cl.getFinishTime() - cl.getStartWaitTime();
//        makespan = Math.max(makespan, 0.1);
//
//        double util = host.getCpuPercentUtilization();
//        double energy = execTime * host.getPowerModel().getPower(util);
//
//        double greenRatio = 0.0;
//        if (dc instanceof DatacenterGreenAware greenDc) {
//            double green = greenDc.getTotalGreenUsed();
//            double brown = greenDc.getTotalBrownUsed();
//            greenRatio = (green + brown > 0) ? green / (green + brown) : 0.0;
//        }
//
//        // Coefficients (can be tuned)
//        double w_makespan = -0.001;
//        double w_energy = -0.0002;
//        double w_green_ratio = 1.0;
//
//        return w_makespan * makespan + w_energy * energy + w_green_ratio * greenRatio;
//    }



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

    //    private void distributeCloudlets(CloudSimPlus sim, GlobalBrokerRL globalBroker,
//                                     List<TimedCloudlet> cloudlets, List<Datacenter> dcs, List<LocalBrokerRL> brokers) {
//    List<TimedCloudlet> submitted = new ArrayList<>();
//
//    sim.addOnClockTickListener(info -> {
//        double now = sim.clock();
//        List<TimedCloudlet> toSubmit = cloudlets.stream().filter(tc -> !submitted.contains(tc) && tc.getSubmissionTime() <= now).toList();
//        for (TimedCloudlet tc: toSubmit) {
//            Cloudlet cl = tc.getCloudlet();
//            long id = cl.getId();
//
//            double[] state = globalBroker.buildState(cl);
//            RLClient.ActionResponse result = globalRlClient.selectAction(state,dcs.size());
//            int action = result.action;
//            double logProb = result.log_prob;
//            double value = result.value;
//
//            globalBroker.submitCloudlet(cl, action);
//            submitted.add(tc);
//
//            globalStateMap.put(id, state);
//            globalActionMap.put(id, action);
//            globalLogProb.put(id, logProb);
//            globalValue.put(id, value);
//
//            double startGreenUsed = calculateTotalGreenEnergy(dcs);
//            if (dcs.get(action) instanceof DatacenterGreenAware green) {
//                cloudletStartDCGreenEnergy.put(id, green.getGreenEnergy());
//            }
//
//            cloudletStartGreenEnergy.put(id, startGreenUsed);
//            System.out.printf("Cloudlet %d submitted at %.2f to broker %d%n", id, now, action);
//
//        }
//    });
//    }
//
    // summarise the finish states

}
