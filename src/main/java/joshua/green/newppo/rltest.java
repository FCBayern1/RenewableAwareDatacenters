package joshua.green.newppo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import joshua.green.data.TimedCloudlet;
import joshua.green.data.new_read;
import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.SimulationConfig;
import joshua.green.RewardNormalizer;

import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;
import org.cloudsimplus.autoscaling.VerticalVmScalingSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.power.models.PowerModelHost;
import org.cloudsimplus.power.models.PowerModelHostSpec;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.resources.Ram;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class rltest {
    private static final Logger logger = LoggerFactory.getLogger(rltest.class);

    private final Map<Integer, RewardNormalizer> localRewardNormalizers = new HashMap<>();


    private final SimulationConfig config;
    private final RLClient globalRlClient;
    private final List<RLClient> localRlClients = new ArrayList<>();

    // 用于监控学习进度（选择统计仍保留）
    private final Map<Integer, List<Double>> dcSelectionHistory = new HashMap<>();

    public rltest(SimulationConfig config) {
        this.config = config;
        this.globalRlClient = new RLClient("global", config.getPythonHost(), config.getPythonPort());

        for (int i = 0; i < config.getDcNum(); i++) {
            localRewardNormalizers.put(i, new RewardNormalizer());
            dcSelectionHistory.put(i, new ArrayList<>());
        }

        new File(config.getOutputDir()).mkdirs();
        new File(config.getLogDir()).mkdirs();
    }

    public static void main(String[] args) {
        SimulationConfig config = SimulationConfig.parse(args);
        config.printConfig();

        if (config.isDryRun()) {
            logger.info("Dry run mode - exiting without running simulation");
            System.exit(0);
        }

        rltest simulation = new rltest(config);

        if (!simulation.globalRlClient.checkConnection()) {
            logger.error("Cannot connect to Python server at {}:{}",
                    config.getPythonHost(), config.getPythonPort());
            logger.error("Please ensure the Python server is running");
            System.exit(1);
        }

        simulation.globalRlClient.clearLogFile();
        simulation.run();
    }

    public void run() {
        logger.info("Starting Federated RL Workflow Scheduling Simulation - Simplified Version");
        logger.info("Configuration: {} DCs, {} Hosts/DC, {} VMs, {} Cloudlets",
                config.getDcNum(), config.getHosts(), config.getVms(), config.getCloudlets());

        long startTime = System.currentTimeMillis();

        for (int episode = 0; episode < config.getEpisodes(); episode++) {
            runEpisode(episode);
        }

        long endTime = System.currentTimeMillis();
        double totalTimeMinutes = (endTime - startTime) / 60000.0;

        logger.info("Simulation completed. Total episodes: {}, Total time: {} minutes",
                config.getEpisodes(), totalTimeMinutes);

        saveSimulationSummary(totalTimeMinutes);
    }

    private void runEpisode(int episode) {
        double[] globalRewardSum = new double[1];
        globalRlClient.startEpisode();
        logger.info("========== Episode {} Started ==========", episode);

        CloudSimPlus simulation = new CloudSimPlus(config.getSimStep());
        GlobalBrokerRL globalBroker = new GlobalBrokerRL(simulation, globalRlClient);
        globalBroker.setRewardSink(r -> globalRewardSum[0] += r);

        List<LocalBrokerRL> brokers = new ArrayList<>();
        List<Datacenter> dcs = new ArrayList<>();

        localRlClients.clear();

        // 创建数据中心
        for (int i = 0; i < config.getDcNum(); i++) {
            DatacenterGreenAware dc = createDatacenter(simulation, i,
                    config.getInitialEnergyArray()[i],
                    config.getInitialScaleFactorArray()[i]);

            dcs.add(dc);
            RLClient localRlClient = new RLClient("local_" + i, config.getPythonHost(), config.getPythonPort());
            localRlClients.add(localRlClient);

            List<Host> hosts = dc.getHostList();
            LocalBrokerRL broker = new LocalBrokerRL(simulation, localRlClient, hosts);
            broker.setVmDestructionDelayFunction(vm -> 5000.0);
            broker.setDatacenterMapper((last, vm) -> dc);
            brokers.add(broker);

            if (config.isVerbose()) {
                logger.debug("Created DC {} with {} hosts, initial: {} kWh, scaling: {}",
                        i, hosts.size(), config.getInitialEnergyArray()[i],
                        config.getInitialScaleFactorArray()[i]);
            }
        }

        globalBroker.setDatacenters(dcs);
        globalBroker.addLocalBrokers(brokers);

        // create VMs
        List<Vm> vmList = createVms();
        globalBroker.setVmList(vmList);

        // load cloudlets
        List<TimedCloudlet> cloudletList = new_read.loadTimedCloudletsFromCSV(
                config.getCloudletFile(),
                config.getCloudlets());

        Cloudlet dummy = new CloudletSimple(1, 1, new UtilizationModelDynamic(0.01));
        dummy.setFileSize(1).setOutputSize(1).setId(0);
        double[] dummyState = globalBroker.buildState(dummy);
        globalBroker.submitCloudlet(dummy, dummyState, 0, 0.0, 0.0);

        // start to schedule the cloudlets
        scheduleCloudlets(simulation, globalBroker, cloudletList, dcs, brokers, globalRewardSum);

        simulation.start();

        // stats collection
        EpisodeStats stats = collectEpisodeStats(dcs, brokers);

        // save the results
        saveEpisodeResults(episode, stats, globalRewardSum[0]);

        // send the metrics to Python
        globalRlClient.logEpisodeMetrics(
                episode + 1,
                stats.totalGreenInitial,
                stats.totalGreenUsed,
                stats.totalEnergyUsed,
                stats.greenEnergyRatio,
                stats.greenUtilizationRatio,
                globalRewardSum[0],
                stats.totalGreenResource,
                stats.totalSurplus,
                stats.makespan
        );

        globalRlClient.endEpisode();

        logger.info("========== Episode {} Completed ==========\n", episode + 1);
    }

    private void scheduleCloudlets(CloudSimPlus sim, GlobalBrokerRL globalBroker,
                                   List<TimedCloudlet> cloudlets, List<Datacenter> dcs,
                                   List<LocalBrokerRL> brokers, double[] globalRewardSum) {

        Set<TimedCloudlet> submitted = new HashSet<>();
        Set<Long> finishedCloudletIds = new HashSet<>();

        // DC selection stats
        Map<Integer, Integer> dcSelectionCount = new HashMap<>();
        for (int i = 0; i < dcs.size(); i++) {
            dcSelectionCount.put(i, 0);
        }

        sim.addOnClockTickListener(evt -> {
            double now = sim.clock();

            List<TimedCloudlet> toSubmit = cloudlets.stream()
                    .filter(tc -> !submitted.contains(tc) && tc.getSubmissionTime() <= now)
                    .toList();

            for (TimedCloudlet tc : toSubmit) {
                Cloudlet cl = tc.getCloudlet();

                // global action
                double[] globalState = globalBroker.buildState(cl);
                RLClient.ActionResponse result = globalRlClient.selectAction(
                        globalState, globalBroker.getLocalBrokers().size());

                dcSelectionCount.put(result.action, dcSelectionCount.get(result.action) + 1);

                globalBroker.submitCloudlet(
                        cl,
                        globalState,
                        result.action,
                        result.log_prob,
                        result.value
                );
                submitted.add(tc);

                if (config.isDebug()) {
                    logger.debug("Cloudlet {} submitted at {} to DC {}",
                            cl.getId(), now, result.action);
                }
            }

            for (LocalBrokerRL localBroker : brokers) {
                for (Cloudlet cl : localBroker.getCloudletFinishedList()) {
                    long id = cl.getId();
                    if (finishedCloudletIds.add(id)) {
                        if (finishedCloudletIds.size() % 100 == 0) {
                            logger.info("Progress: {}/{} tasks completed, Total global reward: {}",
                                    finishedCloudletIds.size(), cloudlets.size(), globalRewardSum[0]);
                        }
                    }
                }
            }
        });

        for (Map.Entry<Integer, Integer> entry : dcSelectionCount.entrySet()) {
            dcSelectionHistory.get(entry.getKey()).add((double) entry.getValue());
        }
    }

    private List<Vm> createVms() {
        List<Vm> list = new ArrayList<>();
        for (int i = 0; i < config.getVms(); i++) {
            var ramScaling = new VerticalVmScalingSimple(Ram.class, 0.20);
            ramScaling.setLowerThresholdFunction(vm -> 0.70);
            ramScaling.setUpperThresholdFunction(vm -> 0.90);
            Vm vm = new VmSimple(config.getHostMips(), config.getVmPes());
            vm.setRam(1024*8).setBw(1000).setSize(10_000);
            vm.setRamVerticalScaling(ramScaling);
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());

            list.add(vm);
        }
        return list;
    }

    private DatacenterGreenAware createDatacenter(CloudSimPlus sim, int dcIndex,
                                                  double initialGreenKwh, double scalingFactor) {

        double[][] serverPowerProfiles = {
                {41.6, 46.7, 52.3, 57.9, 65.4, 73.0, 80.7, 89.5, 99.6, 105, 113},
                {86, 89.4, 92.6, 96, 99.5, 105, 110, 115, 120, 125, 130},
                {93.7, 97, 101, 105, 110, 116, 121, 125, 129, 133, 135},
                {56, 59, 65, 73, 85, 101, 124, 149, 177, 205, 232}
        };

        int[] serverMips = {11700, 20000, 15000, 16000};

        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < config.getHosts(); i++) {
            int serverType = (dcIndex + i) % serverPowerProfiles.length;

            List<Pe> peList = new ArrayList<>();
            int mips = serverMips[serverType];
            for (int j = 0; j < config.getHostPes(); j++) {
                peList.add(new PeSimple(mips));
            }

            long ram = (long)(config.getHostRam() * 1024 * (2 + (serverType % 3) * 0.25));
            long bw = (long)(config.getHostBw() * (2 + (serverType % 2) * 0.5));

            Host host = new HostSimple(ram, bw, config.getHostStorage(), peList);

            PowerModelHost powerModel = new PowerModelHostSpec(serverPowerProfiles[serverType]);
            host.setPowerModel(powerModel);

            hostList.add(host);
        }

        String[] paths = config.getWindDataPath().split(",");
        String csvPath = paths[dcIndex % paths.length];

        DatacenterGreenAware dc = new DatacenterGreenAware(sim, hostList,
                new VmAllocationPolicySimple(), initialGreenKwh, csvPath);

        dc.setGenerationScalingFactor(scalingFactor);

        logger.info("Created DC {} - Initial: {} kWh, Scaling: {}, CSV: {}",
                dcIndex, initialGreenKwh, scalingFactor, csvPath);

        return dc;
    }

    private static class EpisodeStats {
        double totalGreenInitial = 0;
        double totalGreenUsed = 0;
        double totalEnergyUsed = 0;
        double totalGreenGeneration = 0;
        double totalGreenResource = 0;
        double totalSurplus = 0;
        double greenEnergyRatio = 0;
        double greenUtilizationRatio = 0;
        double makespan = 0;
    }

    private EpisodeStats collectEpisodeStats(List<Datacenter> dcs, List<LocalBrokerRL> brokers) {
        EpisodeStats stats = new EpisodeStats();

        for (Datacenter dc : dcs) {
            if (dc instanceof DatacenterGreenAware greenDc) {
                stats.totalGreenInitial += greenDc.getInitalGreenEnergy();
                stats.totalGreenUsed += greenDc.getTotalGreenUsed();
                stats.totalEnergyUsed += greenDc.getTotalGreenUsed() + greenDc.getTotalBrownUsed();
                stats.totalGreenGeneration += greenDc.getTotalGenerate();
                stats.totalSurplus += greenDc.getCumulativeSurplus();
            }
        }

        stats.totalGreenResource = stats.totalGreenInitial + stats.totalGreenGeneration;
        stats.greenEnergyRatio = stats.totalEnergyUsed > 0 ?
                (stats.totalGreenUsed / stats.totalEnergyUsed) * 100 : 0;
        stats.greenUtilizationRatio = stats.totalGreenResource > 0 ?
                (stats.totalGreenUsed / stats.totalGreenResource) * 100 : 0;

        stats.makespan = brokers.stream()
                .flatMap(b -> b.getCloudletFinishedList().stream())
                .mapToDouble(Cloudlet::getFinishTime)
                .max()
                .orElse(0);

        return stats;
    }

    private void saveEpisodeResults(int episode, EpisodeStats stats, double totalReward) {
        String filename = String.format("%s/episode_%03d_results.json",
                config.getOutputDir(), episode + 1);

        try (FileWriter writer = new FileWriter(filename)) {
            Map<String, Object> results = new HashMap<>();
            results.put("episode", episode + 1);
            results.put("totalReward", totalReward);
            results.put("stats", stats);
            results.put("timestamp", new Date().toString());

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(results, writer);

            if (config.isVerbose()) {
                logger.info("Saved episode results to {}", filename);
            }
        } catch (IOException e) {
            logger.error("Failed to save episode results", e);
        }
    }

    private void saveSimulationSummary(double totalTimeMinutes) {
        String filename = config.getOutputDir() + "/simulation_summary.txt";

        try (FileWriter writer = new FileWriter(filename)) {
            writer.write("Federated RL Green Energy Scheduling - Simulation Summary\n");
            writer.write("=".repeat(70) + "\n");
            writer.write("Total Episodes: " + config.getEpisodes() + "\n");
            writer.write("Data Centers: " + config.getDcNum() + "\n");
            writer.write("Hosts per DC: " + config.getHosts() + "\n");
            writer.write("Total VMs: " + config.getVms() + "\n");
            writer.write("Total Cloudlets: " + config.getCloudlets() + "\n");
            writer.write("Total Time: " + String.format("%.2f", totalTimeMinutes) + " minutes\n");
            writer.write("Python Server: " + config.getPythonHost() + ":" + config.getPythonPort() + "\n");
            writer.write("Completed at: " + new Date() + "\n");

            logger.info("Saved simulation summary to {}", filename);
        } catch (IOException e) {
            logger.error("Failed to save simulation summary", e);
        }
    }

}
