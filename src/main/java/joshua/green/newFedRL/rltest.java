package joshua.green.newFedRL;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.SimulationConfig;
import joshua.green.RewardNormalizer;

import joshua.green.data.TimedCloudlet;
import joshua.green.data.new_read;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;
import org.cloudsimplus.autoscaling.VerticalVmScalingSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.power.models.PowerModel;
import org.cloudsimplus.power.models.PowerModelHost;
import org.cloudsimplus.power.models.PowerModelHostSimple;
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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Federated RL Test with Proper Episode Tracking
 * Author: joshua
 * Date: 2025/7/31
 */
public class rltest {
    private static final Logger logger = LoggerFactory.getLogger(rltest.class);

    private final Map<Integer, RewardNormalizer> localRewardNormalizers = new HashMap<>();
    private final SimulationConfig config;
    private final RLClient globalRlClient;
    private final List<RLClient> localRlClients = new ArrayList<>();
    private final Map<Integer, List<Double>> dcSelectionHistory = new HashMap<>();

    // Track cloudlet distribution per broker
    private final Map<Integer, Integer> cloudletsPerBroker = new HashMap<>();

    public rltest(SimulationConfig config) {
        this.config = config;
        this.globalRlClient = new RLClient("global", config.getPythonHost(), config.getPythonPort());

        for (int i = 0; i < config.getDcNum(); i++) {
            localRewardNormalizers.put(i, new RewardNormalizer());
            dcSelectionHistory.put(i, new ArrayList<>());
            cloudletsPerBroker.put(i, 0);
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
        logger.info("Starting Federated RL Workflow Scheduling Simulation");
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

        // Start episode on Python side - resets LSTM states
        globalRlClient.startEpisode();
        logger.info("========== Episode {} Started ==========", episode + 1);

        CloudSimPlus simulation = new CloudSimPlus(config.getSimStep());
        GlobalBrokerRL globalBroker = new GlobalBrokerRL(simulation, globalRlClient);
        globalBroker.setRewardSink(r -> globalRewardSum[0] += r);

        // Reset episode tracking
        globalBroker.resetEpisodeTracking();

        List<LocalBrokerRL> brokers = new ArrayList<>();
        List<Datacenter> dcs = new ArrayList<>();

        localRlClients.clear();

        // Reset cloudlet distribution tracking
        for (int i = 0; i < config.getDcNum(); i++) {
            cloudletsPerBroker.put(i, 0);
        }

        // Create datacenters and brokers
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

            // Reset episode tracking for local broker
            broker.resetEpisodeTracking();

            brokers.add(broker);

            if (config.isVerbose()) {
                logger.debug("Created DC {} with {} hosts, initial: {} kWh, scaling: {}",
                        i, hosts.size(), config.getInitialEnergyArray()[i],
                        config.getInitialScaleFactorArray()[i]);
            }
        }

        globalBroker.setDatacenters(dcs);
        globalBroker.addLocalBrokers(brokers);

        List<Vm> vmList = createVms();
        globalBroker.setVmList(vmList);

        // Load cloudlets
        List<TimedCloudlet> cloudletList = new_read.loadTimedCloudletsFromCSV(
                config.getCloudletFile(),
                config.getCloudlets());

        // ============= CRITICAL: Set total cloudlets for episode tracking =============
        int totalCloudletsCount = cloudletList.size() + 1; // +1 for dummy cloudlet
        globalBroker.setTotalCloudlets(totalCloudletsCount);

        logger.info("Episode {}: Total cloudlets to process = {} (including dummy)",
                episode + 1, totalCloudletsCount);

        // Submit dummy cloudlet
        Cloudlet dummy = new CloudletSimple(1, 1, new UtilizationModelDynamic(0.01));
        dummy.setFileSize(1).setOutputSize(1).setId(0);
        double[] dummyState = globalBroker.buildState(dummy);
        globalBroker.submitCloudlet(dummy, dummyState, 0, 0.0, 0.0);

        // Track dummy cloudlet for broker 0
        cloudletsPerBroker.put(0, cloudletsPerBroker.get(0) + 1);

        // Schedule cloudlets with proper tracking
        scheduleCloudletsWithTracking(simulation, globalBroker, cloudletList, dcs, brokers,
                globalRewardSum, totalCloudletsCount);

        // Start simulation
        simulation.start();

        // ============= Signal episode end after simulation completes =============
        logger.info("Simulation completed for episode {}, signaling episode end to all brokers",
                episode + 1);
        globalBroker.signalEpisodeEnd();
        for (LocalBrokerRL broker : brokers) {
            broker.signalEpisodeEnd();
        }

        // Store DC selection history after simulation completes
        for (int i = 0; i < config.getDcNum(); i++) {
            if (cloudletsPerBroker.containsKey(i)) {
                dcSelectionHistory.get(i).add((double) cloudletsPerBroker.get(i));
            }
        }

        // Collect episode statistics
        EpisodeStats stats = collectEpisodeStats(dcs, brokers);

        // Save episode results
        saveEpisodeResults(episode, stats, globalRewardSum[0]);

        // Log episode metrics to Python server
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

        // End episode on Python side - triggers learning and aggregation
        globalRlClient.endEpisode();

        logger.info("Episode {}: Total Reward = {}, Green Ratio = {}%, Makespan = {}s",
                episode + 1, globalRewardSum[0], stats.greenEnergyRatio, stats.makespan);
        logger.info("========== Episode {} Completed ==========\n", episode + 1);
    }

    private void scheduleCloudletsWithTracking(CloudSimPlus sim, GlobalBrokerRL globalBroker,
                                               List<TimedCloudlet> cloudlets, List<Datacenter> dcs,
                                               List<LocalBrokerRL> brokers, double[] globalRewardSum,
                                               int totalCloudletsCount) {

        Set<TimedCloudlet> submitted = new HashSet<>();
        Set<Long> finishedCloudletIds = new HashSet<>();

        // DC selection stats
        Map<Integer, Integer> dcSelectionCount = new HashMap<>();
        for (int i = 0; i < dcs.size(); i++) {
            dcSelectionCount.put(i, 0);
        }

        // Track when all cloudlets are submitted and finished
        final boolean[] allSubmitted = {false};
        final boolean[] episodeEndSignaled = {false};

        // Pre-scan cloudlets to estimate distribution (optional, for better local broker tracking)
        // This is a simple round-robin estimation
        int estimatedPerBroker = (cloudlets.size() / brokers.size()) + 1;
        for (int i = 0; i < brokers.size(); i++) {
            brokers.get(i).setTotalCloudlets(estimatedPerBroker);
            logger.debug("LocalBroker {} expects approximately {} cloudlets", i, estimatedPerBroker);
        }

        sim.addOnClockTickListener(evt -> {
            double now = sim.clock();

            // Submit cloudlets whose time has come
            List<TimedCloudlet> toSubmit = cloudlets.stream()
                    .filter(tc -> !submitted.contains(tc) && tc.getSubmissionTime() <= now)
                    .toList();

            for (TimedCloudlet tc : toSubmit) {
                Cloudlet cl = tc.getCloudlet();

                // Global action selection
                double[] globalState = globalBroker.buildState(cl);
                RLClient.ActionResponse result = globalRlClient.selectAction(
                        globalState, globalBroker.getLocalBrokers().size());

                // Track DC selection
                dcSelectionCount.put(result.action, dcSelectionCount.get(result.action) + 1);

                // Track cloudlets per broker for accurate counting
                cloudletsPerBroker.put(result.action, cloudletsPerBroker.get(result.action) + 1);

                // Submit with full information for PPO
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

            // Check if all cloudlets have been submitted
            if (!allSubmitted[0] && submitted.size() >= cloudlets.size()) {
                allSubmitted[0] = true;
                logger.info("All {} cloudlets have been submitted", cloudlets.size());

                // Update local broker expected counts based on actual distribution
                for (int i = 0; i < brokers.size(); i++) {
                    int actualCount = cloudletsPerBroker.get(i);
                    brokers.get(i).setTotalCloudlets(actualCount);
                    logger.debug("LocalBroker {} updated to expect {} cloudlets", i, actualCount);
                }
            }

            // Track finished cloudlets
            int totalFinished = 0;
            for (LocalBrokerRL localBroker : brokers) {
                for (Cloudlet cl : localBroker.getCloudletFinishedList()) {
                    long id = cl.getId();
                    if (finishedCloudletIds.add(id)) {
                        // New cloudlet finished
                    }
                }
                totalFinished = finishedCloudletIds.size();
            }

            // Progress logging
            if (totalFinished > 0 && (totalFinished % 100 == 0 || totalFinished == totalCloudletsCount)) {
                logger.info("Progress: {}/{} tasks completed, Total global reward: {}",
                        totalFinished, totalCloudletsCount, globalRewardSum[0]);
            }

            // Check if all cloudlets are finished and signal episode end
            if (!episodeEndSignaled[0] && totalFinished >= totalCloudletsCount) {
                episodeEndSignaled[0] = true;
                logger.info("All {} cloudlets finished, signaling episode complete", totalCloudletsCount);

                // Signal episode end to all brokers
                globalBroker.signalEpisodeEnd();
                for (LocalBrokerRL broker : brokers) {
                    broker.signalEpisodeEnd();
                }

                // Optionally terminate simulation early if all work is done
                if (allSubmitted[0]) {
                    sim.terminate();
                    logger.info("Terminating simulation as all cloudlets are processed");
                }
            }
        });

        // Store DC selection history for analysis
        sim.addOnSimulationStartListener(evt -> {
            logger.debug("Simulation started, tracking DC selections");
        });
    }

    // Create VMs
    private List<Vm> createVms() {
        List<Vm> list = new ArrayList<>();
        for (int i = 0; i < config.getVms(); i++) {
            var ramScaling = new VerticalVmScalingSimple(Ram.class, 0.20);
            ramScaling.setLowerThresholdFunction(vm -> 0.70);
            ramScaling.setUpperThresholdFunction(vm -> 0.90);

            Vm vm = new VmSimple(config.getHostMips(), config.getVmPes());
            vm.setRam(1024 * 8).setBw(1000).setSize(10_000);
            vm.setRamVerticalScaling(ramScaling);
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());

            list.add(vm);
        }
        return list;
    }

    // Create Datacenters
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

    // Episode statistics collection
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

        // Calculate makespan
        stats.makespan = brokers.stream()
                .flatMap(b -> b.getCloudletFinishedList().stream())
                .mapToDouble(Cloudlet::getFinishTime)
                .max()
                .orElse(0);

        // Log broker statistics
        logger.debug("Episode Stats Collection:");
        for (int i = 0; i < brokers.size(); i++) {
            LocalBrokerRL broker = brokers.get(i);
            int finished = broker.getCloudletFinishedList().size();
            int submitted = broker.getCloudletSubmittedList().size();
            logger.debug("  Broker {}: Submitted={}, Finished={}", i, submitted, finished);
        }

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
            results.put("dcSelectionHistory", dcSelectionHistory);
            results.put("cloudletsPerBroker", cloudletsPerBroker);
            results.put("timestamp", new Date().toString());
            results.put("config", Map.of(
                    "dcNum", config.getDcNum(),
                    "hosts", config.getHosts(),
                    "vms", config.getVms(),
                    "cloudlets", config.getCloudlets()
            ));

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

            // Add DC selection summary
            writer.write("\nDC Selection Summary:\n");
            for (int i = 0; i < config.getDcNum(); i++) {
                List<Double> history = dcSelectionHistory.get(i);
                if (!history.isEmpty()) {
                    double total = history.stream().mapToDouble(Double::doubleValue).sum();
                    double avg = total / history.size();
                    writer.write(String.format("  DC %d: Total=%.0f, Avg per episode=%.2f\n",
                            i, total, avg));
                }
            }

            logger.info("Saved simulation summary to {}", filename);
        } catch (IOException e) {
            logger.error("Failed to save simulation summary", e);
        }
    }
}