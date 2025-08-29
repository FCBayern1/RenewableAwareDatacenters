package joshua.green.FedRL;

import joshua.green.data.TimedCloudlet;
import joshua.green.ppo_lr.new_read;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import joshua.green.FedRL.energy.RenewableEnergyGenerator;
import joshua.green.FedRL.energy.SolarEnergyLoader;
import joshua.green.FedRL.energy.WindEnergyLoader;

public class rltest {
    private static final Logger logger = LoggerFactory.getLogger(rltest.class);

    private final SimulationConfig config;
    private final RLClient globalRlClient;
    private final List<RLClient> localRlClients = new ArrayList<>();

    private Map<Long, Double> cloudletStartGreenRatio = new HashMap<>();
    private Map<Long, Double> cloudletStartTime = new HashMap<>();
    private double episodeStartGreenRatio = 0.0;

    public rltest(SimulationConfig config) {
        this.config = config;
        this.globalRlClient = new RLClient("global", config.getPythonHost(), config.getPythonPort());

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

        // 记录开始时间
        long startTime = System.currentTimeMillis();

        for (int episode = 0; episode < config.getEpisodes(); episode++) {
            runEpisode(episode);
        }

        long endTime = System.currentTimeMillis();
        double totalTimeMinutes = (endTime - startTime) / 60000.0;

        logger.info("Simulation completed. Total episodes: {}, Total time: {:.2f} minutes",
                config.getEpisodes(), totalTimeMinutes);

        // 保存最终统计信息
        saveSimulationSummary(totalTimeMinutes);
    }

    private void runEpisode(int episode) {
        double[] globalRewardSum = new double[1];

        globalRlClient.startEpisode();
        logger.info("========== Episode {} Started ==========", episode);

        CloudSimPlus simulation = new CloudSimPlus(config.getSimStep());
        GlobalBrokerRL globalBroker = new GlobalBrokerRL(simulation, globalRlClient);
        List<LocalBrokerRL> brokers = new ArrayList<>();
        List<Datacenter> dcs = new ArrayList<>();
        double totalGreenInitial = 0;

        localRlClients.clear();

        // create dcs and the local scheduler
        for (int i = 0; i < config.getDcNum(); i++) {
            DatacenterGreenAware dc = createDatacenter(simulation, i);

            // 设置不同的绿色能源生成比例
            double scalingFactor = config.getGreenFactorMin() +
                    Math.random() * (config.getGreenFactorMax() - config.getGreenFactorMin());
            dc.setGenerationScalingFactor(scalingFactor);

            totalGreenInitial += dc.getInitalGreenEnergy();
            dcs.add(dc);

            // 为每个数据中心创建本地RL客户端
            RLClient localRlClient = new RLClient("local_" + i,
                    config.getPythonHost(), config.getPythonPort());
            localRlClients.add(localRlClient);

            // 创建本地调度器
            List<Host> hosts = dc.getHostList();
            LocalBrokerRL broker = new LocalBrokerRL(simulation, localRlClient, hosts);
            broker.setVmDestructionDelayFunction(vm -> 5000.0);
            broker.setDatacenterMapper((last, vm) -> dc);
            brokers.add(broker);

            if (config.isVerbose()) {
                logger.debug("Created DC {} with {} hosts, scaling factor: {:.3f}",
                        i, hosts.size(), scalingFactor);
            }
        }

        globalBroker.setDatacenters(dcs);
        globalBroker.addLocalBrokers(brokers);

        List<Vm> vmList = createVms();
        globalBroker.setVmList(vmList);

        List<TimedCloudlet> cloudletList = new_read.loadTimedCloudletsFromCSV(
                "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/data/cloudsim_cloudlets.csv",
                config.getCloudlets());

        Cloudlet dummy = new CloudletSimple(1, 1, new UtilizationModelDynamic(0.01));
        dummy.setFileSize(1).setOutputSize(1).setId(0);
        globalBroker.submitCloudlet(dummy, 0);

        episodeStartGreenRatio = calculateGlobalGreenRatio(dcs);

        scheduleCloudlets(simulation, globalBroker, cloudletList, dcs, brokers, globalRewardSum);

        simulation.start();

        EpisodeStats stats = collectEpisodeStats(dcs, brokers);
        printEpisodeResults(episode, stats, globalRewardSum[0]);

        saveEpisodeResults(episode, stats, globalRewardSum[0]);

        globalRlClient.logEpisodeMetrics(
                episode + 1,
                stats.totalGreenInitial,
                stats.totalGreenUsed,
                stats.totalEnergyUsed,
                stats.greenUtilizationRatio,
                stats.greenEnergyRatio,
                globalRewardSum[0],
                stats.totalGreenResource,
                stats.totalSurplus,
                stats.makespan
        );

        globalRlClient.endEpisode();

        cloudletStartGreenRatio.clear();
        cloudletStartTime.clear();

        logger.info("========== Episode {} Completed ==========\n", episode + 1);
    }

    private DatacenterGreenAware createDatacenter(CloudSimPlus sim, int dcIndex) {
        List<Pe> peList = new ArrayList<>();
        int mips = config.getHostMips() + dcIndex * 100;
        for (int i = 0; i < config.getHostPes(); i++) {
            peList.add(new PeSimple(mips));
        }

        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < config.getHosts(); i++) {
            long ram = (long)(config.getHostRam() * 1024 * 1024 * (1 + (dcIndex % 3) * 0.25));
            long bw = (long)(config.getHostBw() * (1 + (dcIndex % 2) * 0.5));

            Host host = new HostSimple(ram, bw, config.getHostStorage(), peList);

            double maxPower = 50 + dcIndex * 5;      // 50-95W
            double staticPower = 35 + dcIndex * 2;   // 35-53W
            host.setPowerModel(new PowerModelHostSimple(maxPower, staticPower));

            hostList.add(host);
        }

        double initialGreen = config.getGreenInitialMin() +
                Math.random() * (config.getGreenInitialMax() - config.getGreenInitialMin());

        DatacenterGreenAware dc;

        if (config.isEnableHybridEnergy()) {
            dc = createHybridEnergyDatacenter(sim, hostList, dcIndex, initialGreen);
        } else if (config.isUseRealEnergyData() && config.getWindDataPath() != null) {
            dc = createWindPowerDatacenter(sim, hostList, dcIndex, initialGreen);
        } else if (config.isUseRealEnergyData() && config.getSolarDataPath() != null) {
            dc = createSolarPowerDatacenter(sim, hostList, dcIndex, initialGreen);
        } else {
            dc = new DatacenterGreenAware(sim, hostList, new VmAllocationPolicySimple(), initialGreen);
            logger.info("DC {} using traditional green energy profile", dcIndex);
        }

        double scalingFactor = config.getGreenFactorMin() +
                (dcIndex * 1.0 / config.getDcNum()) * (config.getGreenFactorMax() - config.getGreenFactorMin());
        dc.setGenerationScalingFactor(scalingFactor);

        if (config.isVerbose()) {
            logger.info("Created DC {} - Initial Green: {:.1f}kJ, Scaling: {:.2f}, Type: {}",
                    dcIndex, initialGreen/1000, scalingFactor,
                    config.isEnableHybridEnergy() ? "Hybrid" :
                            config.isUseRealEnergyData() ? "Real Data" : "Traditional");
        }

        return dc;
    }

    private DatacenterGreenAware createSolarPowerDatacenter(
            CloudSimPlus sim, List<Host> hostList, int dcIndex, double initialGreen) {

        List<RenewableEnergyGenerator> generators = new ArrayList<>();
        generators.add(new SolarEnergyLoader(config.getSolarDataPath(), config.getSolarPeakPower()));

        DatacenterGreenAware dc = new DatacenterGreenAware(
                sim, hostList, new VmAllocationPolicySimple(),
                initialGreen, generators
        );

        logger.info("DC {} configured with solar power only", dcIndex);
        return dc;
    }


    private DatacenterGreenAware createHybridEnergyDatacenter(
            CloudSimPlus sim, List<Host> hostList, int dcIndex, double initialGreen) {


        String[] windFiles = config.getWindDataPath().split(",");

        String windDataFile = windFiles[dcIndex % windFiles.length];

        double solarPeak = config.getSolarPeakPower() > 0 ?
                config.getSolarPeakPower() : 1000000; // 默认1MW

        DatacenterGreenAware dc = DatacenterGreenAware.createHybridEnergyDatacenter(
                sim, hostList, new VmAllocationPolicySimple(),
                initialGreen,
                config.getSolarDataPath(),  // 可以为null，使用模型
                windDataFile,
                solarPeak,
                1.0
        );

        logger.info("DC {} configured with hybrid energy: Solar ({}W) + Wind ({})",
                dcIndex, solarPeak, windDataFile);

        return dc;
    }

    /**
     * 创建风电数据中心
     */
    private DatacenterGreenAware createWindPowerDatacenter(
            CloudSimPlus sim, List<Host> hostList, int dcIndex, double initialGreen) {

        // 使用不同的风机数据
        String windDataFile = config.getWindDataPath();
        if (windDataFile.contains(",")) {
            // 如果提供了多个文件，选择一个
            String[] files = windDataFile.split(",");
            windDataFile = files[dcIndex % files.length];
        }

        DatacenterGreenAware dc = DatacenterGreenAware.createWithWindPower(
                sim, hostList, new VmAllocationPolicySimple(),
                initialGreen,
                windDataFile,
                1.0
        );

        logger.info("DC {} configured with wind power from: {}", dcIndex, windDataFile);

        return dc;
    }

    private DatacenterGreenAware createCustomEnergyDatacenter(
            CloudSimPlus sim, List<Host> hostList, int dcIndex, double initialGreen) {

        List<RenewableEnergyGenerator> generators = new ArrayList<>();

        // 根据数据中心位置配置不同的能源组合
        switch (dcIndex % 3) {
            case 0:
                // 北方数据中心：更多风能
                generators.add(new WindEnergyLoader(3000000)); // 3MW风机
                generators.add(new SolarEnergyLoader(500000)); // 500kW太阳能
                break;
            case 1:
                // 南方数据中心：更多太阳能
                generators.add(new SolarEnergyLoader(2000000)); // 2MW太阳能
                generators.add(new WindEnergyLoader(1000000)); // 1MW风机
                break;
            case 2:
                // 沿海数据中心：平衡配置
                generators.add(new WindEnergyLoader(config.getWindDataPath()));
                generators.add(new SolarEnergyLoader(1000000));
                break;
        }

        DatacenterGreenAware dc = new DatacenterGreenAware(
                sim, hostList, new VmAllocationPolicySimple(),
                initialGreen, generators
        );

        return dc;
    }

    private List<Vm> createVms() {
        List<Vm> list = new ArrayList<>();
        for (int i = 0; i < config.getVms(); i++) {
            Vm vm = new VmSimple(config.getHostMips(), config.getVmPes());
            vm.setRam(2048).setBw(1000).setSize(10_000);
            list.add(vm);
        }
        return list;
    }

    private void scheduleCloudlets(CloudSimPlus sim, GlobalBrokerRL globalBroker,
                                   List<TimedCloudlet> cloudlets, List<Datacenter> dcs,
                                   List<LocalBrokerRL> brokers, double[] globalRewardSum) {

        // 跟踪已提交的任务
        Set<TimedCloudlet> submitted = new HashSet<>();

        // 存储每个任务的决策信息
        Map<Long, CloudletDecisionInfo> decisionInfoMap = new HashMap<>();
        Set<Long> finishedCloudletIds = new HashSet<>();

        // 注册时钟监听器
        sim.addOnClockTickListener(evt -> {
            double now = sim.clock();

            // 提交到期的任务
            List<TimedCloudlet> toSubmit = cloudlets.stream()
                    .filter(tc -> !submitted.contains(tc) && tc.getSubmissionTime() <= now)
                    .toList();

            for (TimedCloudlet tc : toSubmit) {
                Cloudlet cl = tc.getCloudlet();

                // 获取全局状态
                double[] globalState = globalBroker.buildState(cl);

                // RL决策
                RLClient.ActionResponse result = globalRlClient.selectAction(
                        globalState, globalBroker.getLocalBrokers().size());

                // 记录当前绿色能源比例
                double currentGreenRatio = calculateGlobalGreenRatio(dcs);
                cloudletStartGreenRatio.put(cl.getId(), currentGreenRatio);
                cloudletStartTime.put(cl.getId(), now);

                // 提交到选定的数据中心
                globalBroker.submitCloudlet(cl, result.action);
                submitted.add(tc);

                // 存储决策信息
                CloudletDecisionInfo info = new CloudletDecisionInfo();
                info.globalState = globalState;
                info.globalAction = result.action;
                info.globalLogProb = result.log_prob;
                info.globalValue = result.value;
                info.startGreenEnergy = calculateTotalGreenEnergy(dcs);
                decisionInfoMap.put(cl.getId(), info);

                if (config.isDebug()) {
                    logger.debug("Cloudlet {} submitted at {:.2f} to DC {}",
                            cl.getId(), now, result.action);
                }
            }

            // 处理已完成的任务
            for (int i = 0; i < brokers.size(); i++) {
                LocalBrokerRL localBroker = brokers.get(i);
                RLClient localClient = localRlClients.get(i);

                for (Cloudlet cl : localBroker.getCloudletFinishedList()) {
                    long id = cl.getId();
                    if (!finishedCloudletIds.contains(id)) {
                        finishedCloudletIds.add(id);

                        CloudletDecisionInfo info = decisionInfoMap.get(id);
                        if (info == null) continue; // Skip dummy cloudlet

                        double globalReward = computeGlobalReward(cl, dcs);
                        globalRewardSum[0] += globalReward;

                        double[] globalNextState = globalBroker.buildState(cl);
                        boolean isDone = submitted.size() >= cloudlets.size();

                        globalRlClient.storeExperience(
                                info.globalState, info.globalAction, globalReward,
                                globalNextState, isDone, info.globalLogProb, info.globalValue);

                        if (localBroker.getStateMap().containsKey(id)) {
                            double[] localState = localBroker.getStateMap().get(id);
                            int localAction = localBroker.getActionMap().get(id);
                            double[] localNextState = localBroker.buildState(cl);
                            double localLogProb = localBroker.getProbLogMap().get(id);
                            double localValue = localBroker.getValueMap().get(id);

                            double localReward = computeLocalCloudletReward(cl);

                            localClient.storeExperienceLocal(
                                    localState, localAction, localReward,
                                    localNextState, isDone, localLogProb, localValue);

                            if (config.isDebug()) {
                                logger.debug("Cloudlet {} finished. Global reward: {:.3f}, Local reward: {:.3f}",
                                        id, globalReward, localReward);
                            }
                        }
                    }
                }
            }
        });
    }

    /**
     * 改进的全局奖励函数
     */
    private double computeGlobalReward(Cloudlet cl, List<Datacenter> dcs) {
        if (cl.getVm() == null || cl.getVm().getHost() == null) return 0.0;

        // 计算当前全局绿色能源比例
        double currentGreenRatio = calculateGlobalGreenRatio(dcs);

        // 获取任务开始时的比例
        Double startRatio = cloudletStartGreenRatio.get(cl.getId());
        if (startRatio == null) startRatio = episodeStartGreenRatio;

        // 改进因子（正值表示改进）
        double improvement = currentGreenRatio - startRatio;

        // 任务执行时间（归一化）
        double executionTime = cl.getFinishTime() - cl.getStartTime();
        double normalizedTime = Math.min(executionTime / 1000.0, 1.0); // 1000秒为基准

        // 综合奖励
        double reward = 0.4 * currentGreenRatio      // 当前状态奖励
                + 0.4 * (improvement * 10)    // 改进奖励（放大10倍）
                + 0.2 * (1.0 - normalizedTime); // 时间效率奖励

        return reward;
    }

    private double computeLocalCloudletReward(Cloudlet cl) {
        if (cl.getVm() == null || cl.getVm().getHost() == null) return 0.0;

        Host host = cl.getVm().getHost();
        Datacenter dc = host.getDatacenter();

        double makespan = cl.getFinishTime() - cl.getStartTime();
        double waitTime = cl.getStartWaitTime() - cl.getStartTime();
        double execTime = cl.getFinishTime() - cl.getStartWaitTime();

        double normMakespan = Math.min(makespan / 1000.0, 1.0);
        double normWaitTime = Math.min(waitTime / 100.0, 1.0);

        double util = host.getCpuPercentUtilization();
        double power = host.getPowerModel().getPower(util);
        double energy = execTime * power;
        double normEnergy = Math.min(energy / 5000.0, 1.0);

        double dcGreenRatio = 0.0;
        if (dc instanceof DatacenterGreenAware greenDc) {
            dcGreenRatio = greenDc.getCurrentGreenEnergyRatio();
        }

        double loadBalance = 1.0 - Math.abs(util - 0.7);
        
        double reward = 0.3 * (1.0 - normMakespan)
                + 0.2 * (1.0 - normWaitTime)
                + 0.2 * (1.0 - normEnergy)
                + 0.2 * dcGreenRatio
                + 0.1 * loadBalance;

        return reward;
    }

    private double calculateGlobalGreenRatio(List<Datacenter> dcs) {
        double totalGreen = 0.0;
        double totalEnergy = 0.0;

        for (Datacenter dc : dcs) {
            if (dc instanceof DatacenterGreenAware greenDc) {
                totalGreen += greenDc.getTotalGreenUsed();
                totalEnergy += greenDc.getTotalGreenUsed() + greenDc.getTotalBrownUsed();
            }
        }

        return totalEnergy > 0 ? totalGreen / totalEnergy : 0.0;
    }

    private double calculateTotalGreenEnergy(List<Datacenter> dcs) {
        double total = 0.0;
        for (Datacenter dc : dcs) {
            if (dc instanceof DatacenterGreenAware green) {
                total += green.getCurrentGreenEnergyStock();
            }
        }
        return total;
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

    private void printEpisodeResults(int episode, EpisodeStats stats, double totalReward) {
        logger.info("Episode {} Results:", episode + 1);
        logger.info("  Green Energy: Initial={:.0f}J, Generated={:.0f}J, Used={:.0f}J",
                stats.totalGreenInitial, stats.totalGreenGeneration, stats.totalGreenUsed);
        logger.info("  Energy Usage: Total={:.0f}J, Green Ratio={:.2f}%, Utilization={:.2f}%",
                stats.totalEnergyUsed, stats.greenEnergyRatio, stats.greenUtilizationRatio);
        logger.info("  Performance: Makespan={:.2f}s, Total Reward={:.3f}",
                stats.makespan, totalReward);
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

    private static class CloudletDecisionInfo {
        double[] globalState;
        int globalAction;
        double globalLogProb;
        double globalValue;
        double startGreenEnergy;
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

    private void adjustCloudletForDatacenter(Cloudlet cloudlet, List<Datacenter> dcs) {

        int schedulingClass = inferSchedulingClass(cloudlet);

        switch (schedulingClass) {
            case 0:
                cloudlet.setPriority(cloudlet.getPriority() - 10);
                break;
            case 3:
                cloudlet.setPriority(cloudlet.getPriority() + 10);
                break;
        }
    }

    private int inferSchedulingClass(Cloudlet cloudlet) {
        int priority = cloudlet.getPriority();
        if (priority < 100) return 0;
        if (priority >= 300) return 3;
        if (priority >= 200) return 2;
        return 1;
    }
}