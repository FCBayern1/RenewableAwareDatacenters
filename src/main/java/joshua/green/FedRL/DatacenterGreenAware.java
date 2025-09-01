package joshua.green.FedRL;

import joshua.green.FedRL.energy.RenewableEnergyGenerator;
import joshua.green.FedRL.energy.SolarEnergyLoader;
import joshua.green.FedRL.energy.WindEnergyLoader;

import lombok.Getter;
import lombok.Setter;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicy;
import org.cloudsimplus.core.Simulation;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.resources.DatacenterStorage;
import org.cloudsimplus.resources.SanStorage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

@Getter
@Setter
public class DatacenterGreenAware extends DatacenterSimple {

    private double greenEnergy; // current green energy balance (J)

    private double cumulativeSurplus;

    // 原有的能源生成映射
    private final Map<Integer, Double> greenGenerationMap = new HashMap<>();
    private String greenProfileCsvPath = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/Datacenters/green_generation_continuous.csv";

    // 可再生能源生成器列表
    private List<RenewableEnergyGenerator> energyGenerators = new ArrayList<>();

    // 各种能源的贡献统计
    private Map<String, Double> energyContributions = new HashMap<>();

    // 是否使用新的能源系统
    private boolean useModularEnergySystem = false;

    private double lastTickGreenGeneration = 0;     // J
    private double lastTickTotalEnergyUsed = 0;     // J
    private double lastTickSurplus = 0;             // surplus = green - used

    private double generationScalingFactor = 1.0;

    private double initalGreenEnergy;

    private double totalGenerate;
    private double totalGreenUsed = 0;
    private double totalBrownUsed = 0;

    private double lastTickTime = 0;

    private double generation = 0;

    private int lastProcessedTick = -1;

    private Map<LocalDateTime, Double> realGenerationMap;

    private Double cachedAvgProcessingAbility = null;
    private Double cachedCpuUtilization = null;
    private long lastCacheUpdateTime = -1;


    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, double initialGreenKWh) {
        super(simulation, hostList, vmAllocationPolicy);
        this.greenEnergy = initialGreenKWh;
        this.initalGreenEnergy = initialGreenKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, VmAllocationPolicy vmAllocationPolicy, double greenEnergyKWh) {
        super(simulation, vmAllocationPolicy);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, List<SanStorage> storageList,
                                double greenEnergyKWh) {
        super(simulation, hostList, vmAllocationPolicy, storageList);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, DatacenterStorage storage,
                                double greenEnergyKWh) {
        super(simulation, hostList, vmAllocationPolicy, storage);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList, double greenEnergyKWh) {
        super(simulation, hostList);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    // ==================== 支持模块化能源系统 ====================

    /**
     * 支持多种可再生能源
     */
    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, double initialGreenKWh,
                                List<RenewableEnergyGenerator> generators) {
        super(simulation, hostList, vmAllocationPolicy);
        this.greenEnergy = initialGreenKWh;
        this.initalGreenEnergy = initialGreenKWh;

        if (generators != null && !generators.isEmpty()) {
            this.energyGenerators = generators;
            this.useModularEnergySystem = true;

            // 初始化能源贡献统计
            for (RenewableEnergyGenerator gen : energyGenerators) {
                energyContributions.put(gen.getEnergyType(), 0.0);
            }
            LOGGER.info("Datacenter {} configured with {} renewable energy sources",
                    getId(), energyGenerators.size());
        } else {
            loadGreenEnergyProfile(greenProfileCsvPath);
        }
    }


    /**
     * 创建使用风电数据的数据中心
     */
    public static DatacenterGreenAware createWithWindPower(
            Simulation simulation, List<? extends Host> hostList,
            VmAllocationPolicy vmAllocationPolicy, double initialGreenKWh,
            String windDataPath, double scalingFactor) {

        List<RenewableEnergyGenerator> generators = new ArrayList<>();
        generators.add(new WindEnergyLoader(windDataPath));

        DatacenterGreenAware dc = new DatacenterGreenAware(
                simulation, hostList, vmAllocationPolicy, initialGreenKWh, generators
        );
        dc.setGenerationScalingFactor(scalingFactor);

        return dc;
    }

    /**
     * 创建混合能源数据中心（太阳能+风能）
     */
    public static DatacenterGreenAware createHybridEnergyDatacenter(
            Simulation simulation, List<? extends Host> hostList,
            VmAllocationPolicy vmAllocationPolicy, double initialGreenKWh,
            String solarDataPath, String windDataPath,
            double solarPeakPower, double scalingFactor) {

        List<RenewableEnergyGenerator> generators = new ArrayList<>();

        // 添加太阳能
        if (solarDataPath != null && !solarDataPath.isEmpty()) {
            generators.add(new SolarEnergyLoader(solarDataPath, solarPeakPower));
        } else {
            generators.add(new SolarEnergyLoader(solarPeakPower));
        }

        // 添加风能
        if (windDataPath != null && !windDataPath.isEmpty()) {
            generators.add(new WindEnergyLoader(windDataPath));
        } else {
            generators.add(new WindEnergyLoader(2000000)); // 2MW默认风机
        }

        DatacenterGreenAware dc = new DatacenterGreenAware(
                simulation, hostList, vmAllocationPolicy, initialGreenKWh, generators
        );
        dc.setGenerationScalingFactor(scalingFactor);

        return dc;
    }

    private double generateGreenEnergy(double time) {
        if (useModularEnergySystem) {
            return getTotalGreenPower(time);
        } else {
            int tick = (int)Math.floor(time);
            double base = greenGenerationMap.getOrDefault(tick, 0.0);
            return base * generationScalingFactor;
        }
    }

    /**
     * 获取所有能源生成器的总功率
     */
    private double getTotalGreenPower(double time) {
        double totalPower = 0;

        for (RenewableEnergyGenerator generator : energyGenerators) {
            double power = generator.getPowerAtTime(time) * generationScalingFactor;
            totalPower += power;
        }

        return totalPower;
    }

    /**
     * 获取时间段内的总能量，并更新贡献统计
     */
    private double getTotalGreenEnergy(double startTime, double duration) {
        if (!useModularEnergySystem) {
            // 使用原有方式的简单计算
            double avgPower = generateGreenEnergy(startTime);
            return avgPower * duration;
        }

        double totalEnergy = 0;

        for (RenewableEnergyGenerator generator : energyGenerators) {
            double energy = generator.getEnergyForPeriod(startTime, duration) * generationScalingFactor;
            totalEnergy += energy;

            // 更新贡献统计
            energyContributions.merge(generator.getEnergyType(), energy, Double::sum);
        }

        return totalEnergy;
    }

    public double getPredictedGreenGeneration() {
        if (useModularEnergySystem) {
            double totalPredicted = 0;
            double lookahead = 3600; // 预测1小时后

            for (RenewableEnergyGenerator generator : energyGenerators) {
                totalPredicted += generator.getPredictedPower(getSimulation().clock(), lookahead)
                        * generationScalingFactor;
            }

            return totalPredicted;
        } else {
            double nextTime = getSimulation().clock() + 1.0;
            int nextTick = (int)Math.floor(nextTime);
            return greenGenerationMap.getOrDefault(nextTick, 0.0) * generationScalingFactor;
        }
    }

    public double getPredictedGreenGenerationWindow(int windowSize) {
        double total = 0;
        double currentTime = getSimulation().clock();

        if (useModularEnergySystem) {
            for (RenewableEnergyGenerator generator : energyGenerators) {
                total += generator.getEnergyForPeriod(currentTime, windowSize) * generationScalingFactor;
            }
        } else {
            for (int i = 1; i <= windowSize; i++) {
                int futureTick = (int)Math.floor(currentTime + i);
                total += greenGenerationMap.getOrDefault(futureTick, 0.0);
            }
            total *= generationScalingFactor;
        }

        return total;
    }

    public double getGreenGenerationTrend() {
        double current = generateGreenEnergy(getSimulation().clock());
        double future = getPredictedGreenGeneration();
        return future - current;
    }

    @Override
    protected double updateHostsProcessing() {
        double time = getSimulation().clock();
        double interval = time - getLastProcessTime();
        int currentTick = (int) Math.floor(time);
        double minDelay = Double.MAX_VALUE;

        if (currentTick != lastProcessedTick) {
            lastProcessedTick = currentTick;

            invalidateCache();

            double addedEnergy;
            if (useModularEnergySystem) {
                addedEnergy = getTotalGreenEnergy(getLastProcessTime(), interval);
            } else {
                double power = generateGreenEnergy(time);
                addedEnergy = power * interval;
            }

            lastTickGreenGeneration = addedEnergy;
            totalGenerate += addedEnergy;
            greenEnergy += addedEnergy;

            double powerWatts = getPowerModel().getPower();
            double energyWS = powerWatts * interval;
            lastTickTotalEnergyUsed = energyWS;

            double greenUsed = Math.min(energyWS, Math.max(greenEnergy, 0));
            double brownUsed = energyWS - greenUsed;

            greenEnergy = Math.max(0, greenEnergy - greenUsed);

            totalGreenUsed += greenUsed;
            totalBrownUsed += brownUsed;

            lastTickSurplus = lastTickGreenGeneration - lastTickTotalEnergyUsed;
            cumulativeSurplus += lastTickSurplus;

            // 增强的日志输出
            if (useModularEnergySystem && LOGGER.isInfoEnabled()) {
                StringBuilder energyDetails = new StringBuilder();
                for (RenewableEnergyGenerator gen : energyGenerators) {
                    double power = gen.getPowerAtTime(time) * generationScalingFactor;
                    energyDetails.append(String.format("%s:%.1fkW ", gen.getEnergyType(), power/1000));
                }

                LOGGER.info(String.format("%.2f: DC %d - %s| Used %.2f J (green: %.2f, brown: %.2f, storage: %.2f)",
                        time, getId(), energyDetails.toString(), energyWS, greenUsed, brownUsed, greenEnergy));
            } else {
                LOGGER.info(String.format("%.2f: DC %d used %.2f J (green: %.2f, brown: %.2f, green left: %.2f), CPU = %.2f",
                        time, getId(), energyWS, greenUsed, brownUsed, greenEnergy, getCurrentCpuUtilization()));
            }
        }

        for (var host : getHostList()) {
            double delay = host.updateProcessing(time);
            minDelay = Math.min(minDelay, delay);
        }

        double minTimeBetweenEvents = getSimulation().clock();
        if (minDelay != 0) {
            minDelay = Math.max(minDelay, minTimeBetweenEvents);
        }

        return minDelay;
    }

    // ==================== 新增方法 ====================

    /**
     * 添加能源生成器
     */
    public void addEnergyGenerator(RenewableEnergyGenerator generator) {
        if (generator != null) {
            energyGenerators.add(generator);
            energyContributions.put(generator.getEnergyType(), 0.0);
            useModularEnergySystem = true;
        }
    }

    /**
     * 获取能源组合信息
     */
    public String getEnergyMixInfo() {
        if (!useModularEnergySystem) {
            return "Traditional green energy profile";
        }

        StringBuilder info = new StringBuilder();
        double totalContribution = energyContributions.values().stream()
                .mapToDouble(Double::doubleValue)
                .sum();

        if (totalContribution > 0) {
            for (Map.Entry<String, Double> entry : energyContributions.entrySet()) {
                double percentage = (entry.getValue() / totalContribution) * 100;
                info.append(String.format("%s: %.1f%% ", entry.getKey(), percentage));
            }
        }

        return info.toString();
    }

    /**
     * 获取各能源生成器的当前状态
     */
    public Map<String, Double> getCurrentEnergyGeneratorStatus() {
        Map<String, Double> status = new HashMap<>();

        if (useModularEnergySystem) {
            double currentTime = getSimulation().clock();
            for (RenewableEnergyGenerator gen : energyGenerators) {
                double power = gen.getPowerAtTime(currentTime) * generationScalingFactor;
                status.put(gen.getEnergyType(), power);
            }
        }

        return status;
    }

    /**
     * 设置是否使用模块化能源系统
     */
    public void setUseModularEnergySystem(boolean use) {
        this.useModularEnergySystem = use && !energyGenerators.isEmpty();
    }

    // ==================== 保留所有原有方法 ====================

    public double getSurplusForCurrentTick() {
        return lastTickSurplus;
    }

    public double getCurrentGreenEnergyStock() {
        return greenEnergy;
    }

    public double getCurrentGreenEnergyRatio() {
        double total = totalGreenUsed + totalBrownUsed;
        return total > 0 ? totalGreenUsed / total : 0.0;
    }

    public double getInstantGreenEnergyRatio() {
        if (lastTickTotalEnergyUsed <= 0) return 0.0;
        double greenUsedLastTick = Math.min(lastTickTotalEnergyUsed,
                Math.max(greenEnergy + lastTickGreenGeneration, 0));
        return greenUsedLastTick / lastTickTotalEnergyUsed;
    }

    public double getEnergySelfSufficiencyRate() {
        if (lastTickTotalEnergyUsed <= 0) return 1.0;
        return Math.min(1.0, lastTickGreenGeneration / lastTickTotalEnergyUsed);
    }

    public double getAverageProcessingAbility() {
        double currentTime = getSimulation().clock();
        if (cachedAvgProcessingAbility == null || currentTime != lastCacheUpdateTime) {
            cachedAvgProcessingAbility = calculateAverageProcessingAbility();
            lastCacheUpdateTime = (long)currentTime;
        }
        return cachedAvgProcessingAbility;
    }

    private double calculateAverageProcessingAbility() {
        if (getHostList().isEmpty()) return 0.0;

        double totalMips = 0;
        int hostCount = getHostList().size();

        for (Host host : getHostList()) {
            totalMips += host.getTotalMipsCapacity();
        }

        return totalMips / hostCount;
    }

    public double getCurrentCpuUtilization() {
        double currentTime = getSimulation().clock();
        if (cachedCpuUtilization == null || currentTime != lastCacheUpdateTime) {
            cachedCpuUtilization = calculateCurrentCpuUtilization();
            lastCacheUpdateTime = (long)currentTime;
        }
        return cachedCpuUtilization;
    }

    private double calculateCurrentCpuUtilization() {
        if (getHostList().isEmpty()) return 0.0;

        double totalUtil = 0;
        int hostCount = getHostList().size();

        for (Host host : getHostList()) {
            totalUtil += host.getCpuMipsUtilization() / host.getTotalMipsCapacity();
        }

        return totalUtil / hostCount;
    }

    private void invalidateCache() {
        cachedAvgProcessingAbility = null;
        cachedCpuUtilization = null;
    }

    public int getAvailableHostCount() {
        int count = 0;
        for (Host host : getHostList()) {
            if (host.isActive() && !host.isFailed()) {
                count++;
            }
        }
        return count;
    }

    public double getOverallLoad() {
        double cpuLoad = getCurrentCpuUtilization();
        double ramLoad = getRamUtilization() / 100.0;
        double bwLoad = getBwUtilization() / 100.0;

        // 加权平均，CPU权重最高
        return 0.5 * cpuLoad + 0.3 * ramLoad + 0.2 * bwLoad;
    }

    public long getAverageRam() {
        if (getHostList().isEmpty()) return 0;
        long totalRam = 0;
        int hostCount = getHostList().size();
        for (Host host : getHostList()) {
            totalRam += host.getRam().getCapacity();
        }
        return totalRam/hostCount;
    }

    public double getRamUtilization() {
        if (getHostList().isEmpty()) return 0.0;
        double totalUtil = 0;
        for (Host host : getHostList()) {
            totalUtil += host.getRam().getPercentUtilization();
        }
        return totalUtil / getHostList().size();
    }

    public double getBwUtilization() {
        if (getHostList().isEmpty()) return 0.0;
        double totalBw = 0;
        for (Host host : getHostList()) {
            totalBw += host.getBw().getPercentUtilization();
        }
        return totalBw / getHostList().size();
    }

    public double getTotalPowerDemand() {
        double totalPower = 0.0;

        for (Host host : getHostList()) {
            double util = host.getCpuPercentUtilization();
            totalPower += host.getPowerModel().getPower(util);
        }

        return totalPower;
    }

    private void loadGreenEnergyProfile(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // skip header
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int tick = Integer.parseInt(parts[0].trim());
                double power = Double.parseDouble(parts[1].trim());
                greenGenerationMap.put(tick, power);
            }
            LOGGER.info("Loaded green energy profile with " + greenGenerationMap.size() + " entries");
        } catch (IOException e) {
            throw new RuntimeException("Error loading green energy profile", e);
        }
    }

    public String getDebugInfo() {
        String baseInfo = String.format("DC%d: Green=%.2fJ, Surplus=%.2fJ/s, CPU=%.2f%%, GreenRatio=%.2f%%",
                getId(), greenEnergy, lastTickSurplus,
                getCurrentCpuUtilization() * 100,
                getCurrentGreenEnergyRatio() * 100);

        if (useModularEnergySystem) {
            baseInfo += " | Mix: " + getEnergyMixInfo();
        }

        return baseInfo;
    }

}