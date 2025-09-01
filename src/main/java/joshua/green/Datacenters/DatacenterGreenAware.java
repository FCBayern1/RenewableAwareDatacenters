package joshua.green.Datacenters;

import lombok.Getter;
import lombok.Setter;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicy;
import org.cloudsimplus.core.Simulation;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.resources.DatacenterStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
public class DatacenterGreenAware extends DatacenterSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatacenterGreenAware.class);

    // 能源状态变量（使用同步保护）
    private volatile double greenEnergy; // 当前绿色能源库存（焦耳）
    private volatile double cumulativeSurplus; // 累积盈余

    // 风能生成数据
    private final TreeMap<Double, Double> greenGenerationMap = new TreeMap<>(); // 仿真时间(秒) -> 功率(瓦特)
    private String greenProfileCsvPath; // 风电数据文件路径

    // 常量配置
    private static final double WIND_DATA_INTERVAL = 600.0; // 风能数据间隔（秒）
    private static final double ENERGY_UPDATE_THRESHOLD = 0.01; // 能源更新阈值（秒）
    private static final double ENERGY_BALANCE_TOLERANCE = 0.01; // 能源平衡容差（焦耳）

    // 统计变量
    private volatile double lastTickGreenGeneration = 0;     // 上个tick的绿色能源生成（焦耳）
    private volatile double lastTickTotalEnergyUsed = 0;     // 上个tick的总能源消耗（焦耳）
    private volatile double lastTickSurplus = 0;             // 上个tick的盈余
    private volatile double lastTickBrownUsed = 0;           // 上个tick的棕色能源使用（焦耳）


    // 配置参数
    private double generationScalingFactor = 1.0;            // 生成缩放因子
    private final double initialGreenEnergy;                 // 初始绿色能源（焦耳）

    // 累积统计
    private volatile double totalGenerated = 0;              // 总生成量（焦耳）
    private volatile double totalGreenUsed = 0;              // 总绿色能源使用量（焦耳）
    private volatile double totalBrownUsed = 0;              // 总棕色能源使用量（焦耳）

    // 时间跟踪（统一管理）
    private volatile double lastEnergyUpdateTime = 0;        // 上次能源更新时间
    private volatile double lastGenerationUpdateTime = 0;    // 上次生成更新时间

    // 同步锁
    private final Object energyLock = new Object();

    // 调度标志
    private final AtomicBoolean energyGenerationScheduled = new AtomicBoolean(false);

    // 验证和调试
    private boolean enableEnergyValidation = true;           // 是否启用能源平衡验证
    private long validationCounter = 0;                      // 验证计数器
    private static final long VALIDATION_INTERVAL = 100;     // 验证间隔

    /**
     * 主构造函数
     */
    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, double initialGreenKWh, String filePath) {
        super(simulation, hostList, vmAllocationPolicy);
        this.initialGreenEnergy = initialGreenKWh * 3600000; // kWh转换为焦耳
        this.greenEnergy = this.initialGreenEnergy;
        this.greenProfileCsvPath = filePath;
        initializeDatacenter();
    }

    /**
     * 简化构造函数 - 使用默认文件路径
     */
    public DatacenterGreenAware(Simulation simulation, VmAllocationPolicy vmAllocationPolicy, double greenEnergyKWh) {
        super(simulation, vmAllocationPolicy);
        this.initialGreenEnergy = greenEnergyKWh * 3600000;
        this.greenEnergy = this.initialGreenEnergy;
        this.greenProfileCsvPath = "/Users/joshua/Downloads/Turbine_1_2021.csv";
        initializeDatacenter();
    }

    /**
     * 构造函数 - 带主机列表
     */
    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, double greenEnergyKWh) {
        super(simulation, hostList, vmAllocationPolicy);
        this.initialGreenEnergy = greenEnergyKWh * 3600000;
        this.greenEnergy = this.initialGreenEnergy;
        this.greenProfileCsvPath = "/Users/joshua/Downloads/Turbine_1_2021.csv";
        initializeDatacenter();
    }

    /**
     * 构造函数 - 带存储
     */
    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, DatacenterStorage storage, double greenEnergyKWh) {
        super(simulation, hostList, vmAllocationPolicy, storage);
        this.initialGreenEnergy = greenEnergyKWh * 3600000;
        this.greenEnergy = this.initialGreenEnergy;
        this.greenProfileCsvPath = "/Users/joshua/Downloads/Turbine_1_2021.csv";
        initializeDatacenter();
    }

    /**
     * 构造函数 - 简单版本
     */
    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList, double greenEnergyKWh) {
        super(simulation, hostList);
        this.initialGreenEnergy = greenEnergyKWh * 3600000;
        this.greenEnergy = this.initialGreenEnergy;
        this.greenProfileCsvPath = "/Users/joshua/Downloads/Turbine_1_2021.csv";
        initializeDatacenter();
    }

    /**
     * 初始化数据中心
     */
    private void initializeDatacenter() {
        // 先加载风能数据
        if (!loadWindEnergyProfile(greenProfileCsvPath)) {
            LOGGER.warn("Failed to load wind energy profile, using zero generation");
        }

        // 然后调度能源生成
        scheduleEnergyGeneration();

        LOGGER.info("DatacenterGreenAware {} initialized with {} kWh initial green energy",
                getId(), initialGreenEnergy / 3600000);
    }

    /**
     * 启动独立的能源生成调度
     */
    private void scheduleEnergyGeneration() {
        if (energyGenerationScheduled.compareAndSet(false, true)) {
            // 注册到simulation的clock tick监听器
            getSimulation().addOnClockTickListener(evt -> {
                try {
                    updateGreenEnergyGeneration();
                } catch (Exception e) {
                    LOGGER.error("Error in energy generation update: ", e);
                }
            });
            LOGGER.debug("Energy generation scheduled for datacenter {}", getId());
        }
    }

    /**
     * 独立的绿色能源生成更新方法
     */
    private void updateGreenEnergyGeneration() {
        double currentTime = getSimulation().clock();

        synchronized (energyLock) {
            // 避免重复处理同一时间点
            if (currentTime <= lastGenerationUpdateTime + ENERGY_UPDATE_THRESHOLD) {
                return;
            }

            // 计算从上次更新到现在的发电量
            double windEnergy = calculateWindEnergy(lastGenerationUpdateTime, currentTime);

            if (windEnergy > 0) {
                // 更新能源库存
                greenEnergy += windEnergy;
                totalGenerated += windEnergy;
                lastTickGreenGeneration = windEnergy;

                // 获取当前风电功率用于日志
                double currentWindPowerKW = getWindPowerAtTime(currentTime) * generationScalingFactor / 1000;

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format(
                            "%.2f: DC %d - Wind Generation: %.1f kW | Generated: %.2f J | Storage: %.2f J (%.2f kWh)",
                            currentTime, getId(), currentWindPowerKW, windEnergy, greenEnergy, greenEnergy / 3600000
                    ));
                }
            }

            lastGenerationUpdateTime = currentTime;
        }
    }

    /**
     * 重写主机处理更新方法
     */
    @Override
    protected double updateHostsProcessing() {
        double currentTime = getSimulation().clock();
        double minDelay = Double.MAX_VALUE;

        synchronized (energyLock) {
            // 计算时间间隔
            double interval = currentTime - lastEnergyUpdateTime;

            if (interval > ENERGY_UPDATE_THRESHOLD) {
                // 1. 计算数据中心的能源消耗
                double powerWatts = this.getPowerModel().getPower();
                double energyConsumed = powerWatts * interval;
                lastTickTotalEnergyUsed = energyConsumed;

                // 2. 确定使用多少绿色能源和棕色能源
                double greenUsed = Math.min(energyConsumed, Math.max(greenEnergy, 0));
                double brownUsed = energyConsumed - greenUsed;

                // 3. 更新能源余额和统计
                greenEnergy = Math.max(0, greenEnergy - greenUsed);
                totalGreenUsed += greenUsed;
                totalBrownUsed += brownUsed;
                lastTickBrownUsed = brownUsed;

                // 4. 计算surplus（基于实际的生成和消耗）
                // 注意：生成已经在 updateGreenEnergyGeneration 中处理
                lastTickSurplus = lastTickGreenGeneration - energyConsumed;
                cumulativeSurplus += lastTickSurplus;

                // 5. 记录日志
                if (LOGGER.isInfoEnabled() && interval > 1.0) { // 每秒最多记录一次
                    LOGGER.info(String.format(
                            "%.2f: DC %d - Power: %.1f W | Used: %.2f J (green: %.2f J, brown: %.2f J) | " +
                                    "Storage: %.2f kWh | CPU: %.1f%% | RAM: %.1f%% | Surplus: %.2f J",
                            currentTime, getId(), powerWatts, energyConsumed, greenUsed, brownUsed,
                            greenEnergy / 3600000, getCurrentCpuUtilization() * 100,
                            getCurrentRamUtilization() * 100, lastTickSurplus
                    ));
                }

                lastEnergyUpdateTime = currentTime;

                // 6. 定期验证能源平衡
                if (enableEnergyValidation && ++validationCounter % VALIDATION_INTERVAL == 0) {
                    validateEnergyBalance();
                }
            }
        }

        // 更新主机处理（在同步块外执行）
        for (var host : getHostList()) {
            double delay = host.updateProcessing(currentTime);
            minDelay = Math.min(minDelay, delay);
        }

        // 确保最小延迟合理
        if (minDelay == Double.MAX_VALUE) {
            minDelay = 1.0; // 默认1秒
        }

        return minDelay;
    }

    /**
     * 验证能源平衡
     */
    private void validateEnergyBalance() {
        double totalIn = initialGreenEnergy + totalGenerated;
        double totalOut = totalGreenUsed;
        double currentStock = greenEnergy;

        double balance = totalIn - totalOut - currentStock;

        if (Math.abs(balance) > ENERGY_BALANCE_TOLERANCE) {
            LOGGER.warn("Energy balance violation in DC {}: difference = {} J " +
                            "(in: {}, out: {}, stock: {})",
                    getId(), balance, totalIn, totalOut, currentStock);
        }
    }

    /**
     * 获取指定时间的风电功率（使用线性插值）
     */
    private double getWindPowerAtTime(double time) {
        if (greenGenerationMap.isEmpty()) {
            return 0;
        }

        Map.Entry<Double, Double> floorEntry = greenGenerationMap.floorEntry(time);
        Map.Entry<Double, Double> ceilingEntry = greenGenerationMap.ceilingEntry(time);

        if (floorEntry == null) {
            return ceilingEntry != null ? ceilingEntry.getValue() : 0;
        }
        if (ceilingEntry == null) {
            return floorEntry.getValue();
        }

        if (floorEntry.getKey().equals(time)) {
            return floorEntry.getValue();
        }

        // 线性插值
        double t1 = floorEntry.getKey();
        double t2 = ceilingEntry.getKey();
        double p1 = floorEntry.getValue();
        double p2 = ceilingEntry.getValue();

        double ratio = (time - t1) / (t2 - t1);
        return p1 + ratio * (p2 - p1);
    }

    /**
     * 计算时间段内的发电量（使用梯形法则积分）
     */
    private double calculateWindEnergy(double startTime, double endTime) {
        if (startTime >= endTime || greenGenerationMap.isEmpty()) {
            return 0;
        }

        double totalEnergy = 0;
        double timeStep = Math.min(60, endTime - startTime); // 最大60秒步长

        double currentTime = startTime;
        double previousPower = getWindPowerAtTime(currentTime) * generationScalingFactor;

        while (currentTime < endTime) {
            double nextTime = Math.min(currentTime + timeStep, endTime);
            double nextPower = getWindPowerAtTime(nextTime) * generationScalingFactor;

            // 梯形法则：面积 = (上底 + 下底) * 高 / 2
            double avgPower = (previousPower + nextPower) / 2.0;
            double timeDelta = nextTime - currentTime;
            double energy = avgPower * timeDelta;

            totalEnergy += energy;

            currentTime = nextTime;
            previousPower = nextPower;
        }

        return totalEnergy;
    }

    /**
     * 加载风电数据（读取CSV文件的OT列）
     */
    private boolean loadWindEnergyProfile(String filePath) {
        // 检查文件是否存在
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            LOGGER.error("Wind energy profile file not found: {}", filePath);
            return false;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            // 读取表头
            String line = br.readLine();
            if (line == null) {
                LOGGER.error("CSV file is empty: {}", filePath);
                return false;
            }

            // 查找OT列索引
            String[] headers = line.split(",");
            int otIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].trim().equalsIgnoreCase("OT")) {
                    otIndex = i;
                    break;
                }
            }

            if (otIndex == -1) {
                LOGGER.error("OT column not found in CSV file: {}", filePath);
                return false;
            }

            // 读取数据
            double simulationTime = 0;
            int dataCount = 0;
            int errorCount = 0;
            double totalPower = 0;
            double maxPower = 0;
            double minPower = Double.MAX_VALUE;

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length <= otIndex) {
                    errorCount++;
                    continue;
                }

                try {
                    String powerStr = parts[otIndex].trim();
                    if (powerStr.isEmpty()) {
                        errorCount++;
                        continue;
                    }

                    double powerKW = Double.parseDouble(powerStr);

                    // 验证功率值合理性
                    if (powerKW < 0) {
                        LOGGER.debug("Negative power value at line {}, setting to 0", dataCount + 2);
                        powerKW = 0;
                    } else if (powerKW > 10000) { // 假设单台风机不超过10MW
                        LOGGER.warn("Unusually high power value at line {}: {} kW", dataCount + 2, powerKW);
                    }

                    double powerW = powerKW * 1000; // 转换为瓦特
                    greenGenerationMap.put(simulationTime, powerW);

                    // 更新统计
                    dataCount++;
                    totalPower += powerW;
                    maxPower = Math.max(maxPower, powerW);
                    minPower = Math.min(minPower, powerW);
                    simulationTime += WIND_DATA_INTERVAL;

                } catch (NumberFormatException e) {
                    LOGGER.debug("Error parsing power value at line {}: {}", dataCount + 2, parts[otIndex]);
                    errorCount++;
                }
            }

            if (dataCount == 0) {
                LOGGER.error("No valid data points loaded from {}", filePath);
                return false;
            }

            // 输出加载统计
            double avgPowerKW = (totalPower / dataCount) / 1000;
            LOGGER.info(String.format(
                    "Loaded wind power profile from %s:\n" +
                            "  - Data points: %d (errors: %d)\n" +
                            "  - Average power: %.1f kW\n" +
                            "  - Min/Max power: %.1f/%.1f kW\n" +
                            "  - Duration: %.1f hours (%.1f days)",
                    filePath, dataCount, errorCount, avgPowerKW,
                    minPower / 1000, maxPower / 1000,
                    simulationTime / 3600, simulationTime / 86400
            ));

            return true;

        } catch (IOException e) {
            LOGGER.error("Error loading wind energy profile from {}", filePath, e);
            return false;
        }
    }

    /**
     * 获取当前风电功率（瓦特）
     */
    public double getCurrentGreenPower() {
        synchronized (energyLock) {
            double time = getSimulation().clock();
            return getWindPowerAtTime(time) * generationScalingFactor;
        }
    }

    /**
     * 预测未来一段时间的绿色能源可用量
     */
    public double predictGreenAvailability(double startTime, double endTime) {
        synchronized (energyLock) {
            double currentTime = getSimulation().clock();

            // 确保预测时间合理
            if (startTime < currentTime) {
                startTime = currentTime;
            }
            if (endTime <= startTime) {
                return greenEnergy;
            }

            double currentStock = greenEnergy;
            double futureGeneration = calculateWindEnergy(startTime, endTime);

            // 估算未来消耗（基于当前功率）
            double currentPower = this.getPowerModel().getPower();
            double estimatedConsumption = currentPower * (endTime - startTime);

            return Math.max(0, currentStock + futureGeneration - estimatedConsumption);
        }
    }

    /**
     * 获取绿色能源库存状态
     */
    public GreenEnergyStatus getGreenEnergyStatus() {
        synchronized (energyLock) {
            double currentStock = greenEnergy;
            double currentPower = getCurrentGreenPower();
            double stockRatio = currentStock / initialGreenEnergy;

            if (stockRatio > 0.8 && currentPower > 0) {
                return GreenEnergyStatus.ABUNDANT;
            } else if (stockRatio > 0.3 || currentPower > 0) {
                return GreenEnergyStatus.SUFFICIENT;
            } else if (currentStock > 0) {
                return GreenEnergyStatus.LOW;
            } else {
                return GreenEnergyStatus.DEPLETED;
            }
        }
    }

    /**
     * 绿色能源状态枚举
     */
    public enum GreenEnergyStatus {
        ABUNDANT,   // 充足（>80%库存且有发电）
        SUFFICIENT, // 足够（>30%库存或有发电）
        LOW,        // 低（有库存但不足30%）
        DEPLETED    // 耗尽（无库存）
    }

    // ========== 性能指标方法 ==========

    /**
     * 获取平均处理能力（MIPS）
     */
    public double getAverageProcessingAbility() {
        List<Host> hosts = getHostList();
        if (hosts.isEmpty()) return 0.0;

        double totalMips = hosts.stream()
                .mapToDouble(Host::getTotalMipsCapacity)
                .sum();

        return totalMips / hosts.size();
    }

    /**
     * 获取当前CPU利用率
     */
    public double getCurrentCpuUtilization() {
        List<Host> hosts = getHostList();
        if (hosts.isEmpty()) return 0.0;

        double totalCapacity = 0.0;
        double totalAvailable = 0.0;

        for (Host host : hosts) {
            totalCapacity += host.getTotalMipsCapacity();
            totalAvailable += host.getTotalAvailableMips();
        }

        if (totalCapacity <= 0) return 0.0;

        return (totalCapacity - totalAvailable) / totalCapacity;
    }

    /**
     * 获取当前RAM利用率
     */
    public double getCurrentRamUtilization() {
        List<Host> hosts = getHostList();
        if (hosts.isEmpty()) return 0.0;

        double totalUtil = hosts.stream()
                .mapToDouble(h -> h.getRam().getPercentUtilization())
                .sum();

        return totalUtil / hosts.size();
    }

    /**
     * 获取平均RAM容量
     */
    public long getAverageRam() {
        List<Host> hosts = getHostList();
        if (hosts.isEmpty()) return 0;

        long totalRam = hosts.stream()
                .mapToLong(h -> h.getRam().getCapacity())
                .sum();

        return totalRam / hosts.size();
    }

    /**
     * 获取RAM利用率
     */
    public double getRamUtilization() {
        return getCurrentRamUtilization();
    }

    /**
     * 获取带宽利用率
     */
    public double getBwUtilization() {
        List<Host> hosts = getHostList();
        if (hosts.isEmpty()) return 0.0;

        double totalBw = hosts.stream()
                .mapToDouble(h -> h.getBw().getPercentUtilization())
                .sum();

        return totalBw / hosts.size();
    }

    /**
     * 获取综合负载
     */
    public double getOverallLoad() {
        double cpuLoad = getCurrentCpuUtilization();
        double ramLoad = getRamUtilization() / 100.0;
        double bwLoad = getBwUtilization() / 100.0;

        // 加权平均：CPU权重50%，RAM权重30%，带宽权重20%
        return 0.5 * cpuLoad + 0.3 * ramLoad + 0.2 * bwLoad;
    }

    // ========== 能源指标方法 ==========

    /**
     * 获取绿色能源使用比例
     */
    public double getGreenEnergyRatio() {
        synchronized (energyLock) {
            double total = totalGreenUsed + totalBrownUsed;
            return total > 0 ? totalGreenUsed / total : 1.0;
        }
    }

    /**
     * 获取当前绿色能源使用比例
     */
    public double getCurrentGreenEnergyRatio() {
        return getGreenEnergyRatio();
    }

    /**
     * 获取能源自给率
     */
    public double getEnergySelfSufficiency() {
        synchronized (energyLock) {
            if (lastTickTotalEnergyUsed <= 0) return 1.0;
            return Math.min(1.0, lastTickGreenGeneration / lastTickTotalEnergyUsed);
        }
    }

    /**
     * 获取当前绿色能源库存（焦耳）
     */
    public double getCurrentGreenEnergyStock() {
        synchronized (energyLock) {
            return greenEnergy;
        }
    }

    /**
     * 获取当前绿色能源库存（千瓦时）
     */
    public double getCurrentGreenEnergyStockKWh() {
        return getCurrentGreenEnergyStock() / 3600000.0;
    }

    /**
     * 获取上个tick的棕色能源使用量（千瓦时）
     */
    public double getLastTickBrownUsedKWh() {
        synchronized (energyLock) {
            return lastTickBrownUsed / 3600000.0;
        }
    }

    /**
     * 获取当前tick的盈余
     */
    public double getSurplusForCurrentTick() {
        synchronized (energyLock) {
            return lastTickSurplus;
        }
    }

    /**
     * 获取累积盈余
     */
    public double getCumulativeSurplus() {
        synchronized (energyLock) {
            return cumulativeSurplus;
        }
    }

    /**
     * 获取总生成量（千瓦时）
     */
    public double getTotalGeneratedKWh() {
        synchronized (energyLock) {
            return totalGenerated / 3600000.0;
        }
    }

    /**
     * 获取总绿色能源使用量（千瓦时）
     */
    public double getTotalGreenUsedKWh() {
        synchronized (energyLock) {
            return totalGreenUsed / 3600000.0;
        }
    }

    /**
     * 获取总棕色能源使用量（千瓦时）
     */
    public double getTotalBrownUsedKWh() {
        synchronized (energyLock) {
            return totalBrownUsed / 3600000.0;
        }
    }



    /**
     * 获取初始绿色能源（焦耳）
     */
    public double getInitalGreenEnergy() {
        return initialGreenEnergy;
    }


    /**
     * 获取总生成量（焦耳）
     */
    public double getTotalGenerate() {
        synchronized (energyLock) {
            return totalGenerated;
        }
    }

    /**
     * 设置生成缩放因子
     */
    public void setGenerationScalingFactor(double factor) {
        if (factor <= 0) {
            throw new IllegalArgumentException("Generation scaling factor must be positive");
        }
        synchronized (energyLock) {
            this.generationScalingFactor = factor;
            LOGGER.info("Generation scaling factor set to {}", factor);
        }
    }

    /**
     * 设置能源验证开关
     */
    public void setEnableEnergyValidation(boolean enable) {
        this.enableEnergyValidation = enable;
        LOGGER.info("Energy validation {}", enable ? "enabled" : "disabled");
    }

    /**
     * 获取数据中心状态摘要
     */
    public String getStatusSummary() {
        synchronized (energyLock) {
            return String.format(
                    "DC %d Status:\n" +
                            "  Green Energy: %.2f kWh (%.1f%%)\n" +
                            "  Wind Power: %.1f kW\n" +
                            "  Total Generated: %.2f kWh\n" +
                            "  Green Used: %.2f kWh\n" +
                            "  Brown Used: %.2f kWh\n" +
                            "  Green Ratio: %.1f%%\n" +
                            "  CPU Util: %.1f%%\n" +
                            "  RAM Util: %.1f%%\n" +
                            "  Overall Load: %.1f%%",
                    getId(),
                    greenEnergy / 3600000, (greenEnergy / initialGreenEnergy) * 100,
                    getCurrentGreenPower() / 1000,
                    totalGenerated / 3600000,
                    totalGreenUsed / 3600000,
                    totalBrownUsed / 3600000,
                    getGreenEnergyRatio() * 100,
                    getCurrentCpuUtilization() * 100,
                    getCurrentRamUtilization(),
                    getOverallLoad() * 100
            );
        }
    }
}