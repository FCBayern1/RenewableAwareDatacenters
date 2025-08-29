package joshua.green.FedRL;

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

    // 添加数据中心列表引用
    private List<Datacenter> datacenters = new ArrayList<>();

    // 状态归一化器
    private final StateNormalizer normalizer = new StateNormalizer();

    // 调试计数器
    private int stateCallCount = 0;

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

    /**
     * 设置数据中心列表
     * 需要在初始化时调用
     */
    public void setDatacenters(List<Datacenter> datacenters) {
        this.datacenters = datacenters;
    }

    @Override
    protected void startInternal() {
        LOGGER.info("{} starting...", getName());
        distributeVMs();
    }

    private void distributeVMs() {
        int count = localBrokers.size();
        for (int i = 0; i < vmList.size(); i++) {
            localBrokers.get(i % count).submitVm(vmList.get(i));
        }
        LOGGER.info("Distributed {} VMs to {} Local Brokers", vmList.size(), count);
    }

    /**
     * 构建强化学习状态向量
     * 包含数据中心特征、任务特征和全局信息
     */
    public double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        // ===== Part 1: 数据中心特征 =====
        for (int i = 0; i < localBrokers.size(); i++) {
            LocalBrokerRL broker = localBrokers.get(i);
            Datacenter dc = broker.getLastSelectedDc();

            if (dc instanceof DatacenterGreenAware greenDC) {
                // 1. 绿色能源盈余（瞬时流量）
                double surplus = greenDC.getSurplusForCurrentTick();
                normalizer.updateObservation("dc" + i + "_surplus", surplus);
                state.add(normalizer.normalizeGreenSurplus(surplus));

                // 2. 绿色能源库存量
                double stock = greenDC.getCurrentGreenEnergyStock();
                normalizer.updateObservation("dc" + i + "_stock", stock);
                state.add(normalizer.normalizeGreenStock(stock));

                // 3. 平均处理速度 (Fdc_i)
                double avgMips = greenDC.getAverageProcessingAbility();
                normalizer.updateObservation("dc" + i + "_mips", avgMips);
                state.add(normalizer.normalizeMips(avgMips));

                // 4. 当前CPU利用率 (Udc_i)
                double cpuUtil = greenDC.getCurrentCpuUtilization();
                state.add(normalizer.normalizeCpuUtilization(cpuUtil));

                // 5. 队列长度（负载均衡指标）
                // 修复：使用正确的方法计算队列长度
                int queueLength = calculateQueueLength(broker);
                state.add(normalizer.normalizeQueueLength(queueLength));

                // 6. 预测的下一时刻绿色能源生成
                double predictedGen = greenDC.getPredictedGreenGeneration();
                state.add(normalizer.normalizeGreenStock(predictedGen));

            } else {
                // 数据中心不可用时的默认值
                state.add(0.0);  // surplus
                state.add(0.0);  // stock
                state.add(0.0);  // mips
                state.add(1.0);  // utilization (满载)
                state.add(1.0);  // queue (满)
                state.add(0.0);  // predicted generation
            }
        }

        // ===== Part 2: 任务特征 =====
        // 1. CPU需求 (t_cpu_j)
        double cpuReq = cl.getLength();
        normalizer.updateObservation("task_cpu", cpuReq);
        state.add(normalizer.normalizeCpuRequirement(cpuReq));

        // 2. 内存需求 (t_mem_j)
        double memReq = getTaskMemoryRequirement(cl);
        normalizer.updateObservation("task_mem", memReq);
        state.add(normalizer.normalizeMemRequirement(memReq));

        // ===== Part 3: 全局信息 =====
        // 1. 当前仿真时间
        double currentTime = getSimulation().clock();
        state.add(normalizer.normalizeTime(currentTime));

        // 2. 全局绿色能源利用率
        double globalGreenRatio = calculateGlobalGreenRatio();
        state.add(globalGreenRatio);

        // 调试：定期打印统计信息
        if (++stateCallCount % 1000 == 0) {
            normalizer.printStatistics();
            LOGGER.info("State dimension: {} for cloudlet {}", state.size(), cl.getId());
            validateState(state.stream().mapToDouble(Double::doubleValue).toArray());
        }

        return state.stream().mapToDouble(Double::doubleValue).toArray();
    }

    /**
     * 计算本地调度器的队列长度
     */
    private int calculateQueueLength(LocalBrokerRL broker) {
        // 正在执行和等待的任务总数
        int submittedCount = broker.getCloudletSubmittedList().size();
        int finishedCount = broker.getCloudletFinishedList().size();
        int waitingCount = broker.getCloudletWaitingList().size();

        // 执行中的任务 = 已提交 - 已完成
        int executingCount = submittedCount - finishedCount;

        // 总队列长度 = 等待中 + 执行中
        return waitingCount + executingCount;
    }

    /**
     * 获取任务的内存需求
     */
    private double getTaskMemoryRequirement(Cloudlet cl) {
        double memReq = cl.getUtilizationOfRam();

        // 如果是百分比（0-1之间），转换为实际内存需求
        if (memReq <= 1.0) {
            // 假设VM标准内存为2GB
            memReq = memReq * 2048;
        }

        return memReq;
    }

    /**
     * 计算全局绿色能源利用率
     * 使用数据中心列表或通过本地调度器获取
     */
    private double calculateGlobalGreenRatio() {
        double totalGreenUsed = 0;
        double totalEnergyUsed = 0;

        // 优先使用数据中心列表
        if (!datacenters.isEmpty()) {
            for (Datacenter dc : datacenters) {
                if (dc instanceof DatacenterGreenAware greenDC) {
                    totalGreenUsed += greenDC.getTotalGreenUsed();
                    totalEnergyUsed += greenDC.getTotalGreenUsed() + greenDC.getTotalBrownUsed();
                }
            }
        } else {
            // 备选方案：通过本地调度器获取
            for (LocalBrokerRL broker : localBrokers) {
                Datacenter dc = broker.getLastSelectedDc();
                if (dc instanceof DatacenterGreenAware greenDC) {
                    totalGreenUsed += greenDC.getTotalGreenUsed();
                    totalEnergyUsed += greenDC.getTotalGreenUsed() + greenDC.getTotalBrownUsed();
                }
            }
        }

        return totalEnergyUsed > 0 ? totalGreenUsed / totalEnergyUsed : 0;
    }

    /**
     * 验证状态向量的有效性
     */
    private void validateState(double[] state) {
        for (int i = 0; i < state.length; i++) {
            if (Double.isNaN(state[i]) || Double.isInfinite(state[i])) {
                LOGGER.error("Invalid state value at index {}: {}", i, state[i]);
                state[i] = 0.0; // 使用默认值
            }
            // 检查是否在合理范围内
            if (Math.abs(state[i]) > 10.0) {
                LOGGER.warn("State value at index {} might be too large: {}", i, state[i]);
            }
        }
    }

    /**
     * 提交任务到指定的本地调度器
     */
    public void submitCloudlet(Cloudlet cl, int action) {
        if (action < 0 || action >= localBrokers.size()) {
            LOGGER.error("Invalid Local Broker index {} for Cloudlet {}", action, cl.getId());
            return;
        }

        LocalBrokerRL broker = localBrokers.get(action);
        broker.submitCloudlet(cl);

        LOGGER.info("GlobalBrokerRL: Cloudlet {} submitted to LocalBroker {} (DC {}) at time {}",
                cl.getId(), broker.getId(), action, getSimulation().clock());
    }

    /**
     * 获取状态向量的维度
     * 用于Python端配置
     */
    public int getStateDimension() {
        // 每个DC有6个特征，加上任务2个特征，加上全局2个特征
        return localBrokers.size() * 6 + 2 + 2;
    }

    @Override
    public void processEvent(SimEvent simEvent) {
        // 暂时不需要处理内部事件
    }

    /**
     * 获取调试信息
     */
    public String getDebugInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("GlobalBroker Status:\n");
        sb.append("  Local Brokers: ").append(localBrokers.size()).append("\n");
        sb.append("  VMs: ").append(vmList.size()).append("\n");
        sb.append("  Cloudlets in queue: ").append(cloudletQueue.size()).append("\n");
        sb.append("  State dimension: ").append(getStateDimension()).append("\n");
        return sb.toString();
    }
}