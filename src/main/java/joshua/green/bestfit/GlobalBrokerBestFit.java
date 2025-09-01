package joshua.green.bestfit;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.data.TimedCloudlet;

import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimEntity;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.core.events.SimEvent;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 改进的 Global Broker using Best-Fit 算法
 * 主要改进：
 * 1. 多因素综合评分机制
 * 2. 动态负载均衡
 * 3. 性能监控和统计
 * 4. 智能选择策略
 */
public class GlobalBrokerBestFit extends CloudSimEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalBrokerBestFit.class);

    // 核心组件
    private final List<DatacenterBrokerSimple> localBrokers = new ArrayList<>();
    private final List<Datacenter> datacenters = new ArrayList<>();

    // Cloudlet管理
    private final Queue<Cloudlet> cloudletQueue = new LinkedList<>();
    private final List<TimedCloudlet> timedCloudlets = new ArrayList<>();
    private int nextTimedIndex = 0;

    // 统计和监控
    private final Set<Long> submittedIds = new HashSet<>();
    private final Map<Integer, Integer> dcAssignmentCount = new HashMap<>();
    private final Map<Integer, Double> dcTotalResponseTime = new HashMap<>();
    private final Map<Integer, Integer> dcCompletedCount = new HashMap<>();

    // 配置参数
    private boolean terminateWhenAllDone = true;
    private int maxSubmitPerTick = Integer.MAX_VALUE;

    // 权重配置（可调整）
    private double greenEnergyWeight = 0.5;
    private double loadBalanceWeight = 0.3;
    private double queueLengthWeight = 0.2;

    // 性能统计
    private long totalSubmitted = 0;
    private long totalCompleted = 0;

    // 算法模式
    public enum Mode {
        GREEN_PRIORITY,    // 绿色能源优先
        LOAD_BALANCED,     // 负载均衡优先
        HYBRID            // 混合模式（默认）
    }

    private Mode currentMode = Mode.HYBRID;

    public GlobalBrokerBestFit(CloudSimPlus simulation) {
        super(simulation);
    }

    /* -------------------- 配置接口 -------------------- */

    public void addLocalBrokerWithDatacenter(DatacenterBrokerSimple broker, Datacenter dc) {
        int index = localBrokers.size();
        localBrokers.add(broker);
        datacenters.add(dc);
        dcAssignmentCount.put(index, 0);
        dcTotalResponseTime.put(index, 0.0);
        dcCompletedCount.put(index, 0);
    }

    public void setCloudletList(List<Cloudlet> list) {
        cloudletQueue.clear();
        cloudletQueue.addAll(list);
        LOGGER.info("Queued {} cloudlets (without timestamps)", list.size());
    }

    public void setTimedCloudlets(List<TimedCloudlet> list) {
        timedCloudlets.clear();
        timedCloudlets.addAll(list);
        timedCloudlets.sort(Comparator.comparingDouble(TimedCloudlet::getSubmissionTime));
        nextTimedIndex = 0;
        LOGGER.info("Queued {} timed cloudlets with timestamps", timedCloudlets.size());
    }

    public void setMode(Mode mode) {
        this.currentMode = mode;
        adjustWeightsForMode(mode);
    }

    public void setWeights(double green, double load, double queue) {
        double sum = green + load + queue;
        if (sum > 0) {
            this.greenEnergyWeight = green / sum;
            this.loadBalanceWeight = load / sum;
            this.queueLengthWeight = queue / sum;
        }
    }

    public void setTerminateWhenAllDone(boolean terminateWhenAllDone) {
        this.terminateWhenAllDone = terminateWhenAllDone;
    }

    public void setMaxSubmitPerTick(int maxSubmitPerTick) {
        this.maxSubmitPerTick = Math.max(1, maxSubmitPerTick);
    }

    /* -------------------- 改进的 Best-Fit 算法 -------------------- */

    /**
     * 核心算法：选择最佳数据中心
     */
    public void submitCloudlet(Cloudlet cl) {
        if (datacenters.isEmpty() || localBrokers.isEmpty()) {
            LOGGER.error("No datacenters/local brokers available for cloudlet {}", cl.getId());
            return;
        }

        // 选择最佳DC
        int bestIndex = selectBestDatacenter(cl);

        // 提交到选中的broker
        localBrokers.get(bestIndex).submitCloudlet(cl);
        submittedIds.add(cl.getId());
        dcAssignmentCount.merge(bestIndex, 1, Integer::sum);
        totalSubmitted++;

        // 记录决策
        logDecision(cl, bestIndex);
    }

    /**
     * 选择最佳数据中心的核心逻辑
     */
    private int selectBestDatacenter(Cloudlet cl) {
        double[] scores = new double[datacenters.size()];
        double maxScore = Double.NEGATIVE_INFINITY;
        int bestIndex = 0;

        // 收集统计信息用于负载均衡
        double avgAssignments = dcAssignmentCount.values().stream()
                .mapToInt(Integer::intValue)
                .average()
                .orElse(0.0);

        // 计算每个DC的综合得分
        for (int i = 0; i < datacenters.size(); i++) {
            scores[i] = calculateDatacenterScore(i, cl, avgAssignments);

            if (scores[i] > maxScore) {
                maxScore = scores[i];
                bestIndex = i;
            }
        }

        // 处理相同得分的情况（选择分配最少的）
        List<Integer> tiedIndices = new ArrayList<>();
        for (int i = 0; i < scores.length; i++) {
            if (Math.abs(scores[i] - maxScore) < 1e-9) {
                tiedIndices.add(i);
            }
        }

        if (tiedIndices.size() > 1) {
            bestIndex = tiedIndices.stream()
                    .min(Comparator.comparingInt(idx -> dcAssignmentCount.getOrDefault(idx, 0)))
                    .orElse(bestIndex);
        }

        return bestIndex;
    }

    /**
     * 计算数据中心的综合得分
     */
    private double calculateDatacenterScore(int dcIndex, Cloudlet cl, double avgAssignments) {
        Datacenter dc = datacenters.get(dcIndex);
        DatacenterBrokerSimple broker = localBrokers.get(dcIndex);

        double score = 0.0;

        // 1. 绿色能源得分
        double greenScore = calculateGreenScore(dc);

        // 2. 负载均衡得分
        double loadScore = calculateLoadScore(dcIndex, dc, avgAssignments);

        // 3. 队列长度得分（反向）
        int queueLength = broker.getCloudletWaitingList().size();
        double queueScore = 1.0 / (1.0 + queueLength * 0.05);

        // 4. 资源可用性得分
        double resourceScore = calculateResourceScore(dc, cl);

        // 综合得分计算
        score = greenEnergyWeight * greenScore
                + loadBalanceWeight * loadScore
                + queueLengthWeight * queueScore;

        // 资源可用性作为乘法因子（资源不足则大幅降低得分）
        score *= resourceScore;

        // 性能惩罚（如果该DC历史性能差）
        score *= calculatePerformancePenalty(dcIndex);

        return score;
    }

    /**
     * 计算绿色能源得分
     */
    private double calculateGreenScore(Datacenter dc) {
        if (!(dc instanceof DatacenterGreenAware)) {
            return 0.0;
        }

        DatacenterGreenAware greenDc = (DatacenterGreenAware) dc;
        double greenStock = greenDc.getCurrentGreenEnergyStock();

        // 简单归一化：假设最大库存为10000（可根据实际调整）
        double maxExpectedStock = 10000.0;
        double normalizedStock = Math.min(greenStock / maxExpectedStock, 1.0);

        // 非线性变换，增强差异
        return Math.pow(normalizedStock, 0.5);
    }

    /**
     * 计算负载均衡得分
     */
    private double calculateLoadScore(int dcIndex, Datacenter dc, double avgAssignments) {
        int currentAssignments = dcAssignmentCount.getOrDefault(dcIndex, 0);

        // 分配数量均衡得分
        double assignmentBalance = 1.0;
        if (avgAssignments > 0) {
            double deviation = Math.abs(currentAssignments - avgAssignments) / avgAssignments;
            assignmentBalance = Math.exp(-deviation);
        }

        // CPU利用率得分（目标：60-80%）
        double cpuScore = 0.5; // 默认值
        try {
            double avgCpuUtil = dc.getHostList().stream()
                    .mapToDouble(Host::getCpuPercentUtilization)
                    .average()
                    .orElse(0.5);

            if (avgCpuUtil < 0.6) {
                cpuScore = avgCpuUtil / 0.6;
            } else if (avgCpuUtil <= 0.8) {
                cpuScore = 1.0;
            } else {
                cpuScore = Math.max(0.3, 1.0 - (avgCpuUtil - 0.8) * 2);
            }
        } catch (Exception e) {
            // 如果无法获取CPU利用率，使用默认值
        }

        // VM数量得分
        double vmScore = 0.5;
        try {
            int totalVms = dc.getHostList().stream()
                    .mapToInt(host -> host.getVmList().size())
                    .sum();
            int maxVmsPerHost = 10; // 假设每个Host最多10个VM
            int maxVms = dc.getHostList().size() * maxVmsPerHost;
            if (maxVms > 0) {
                vmScore = 1.0 - (double) totalVms / maxVms;
            }
        } catch (Exception e) {
            // 使用默认值
        }

        return (assignmentBalance * 0.5 + cpuScore * 0.3 + vmScore * 0.2);
    }

    /**
     * 计算资源可用性得分
     */
    private double calculateResourceScore(Datacenter dc, Cloudlet cl) {
        try {
            // 检查是否有足够的处理能力
            long requiredMips = cl.getLength();

            // 获取可用MIPS（简化计算）
            double totalMips = dc.getHostList().stream()
                    .mapToDouble(host -> host.getMips())
                    .sum();

            if (totalMips < requiredMips) {
                return 0.1; // 资源严重不足
            }

            // 资源充足度得分
            double adequacy = Math.min(totalMips / (requiredMips * 5), 1.0);
            return 0.5 + adequacy * 0.5;

        } catch (Exception e) {
            return 0.8; // 出错时给予中等得分
        }
    }

    /**
     * 计算性能惩罚因子
     */
    private double calculatePerformancePenalty(int dcIndex) {
        int completed = dcCompletedCount.getOrDefault(dcIndex, 0);
        if (completed == 0) {
            return 1.0; // 没有历史数据，不惩罚
        }

        double avgResponseTime = dcTotalResponseTime.getOrDefault(dcIndex, 0.0) / completed;

        // 根据平均响应时间计算惩罚
        // 假设理想响应时间为100，超过200则开始惩罚
        if (avgResponseTime < 100) {
            return 1.1; // 轻微奖励
        } else if (avgResponseTime < 200) {
            return 1.0; // 不惩罚
        } else {
            return Math.max(0.5, 1.0 - (avgResponseTime - 200) / 1000);
        }
    }

    /**
     * 根据模式调整权重
     */
    private void adjustWeightsForMode(Mode mode) {
        switch (mode) {
            case GREEN_PRIORITY:
                greenEnergyWeight = 0.7;
                loadBalanceWeight = 0.2;
                queueLengthWeight = 0.1;
                break;
            case LOAD_BALANCED:
                greenEnergyWeight = 0.2;
                loadBalanceWeight = 0.6;
                queueLengthWeight = 0.2;
                break;
            case HYBRID:
            default:
                greenEnergyWeight = 0.5;
                loadBalanceWeight = 0.3;
                queueLengthWeight = 0.2;
                break;
        }
    }

    /**
     * 记录决策日志
     */
    private void logDecision(Cloudlet cl, int dcIndex) {
        double now = getSimulation().clock();
        Datacenter dc = datacenters.get(dcIndex);

        String greenInfo = "";
        if (dc instanceof DatacenterGreenAware) {
            DatacenterGreenAware greenDc = (DatacenterGreenAware) dc;
            greenInfo = String.format(" | Green: %.1f", greenDc.getCurrentGreenEnergyStock());
        }

        int queueLength = localBrokers.get(dcIndex).getCloudletWaitingList().size();
        int assignments = dcAssignmentCount.getOrDefault(dcIndex, 0);

        LOGGER.info("t={}: Cloudlet {} -> DC {} | Assignments: {} | Queue: {}{}",
                String.format("%.2f", now),
                cl.getId(),
                dcIndex,
                assignments,
                queueLength,
                greenInfo
        );
    }

    /* -------------------- 仿真控制 -------------------- */

    @Override
    protected void startInternal() {
        // 监听每个broker的cloudlet完成事件（手动方式）
        getSimulation().addOnClockTickListener(evt -> {
            // 更新完成统计
            updateCompletionStats();
        });

        getSimulation().addOnClockTickListener(evt -> {
            final double now = getSimulation().clock();
            int submittedThisTick = 0;

            // 提交带时间戳的任务
            while (nextTimedIndex < timedCloudlets.size()
                    && timedCloudlets.get(nextTimedIndex).getSubmissionTime() <= now
                    && submittedThisTick < maxSubmitPerTick) {

                TimedCloudlet tc = timedCloudlets.get(nextTimedIndex++);
                Cloudlet cl = tc.getCloudlet();

                if (!submittedIds.contains(cl.getId())) {
                    submitCloudlet(cl);
                    submittedThisTick++;
                }
            }

            // 兼容旧逻辑：无时间戳的任务
            if (timedCloudlets.isEmpty() && !cloudletQueue.isEmpty()
                    && submittedThisTick < maxSubmitPerTick) {
                Cloudlet cl = cloudletQueue.poll();
                if (cl != null) {
                    submitCloudlet(cl);
                }
            }

            // 检查是否完成所有任务
            checkTerminationCondition(now);

            // 定期输出统计信息
            if ((int) now % 100 == 0 && now > 0) {
                printStatistics();
            }
        });
    }

    /**
     * 更新完成统计
     */
    private void updateCompletionStats() {
        for (int i = 0; i < localBrokers.size(); i++) {
            DatacenterBrokerSimple broker = localBrokers.get(i);
            List<? extends Cloudlet> finishedList = broker.getCloudletFinishedList();

            // 统计新完成的任务
            int newCompleted = finishedList.size() - dcCompletedCount.getOrDefault(i, 0);
            if (newCompleted > 0) {
                dcCompletedCount.put(i, finishedList.size());
                totalCompleted += newCompleted;

                // 更新响应时间（简化：使用实际CPU时间作为响应时间的代理）
                double responseTime = finishedList.stream()
                        .skip(finishedList.size() - newCompleted)
                        .mapToDouble(Cloudlet::getTotalExecutionTime)
                        .sum();
                dcTotalResponseTime.merge(i, responseTime, Double::sum);
            }
        }
    }

    /**
     * 检查终止条件
     */
    private void checkTerminationCondition(double now) {
        if (!terminateWhenAllDone) return;

        boolean allSubmitted = timedCloudlets.isEmpty()
                ? cloudletQueue.isEmpty()
                : (nextTimedIndex >= timedCloudlets.size());

        if (allSubmitted) {
            int finished = localBrokers.stream()
                    .mapToInt(b -> b.getCloudletFinishedList().size())
                    .sum();

            if (finished >= submittedIds.size()) {
                LOGGER.info("All {} cloudlets completed at t={}. Terminating simulation.",
                        finished, String.format("%.2f", now));
                printFinalStatistics();
                getSimulation().terminate();
            }
        }
    }

    /**
     * 打印统计信息
     */
    private void printStatistics() {
        LOGGER.info("=== Periodic Statistics ===");
        LOGGER.info("Total Submitted: {} | Total Completed: {}", totalSubmitted, totalCompleted);

        for (int i = 0; i < datacenters.size(); i++) {
            Datacenter dc = datacenters.get(i);
            int assignments = dcAssignmentCount.getOrDefault(i, 0);
            int completed = dcCompletedCount.getOrDefault(i, 0);
            double avgResponse = completed > 0
                    ? dcTotalResponseTime.getOrDefault(i, 0.0) / completed
                    : 0;

            String dcInfo = String.format("DC%d: Assigned=%d, Completed=%d, AvgResponse=%.2f",
                    i, assignments, completed, avgResponse);

            if (dc instanceof DatacenterGreenAware) {
                DatacenterGreenAware greenDc = (DatacenterGreenAware) dc;
                dcInfo += String.format(", GreenStock=%.1f", greenDc.getCurrentGreenEnergyStock());
            }

            LOGGER.info(dcInfo);
        }
    }

    /**
     * 打印最终统计
     */
    private void printFinalStatistics() {
        LOGGER.info("=== Final Statistics ===");
        LOGGER.info("Mode: {} | Weights: Green={:.2f}, Load={:.2f}, Queue={:.2f}",
                currentMode, greenEnergyWeight, loadBalanceWeight, queueLengthWeight);
        printStatistics();

        // 计算负载均衡度
        if (!dcAssignmentCount.isEmpty()) {
            double avg = dcAssignmentCount.values().stream()
                    .mapToInt(Integer::intValue)
                    .average()
                    .orElse(0.0);

            double variance = dcAssignmentCount.values().stream()
                    .mapToDouble(count -> Math.pow(count - avg, 2))
                    .average()
                    .orElse(0.0);

            double stdDev = Math.sqrt(variance);
            double cv = avg > 0 ? stdDev / avg : 0;

            LOGGER.info("Load Balance: Avg={:.1f}, StdDev={:.1f}, CV={:.2f}",
                    avg, stdDev, cv);
        }
    }

    @Override
    public void processEvent(SimEvent simEvent) {
        // Not used
    }

    /* -------------------- Getters for monitoring -------------------- */

    public Map<Integer, Integer> getDcAssignmentCount() {
        return new HashMap<>(dcAssignmentCount);
    }

    public long getTotalSubmitted() {
        return totalSubmitted;
    }

    public long getTotalCompleted() {
        return totalCompleted;
    }

    public Mode getCurrentMode() {
        return currentMode;
    }
}