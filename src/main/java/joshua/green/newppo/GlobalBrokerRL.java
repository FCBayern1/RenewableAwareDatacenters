package joshua.green.newppo;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.StateNormalizer;

import lombok.Getter;
import lombok.Setter;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimEntity;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.core.events.SimEvent;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleConsumer;


@Getter
@Setter
public class GlobalBrokerRL extends CloudSimEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalBrokerRL.class);

    /* === 外部回调 & 内部累计（per-episode） === */
    private DoubleConsumer rewardSink;
    private double episodeRewardSum = 0.0;
    public void resetEpisodeRewardSum() { episodeRewardSum = 0.0; }

    /* === 组件 & 状态 === */
    private final List<LocalBrokerRL> localBrokers = new ArrayList<>();
    private final RLClient rlClient;
    private final Queue<Cloudlet> cloudletQueue = new LinkedList<>();
    private final List<Vm> vmList = new ArrayList<>();
    private List<Datacenter> datacenters = new ArrayList<>();
    private final StateNormalizer normalizer = new StateNormalizer();
    private int stateCallCount = 0;

    /* === Pending 结构：动作时刻的快照 === */
    private static class PendingExp {
        double[] state;      // 动作时的状态
        int action;          // 选中的 LocalBroker / DC 索引
        double ts;           // 动作时刻（schedule时）
        // 能耗快照（系统/所选DC）
        double sysGreenStart, sysTotalStart;
        double dcGreenStart, dcTotalStart;
        // PPO 用（可选）
        double logProb;
        double value;
    }
    private final Map<Long, PendingExp> pendingMap = new ConcurrentHashMap<>();
    private final Map<Integer, Double> dcGreenRatioEma = new ConcurrentHashMap<>();

    private static final double W1 = 0.6;
    private static final double W2 = 0.2;
    private static final double W3 = 0.2;
    private static final double TIME_SCALE = 100.0;
    private static final double EMA_ALPHA = 0.05;

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

    public void setVmList(List<Vm> list) {
        vmList.addAll(list);
    }

    public void setCloudletList(List<Cloudlet> list) {
        cloudletQueue.addAll(list);
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

    public double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        for (int i = 0; i < localBrokers.size(); i++) {
            LocalBrokerRL broker = localBrokers.get(i);
            Datacenter dc = broker.getLastSelectedDc();

            if (dc instanceof DatacenterGreenAware greenDC) {
                double surplus = greenDC.getSurplusForCurrentTick();
                normalizer.updateObservation("dc" + i + "_surplus", surplus);
                state.add(normalizer.normalizeGreenSurplus(surplus));

                double stock = greenDC.getCurrentGreenEnergyStock();
                normalizer.updateObservation("dc" + i + "_stock", stock);
                state.add(normalizer.normalizeGreenStock(stock));

                double avgMips = greenDC.getAverageProcessingAbility();
                normalizer.updateObservation("dc" + i + "_mips", avgMips);
                state.add(normalizer.normalizeMips(avgMips));

                double cpuUtil = greenDC.getCurrentCpuUtilization();
                state.add(normalizer.normalizeCpuUtilization(cpuUtil));

                int queueLength = calculateQueueLength(broker);
                state.add(normalizer.normalizeQueueLength(queueLength));
            } else {
                // 无绿色增强 DC 的兜底
                state.add(0.0); // surplus
                state.add(0.0); // stock
                state.add(0.0); // mips
                state.add(1.0); // util
                state.add(1.0); // queue
            }
        }

        // 任务特征
        double cpuReq = cl.getLength();
        normalizer.updateObservation("task_cpu", cpuReq);
        state.add(normalizer.normalizeCpuRequirement(cpuReq));

        double memReq = getTaskMemoryRequirement(cl);
        normalizer.updateObservation("task_mem", memReq);
        state.add(normalizer.normalizeMemRequirement(memReq));

        // 全局信息
        double currentTime = getSimulation().clock();
        state.add(normalizer.normalizeTime(currentTime));

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

    private int calculateQueueLength(LocalBrokerRL broker) {
        int submittedCount = broker.getCloudletSubmittedList().size();
        int finishedCount = broker.getCloudletFinishedList().size();
        int waitingCount = broker.getCloudletWaitingList().size();
        int executingCount = submittedCount - finishedCount;
        return waitingCount + executingCount;
    }

    private double getTaskMemoryRequirement(Cloudlet cl) {
        double memReq = cl.getUtilizationOfRam();
        if (memReq <= 1.0) { // 百分比则换算为实际值（这里假设标准VM内存2GB）
            memReq = memReq * 2048;
        }
        return memReq;
    }

    private double calculateGlobalGreenRatio() {
        double totalGreenUsed = 0;
        double totalEnergyUsed = 0;

        if (!datacenters.isEmpty()) {
            for (Datacenter dc : datacenters) {
                if (dc instanceof DatacenterGreenAware greenDC) {
                    totalGreenUsed += greenDC.getTotalGreenUsed();
                    totalEnergyUsed += greenDC.getTotalGreenUsed() + greenDC.getTotalBrownUsed();
                }
            }
        } else {
            for (LocalBrokerRL broker : localBrokers) {
                Datacenter dc = broker.getLastSelectedDc();
                if (dc instanceof DatacenterGreenAware greenDC) {
                    totalGreenUsed += greenDC.getTotalGreenUsed();
                    totalEnergyUsed += greenDC.getTotalGreenUsed() + greenDC.getTotalBrownUsed();
                }
            }
        }
        return totalEnergyUsed > 0 ? totalGreenUsed / totalEnergyUsed : 0.0;
    }

    private void validateState(double[] state) {
        for (int i = 0; i < state.length; i++) {
            if (Double.isNaN(state[i]) || Double.isInfinite(state[i])) {
                LOGGER.error("Invalid state value at index {}: {}", i, state[i]);
                state[i] = 0.0; // 默认值
            }
            if (Math.abs(state[i]) > 10.0) {
                LOGGER.warn("State value at index {} might be too large: {}", i, state[i]);
            }
        }
    }

    /* ===================== 系统/DC 能耗快照读取 ===================== */
    private double getSystemGreenUsed() {
        double sum = 0.0;
        if (!datacenters.isEmpty()) {
            for (Datacenter dc : datacenters) {
                if (dc instanceof DatacenterGreenAware g) sum += g.getTotalGreenUsed();
            }
        } else {
            for (LocalBrokerRL b : localBrokers) {
                Datacenter dc = b.getLastSelectedDc();
                if (dc instanceof DatacenterGreenAware g) sum += g.getTotalGreenUsed();
            }
        }
        return sum;
    }

    private double getSystemTotalUsed() {
        double sum = 0.0;
        if (!datacenters.isEmpty()) {
            for (Datacenter dc : datacenters) {
                if (dc instanceof DatacenterGreenAware g) {
                    sum += g.getTotalGreenUsed() + g.getTotalBrownUsed();
                }
            }
        } else {
            for (LocalBrokerRL b : localBrokers) {
                Datacenter dc = b.getLastSelectedDc();
                if (dc instanceof DatacenterGreenAware g) {
                    sum += g.getTotalGreenUsed() + g.getTotalBrownUsed();
                }
            }
        }
        return sum;
    }

    private double getDcGreenUsedByIndex(int idx) {
        if (idx < 0 || idx >= localBrokers.size()) return 0.0;
        Datacenter dc = localBrokers.get(idx).getLastSelectedDc();
        return (dc instanceof DatacenterGreenAware g) ? g.getTotalGreenUsed() : 0.0;
    }

    private double getDcTotalUsedByIndex(int idx) {
        if (idx < 0 || idx >= localBrokers.size()) return 0.0;
        Datacenter dc = localBrokers.get(idx).getLastSelectedDc();
        if (dc instanceof DatacenterGreenAware g) {
            return g.getTotalGreenUsed() + g.getTotalBrownUsed();
        }
        return 0.0;
    }

    /* ===================== 提交任务（对齐奖励） ===================== */

    /**
     * 旧签名：保持兼容；内部会构建 state，logProb/value 置 0 兜底。
     */
    public void submitCloudlet(Cloudlet cl, int action) {
        if (action < 0 || action >= localBrokers.size()) {
            LOGGER.error("Invalid Local Broker index {} for Cloudlet {}", action, cl.getId());
            return;
        }
        double[] state = buildState(cl);
        submitCloudlet(cl, state, action, 0.0, 0.0);
    }

    /**
     * 新签名：在动作时刻记录 pending；注册完成回调；按 [ts->tf] 结算 reward 并上报。
     */
    public void submitCloudlet(Cloudlet cl, double[] state, int action, double logProb, double value) {
        if (action < 0 || action >= localBrokers.size()) {
            LOGGER.error("Invalid Local Broker index {} for Cloudlet {}", action, cl.getId());
            return;
        }

        final double now = getSimulation().clock();

        // 1) 记录 pending
        PendingExp exp = new PendingExp();
        exp.state = state != null ? state.clone() : null;
        exp.action = action;
        exp.ts = now;

        exp.sysGreenStart = getSystemGreenUsed();
        exp.sysTotalStart = getSystemTotalUsed();
        exp.dcGreenStart  = getDcGreenUsedByIndex(action);
        exp.dcTotalStart  = getDcTotalUsedByIndex(action);

        exp.logProb = logProb;
        exp.value   = value;

        pendingMap.put(cl.getId(), exp);

        // 2) 注册完成回调：完成即结算 reward
        cl.addOnFinishListener(info -> {
            Cloudlet finished = info.getCloudlet();
            PendingExp p = pendingMap.remove(finished.getId());
            if (p == null) {
                LOGGER.warn("Global pending missing for Cloudlet {}", finished.getId());
                return;
            }

            final double tf = info.getTime();
            final double r_global = computeGlobalReward(p, tf, p.action);

            episodeRewardSum += r_global;
            if (rewardSink != null) {
                rewardSink.accept(r_global);
            } else {
                LOGGER.debug("rewardSink is null, r_global={} not emitted", r_global);
            }
            LOGGER.debug("GlobalReward: cl={}, r_global={}, episodeSumNow={}",
                    finished.getId(),
                    String.format("%.6f", r_global),
                    String.format("%.6f", episodeRewardSum));

            // next state：用完成时刻的快照
            double[] nextState = buildState(finished);

            try {
                rlClient.storeExperience(
                        p.state != null ? p.state : new double[]{},
                        p.action,
                        r_global,
                        nextState,
                        false,          // done: 由外部控制 episode 终止
                        p.logProb,
                        p.value
                );
            } catch (Exception e) {
                LOGGER.error("storeExperience (global) failed for Cloudlet {}: {}", finished.getId(), e.getMessage());
            }
        });

        // 3) 实际把任务交给对应 LocalBroker
        LocalBrokerRL broker = localBrokers.get(action);
        broker.submitCloudlet(cl);

        LOGGER.info("GlobalBrokerRL: Cloudlet {} -> LocalBroker {} (DC {}) at t={}",
                cl.getId(), broker.getId(), action, now);
    }

    public int getStateDimension() {
        // 每个DC有5个特征，加上任务2个特征，加上全局2个特征
        return localBrokers.size() * 5 + 2 + 2;
    }

    @Override
    public void processEvent(SimEvent simEvent) {
        // 暂时不需要处理内部事件
    }

    /**
     * 计算 Global 层的 reward。
     * @param p  pendingExp，包含动作时刻的能耗快照、state 等
     * @param tf Cloudlet 完成时刻
     * @param action 动作（选择的 datacenter index）
     * @return 该 cloudlet 的全局 reward
     */
    private double computeGlobalReward(PendingExp p, double tf, int action) {
        // 1) 窗口时长
        final double duration = Math.max(0.0, tf - p.ts);
        final double timePenalty = Math.min(1.0, duration / TIME_SCALE);

        // 2) 系统窗口切片
        double dG_sys = getSystemGreenUsed() - p.sysGreenStart;
        double dE_sys = getSystemTotalUsed() - p.sysTotalStart;
        double ratio_sys = (dE_sys > 0) ? (dG_sys / dE_sys) : 0.0;

        // 3) 所选 DC 窗口切片
        double dG_dc = getDcGreenUsedByIndex(action) - p.dcGreenStart;
        double dE_dc = getDcTotalUsedByIndex(action) - p.dcTotalStart;
        double ratio_dc = (dE_dc > 0) ? (dG_dc / dE_dc) : 0.0;

        // 4) DC EMA 基线
        double baseline = dcGreenRatioEma.getOrDefault(action, 0.0);
        double newBaseline = (1.0 - EMA_ALPHA) * baseline + EMA_ALPHA * ratio_dc;
        dcGreenRatioEma.put(action, newBaseline);

        // 5) 最终 reward 组合
        double r = W1 * ratio_sys
                + W2 * (ratio_dc - baseline)
                - W3 * timePenalty;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[Reward] dur={}, dG_sys={}, dE_sys={}, ratio_sys={}, dG_dc={}, dE_dc={}, ratio_dc={}, base(old)->{}, r={}",
                    String.format("%.3f", duration),
                    String.format("%.3f", dG_sys),
                    String.format("%.3f", dE_sys),
                    String.format("%.3f", ratio_sys),
                    String.format("%.3f", dG_dc),
                    String.format("%.3f", dE_dc),
                    String.format("%.3f", ratio_dc),
                    String.format("%.3f", baseline),
                    String.format("%.6f", r));
        }
        return r;
    }

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
