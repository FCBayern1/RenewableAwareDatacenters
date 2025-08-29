package joshua.green.bestfit;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.data.TimedCloudlet;

import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimEntity;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.core.events.SimEvent;
import org.cloudsimplus.datacenters.Datacenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Global Broker using Best-Fit (pick the greenest Datacenter) to assign Cloudlets.
 * 支持两种提交方式：
 *  (1) setCloudletList(List<Cloudlet>)：无时间戳，按 tick 逐个提交（兼容旧行为）
 *  (2) setTimedCloudlets(List<TimedCloudlet>)：有时间戳，按 submissionTime 到点提交（推荐）
 */
public class GlobalBrokerBestFit extends CloudSimEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalBrokerBestFit.class);

    private final List<DatacenterBrokerSimple> localBrokers = new ArrayList<>();
    private final List<Datacenter> datacenters = new ArrayList<>();

    /** 无时间戳的 Cloudlet 队列（兼容旧逻辑） */
    private final Queue<Cloudlet> cloudletQueue = new LinkedList<>();
    /** 有时间戳的 Cloudlet 列表（推荐） */
    private final List<TimedCloudlet> timedCloudlets = new ArrayList<>();
    private int nextTimedIndex = 0;

    /** 已提交 Cloudlet 的 id 集合（去重/统计） */
    private final Set<Long> submittedIds = new HashSet<>();
    /** 是否在“全部提交且全部完成”后自动终止仿真 */
    private boolean terminateWhenAllDone = true;
    /** 可选：限制每个 tick 最多提交多少个（防止同刻大量涌入），默认不限 */
    private int maxSubmitPerTick = Integer.MAX_VALUE;

    public GlobalBrokerBestFit(CloudSimPlus simulation) {
        super(simulation);
    }

    /* -------------------- 配置接口 -------------------- */

    public void addLocalBrokerWithDatacenter(DatacenterBrokerSimple broker, Datacenter dc) {
        localBrokers.add(broker);
        datacenters.add(dc);
    }


    /** 新接口：带时间戳的 Cloudlet 列表 */
    public void setTimedCloudlets(List<TimedCloudlet> list) {
        timedCloudlets.clear();
        timedCloudlets.addAll(list);
        // 按提交时间排序，保证线性扫描即可
        timedCloudlets.sort(Comparator.comparingDouble(TimedCloudlet::getSubmissionTime));
        nextTimedIndex = 0;
        LOGGER.info("Queued {} timed cloudlets (with submissionTime)", timedCloudlets.size());
    }

    public void setTerminateWhenAllDone(boolean terminateWhenAllDone) {
        this.terminateWhenAllDone = terminateWhenAllDone;
    }

    public void setMaxSubmitPerTick(int maxSubmitPerTick) {
        this.maxSubmitPerTick = Math.max(1, maxSubmitPerTick);
    }

    /* -------------------- Best-Fit 提交 -------------------- */

    /** 选择“最绿”的 DC，并把 cloudlet 交给对应的本地 broker */
    public void submitCloudlet(Cloudlet cl) {
        if (datacenters.isEmpty() || localBrokers.isEmpty()) {
            LOGGER.error("No datacenters/local brokers bound. Cannot submit cloudlet {}", cl.getId());
            return;
        }

        int bestIndex = 0;
        double bestScore = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < datacenters.size(); i++) {
            Datacenter dc = datacenters.get(i);
            double greenStock = 0.0;

            if (dc instanceof DatacenterGreenAware greenDc) {
                // 与你现有代码保持一致：用“当前绿色库存”做主指标
                greenStock = greenDc.getCurrentGreenEnergyStock();
            }

            double score = greenStock;

            // tie-break：同分时选“等待队列更短”的 broker（更均衡）
            if (Math.abs(score - bestScore) < 1e-9) {
                int qBest = localBrokers.get(bestIndex).getCloudletWaitingList().size();
                int qCur  = localBrokers.get(i).getCloudletWaitingList().size();
                if (qCur < qBest) bestIndex = i;
            } else if (score > bestScore) {
                bestScore = score;
                bestIndex = i;
            }
        }

        localBrokers.get(bestIndex).submitCloudlet(cl);
        submittedIds.add(cl.getId());

        double now = getSimulation().clock();
        LOGGER.info("t={}: Cloudlet {} -> Broker {} (best green score = {})",
                String.format("%.2f", now), cl.getId(), bestIndex,
                String.format("%.3f", bestScore));
    }

    /* -------------------- 仿真启动：按时间驱动提交 -------------------- */

    @Override
    protected void startInternal() {
        getSimulation().addOnClockTickListener(evt -> {
            final double now = getSimulation().clock();
            int submittedThisTick = 0;

            /* 1) 若使用“带时间戳”的列表：提交所有“到点”的任务（可限流） */
            while (nextTimedIndex < timedCloudlets.size()
                    && timedCloudlets.get(nextTimedIndex).getSubmissionTime() <= now
                    && submittedThisTick < maxSubmitPerTick) {

                TimedCloudlet tc = timedCloudlets.get(nextTimedIndex++);
                Cloudlet cl = tc.getCloudlet();

                if (submittedIds.contains(cl.getId())) {
                    continue; // 防御：避免重复提交
                }
                submitCloudlet(cl);
                submittedThisTick++;
            }

            /* 2) 兼容旧逻辑：如果没有 timedCloudlets，可从简单队列里按 tick 提交 */
            if (timedCloudlets.isEmpty() && !cloudletQueue.isEmpty()
                    && submittedThisTick < maxSubmitPerTick) {
                Cloudlet cl = cloudletQueue.poll();
                if (cl != null) {
                    submitCloudlet(cl);
                }
            }

            /* 3) 可选：所有提交完且全部完成 → 终止仿真 */
            if (terminateWhenAllDone) {
                int totalPlanned = timedCloudlets.isEmpty()
                        ? (submittedIds.size() + cloudletQueue.size()) // 只能估个下界
                        : timedCloudlets.size();

                boolean allSubmitted = timedCloudlets.isEmpty()
                        ? cloudletQueue.isEmpty()
                        : (nextTimedIndex >= timedCloudlets.size());

                if (allSubmitted) {
                    int finished = 0;
                    for (DatacenterBrokerSimple b : localBrokers) {
                        finished += b.getCloudletFinishedList().size();
                    }
                    if (finished >= submittedIds.size() && submittedIds.size() >= totalPlanned) {
                        LOGGER.info("All cloudlets submitted and finished. Terminating simulation at t={}",
                                String.format("%.2f", now));
                        getSimulation().terminate();
                    }
                }
            }
        });
    }

    @Override
    public void processEvent(SimEvent simEvent) {
        // Not used
    }
}
