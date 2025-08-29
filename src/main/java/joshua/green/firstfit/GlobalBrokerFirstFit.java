package joshua.green.firstfit;

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
 * Global Broker using First-Fit to assign Cloudlets.
 * 支持两种提交方式：
 *  (1) setCloudletList(List<Cloudlet>)：无时间戳，按 tick 逐个提交（兼容旧行为）
 *  (2) setTimedCloudlets(List<TimedCloudlet>)：有时间戳，按 submissionTime 到点提交（推荐）
 *
 * First-Fit 选择策略：按 datacenters 注册顺序遍历，选第一个“满足条件”的 DC。
 * 这里默认条件为：绿色库存 > 0；若都不满足，则回退到 index 0。
 */
public class GlobalBrokerFirstFit extends CloudSimEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalBrokerFirstFit.class);

    private final List<DatacenterBrokerSimple> localBrokers = new ArrayList<>();
    private final List<Datacenter> datacenters = new ArrayList<>();

    // 旧：不带时间戳
    private final Queue<Cloudlet> cloudletQueue = new LinkedList<>();
    // 新：带时间戳
    private final List<TimedCloudlet> timedCloudlets = new ArrayList<>();
    private int nextTimedIndex = 0;

    private final Set<Long> submittedIds = new HashSet<>();
    private boolean terminateWhenAllDone = true;
    private int maxSubmitPerTick = Integer.MAX_VALUE;

    public GlobalBrokerFirstFit(CloudSimPlus simulation) {
        super(simulation);
    }

    /* -------------------- 配置接口 -------------------- */

    public void addLocalBrokerWithDatacenter(DatacenterBrokerSimple broker, Datacenter dc) {
        localBrokers.add(broker);
        datacenters.add(dc);
    }

    /** 旧接口：不带时间戳的 Cloudlet */
    public void setCloudletList(List<Cloudlet> list) {
        cloudletQueue.clear();
        cloudletQueue.addAll(list);
        LOGGER.info("Queued {} cloudlets (no timestamp)", list.size());
    }

    /** 新接口：带时间戳的 Cloudlet */
    public void setTimedCloudlets(List<TimedCloudlet> list) {
        timedCloudlets.clear();
        timedCloudlets.addAll(list);
        timedCloudlets.sort(Comparator.comparingDouble(TimedCloudlet::getSubmissionTime));
        nextTimedIndex = 0;
        LOGGER.info("Queued {} timed cloudlets (with submissionTime)", timedCloudlets.size());
    }

    public void setTerminateWhenAllDone(boolean b) { this.terminateWhenAllDone = b; }
    public void setMaxSubmitPerTick(int n) { this.maxSubmitPerTick = Math.max(1, n); }

    /* -------------------- First-Fit 提交 -------------------- */

    public void submitCloudlet(Cloudlet cl) {
        if (datacenters.isEmpty() || localBrokers.isEmpty()) {
            LOGGER.error("No datacenters/local brokers bound. Cannot submit cloudlet {}", cl.getId());
            return;
        }

        int chosen = pickFirstFitIndex();
        localBrokers.get(chosen).submitCloudlet(cl);
        submittedIds.add(cl.getId());

        double now = getSimulation().clock();
        LOGGER.info("t={}: Cloudlet {} -> Broker {} (First-Fit)", String.format("%.2f", now), cl.getId(), chosen);
    }

    /** First-Fit：选择第一个满足“绿色库存 > 0”的 DC；否则回退 index 0 */
    private int pickFirstFitIndex() {
        for (int i = 0; i < datacenters.size(); i++) {
            Datacenter dc = datacenters.get(i);
            if (dc instanceof DatacenterGreenAware g) {
                if (g.getGreenEnergy() > 0) return i;
            } else {
                // 若不是 GreenAware，就当作可用
                return i;
            }
        }
        return 0;
    }

    /* -------------------- 仿真启动：按时间驱动提交 -------------------- */

    @Override
    protected void startInternal() {
        getSimulation().addOnClockTickListener(evt -> {
            final double now = getSimulation().clock();
            int submittedThisTick = 0;

            // 1) 带时间戳：提交所有“到点”的任务（可限流）
            while (nextTimedIndex < timedCloudlets.size()
                    && timedCloudlets.get(nextTimedIndex).getSubmissionTime() <= now
                    && submittedThisTick < maxSubmitPerTick) {

                TimedCloudlet tc = timedCloudlets.get(nextTimedIndex++);
                Cloudlet cl = tc.getCloudlet();

                if (submittedIds.add(cl.getId())) {
                    submitCloudlet(cl);
                    submittedThisTick++;
                }
            }

            // 2) 兼容旧逻辑：如果没有 timedCloudlets，则从旧队列按 tick 提交
            if (timedCloudlets.isEmpty() && !cloudletQueue.isEmpty()
                    && submittedThisTick < maxSubmitPerTick) {
                Cloudlet cl = cloudletQueue.poll();
                if (cl != null) submitCloudlet(cl);
            }

            // 3) （可选）全部提交且全部完成 → 终止仿真
            if (terminateWhenAllDone) {
                boolean allSubmitted = timedCloudlets.isEmpty()
                        ? cloudletQueue.isEmpty()
                        : (nextTimedIndex >= timedCloudlets.size());

                if (allSubmitted) {
                    int finished = 0;
                    for (DatacenterBrokerSimple b : localBrokers) {
                        finished += b.getCloudletFinishedList().size();
                    }
                    if (finished >= submittedIds.size() && !submittedIds.isEmpty()) {
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
