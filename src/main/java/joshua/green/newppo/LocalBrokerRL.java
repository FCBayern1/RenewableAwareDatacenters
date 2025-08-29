package joshua.green.newppo;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.StateNormalizer;
import lombok.Getter;
import lombok.NonNull;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local Broker with Reinforcement Learning control over VM selection.
 * 负责在数据中心内部将任务分配到具体的主机
 */
public class LocalBrokerRL extends DatacenterBrokerSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalBrokerRL.class);

    private final RLClient rlClient;
    private final List<Host> hosts = new ArrayList<>();

    private final StateNormalizer normalizer = new StateNormalizer();

    @Getter private final Map<Long, double[]> stateMap  = new HashMap<>();
    @Getter private final Map<Long, Integer>  actionMap = new HashMap<>();
    @Getter private final Map<Long, Double>   probLogMap = new HashMap<>();
    @Getter private final Map<Long, Double>   valueMap = new HashMap<>();

    private static class PendingExp {
        double[] state;
        int action;
        double logProb;
        double value;

        double ts;
        long hostId = -1;
        double dcGreenStart, dcTotalStart;
        int dcIndex = -1;
    }
    private final Map<Long, PendingExp> pendingMap = new ConcurrentHashMap<>();

    private static final double A1 = 0.40; // Wait time penalty
    private static final double A2 = 0.40; // Execution penalty
    private static final double A3 = 0.20; // Total Energy Consumption penalty（DC）
    private static final double A4 = 0.10; // Green Energy Consumption Increase（DC）
    private static final double TIME_SCALE_LOCAL = 100.0; // Scale the time to 1-100
    private static final double ENERGY_SCALE = 1.0;

    private int stateCallCount = 0;

    public LocalBrokerRL(CloudSimPlus simulation, RLClient rlClient, List<Host> hosts) {
        super(simulation);
        this.rlClient = rlClient;
        this.hosts.addAll(hosts);
    }

    @Override
    public DatacenterBroker submitCloudlet(@NonNull Cloudlet cloudlet) {
        // build the current state of submission
        double[] state = buildState(cloudlet);

        // select the host
        RLClient.ActionResponse result = rlClient.selectActionLocal(state, hosts.size());
        int action   = result.action;
        double logProb = result.log_prob;
        double value   = result.value;

        // select the VM for the cloudlet
        Host selectedHost = null;
        if (action >= 0 && action < hosts.size()) {
            selectedHost = hosts.get(action);
            if (selectedHost != null) {
                Vm vm = findBestVm(selectedHost);
                if (vm != null) {
                    cloudlet.setVm(vm);
                    LOGGER.debug("Local: Cloudlet {} -> VM {} on Host {}",
                            cloudlet.getId(), vm.getId(), selectedHost.getId());
                } else {
                    LOGGER.warn("Local: No suitable VM on Host {} for Cloudlet {}",
                            selectedHost.getId(), cloudlet.getId());
                }
            }
        } else {
            LOGGER.error("Local: Invalid action {} for {} hosts", action, hosts.size());
        }

        // 4) 记录 pending（动作-奖励对齐）
        final double now = getSimulation().clock();
        PendingExp exp = new PendingExp();
        exp.state = state != null ? state.clone() : null;
        exp.action = action;
        exp.logProb = logProb;
        exp.value = value;
        exp.ts = now;
        if (selectedHost != null) exp.hostId = selectedHost.getId();

        Datacenter dc = getLastSelectedDc(); // 当前 broker 对应的 DC
        if (dc instanceof DatacenterGreenAware g) {
            exp.dcGreenStart = g.getTotalGreenUsed();
            exp.dcTotalStart = g.getTotalGreenUsed() + g.getTotalBrownUsed();
            // 如果你维护了 DC 索引，可在构造时注入，这里简化未设置
        }
        pendingMap.put(cloudlet.getId(), exp);

        // 5) 注册完成回调：结算本地 reward 并上报
        cloudlet.addOnFinishListener(info -> {
            Cloudlet finished = info.getCloudlet();
            PendingExp p = pendingMap.remove(finished.getId());
            if (p == null) {
                LOGGER.warn("Local: pending missing for Cloudlet {}", finished.getId());
                return;
            }

            double tf = info.getTime();
            double rLocal = computeLocalReward(p, finished, tf);

            // next state：用完成时刻快照
            double[] nextState = buildState(finished);

            try {
                rlClient.storeExperienceLocal(
                        p.state != null ? p.state : new double[]{},
                        p.action,
                        rLocal,
                        nextState,
                        false,      // done
                        p.logProb,
                        p.value
                );
            } catch (Exception e) {
                LOGGER.error("storeExperienceLocal failed for Cloudlet {}: {}", finished.getId(), e.getMessage());
            }
        });

        // （保留）存储决策信息：便于你旧的统计代码继续工作
        long cloudletId = cloudlet.getId();
        stateMap.put(cloudletId, state);
        actionMap.put(cloudletId, action);
        probLogMap.put(cloudletId, logProb);
        valueMap.put(cloudletId, value);

        // 6) 提交给父类（触发 CloudSim 调度）
        return super.submitCloudlet(cloudlet);
    }

    /**
     * 计算本地奖励（完成即结算，窗口 [ts -> tf]）
     * 公式：
     *   r_local = -A1 * norm(T_wait) - A2 * norm(T_exec) - A3 * norm(ΔE_dc) + A4 * norm(ΔG_dc)
     */
    private double computeLocalReward(PendingExp p, Cloudlet cl, double tf) {
        // 1) 时间项（转为 0~1 小量）
        double T_wait = Math.max(0.0, cl.getStartWaitTime());
        double T_exec = Math.max(0.0, cl.getTotalExecutionTime()); // 或 cl.getTotalExecutionTime()
        double waitPenalty = Math.min(1.0, T_wait / TIME_SCALE_LOCAL);
        double execPenalty = Math.min(1.0, T_exec / TIME_SCALE_LOCAL);

        // 2) DC 能耗增量（Host 粒度若不可得则用 DC 粒度）
        double dE_dc = 0.0, dG_dc = 0.0;
        Datacenter dc = getLastSelectedDc();
        if (dc instanceof DatacenterGreenAware g) {
            double dcGreenEnd = g.getTotalGreenUsed();
            double dcTotalEnd = g.getTotalGreenUsed() + g.getTotalBrownUsed();
            dG_dc = Math.max(0.0, dcGreenEnd - p.dcGreenStart);
            dE_dc = Math.max(0.0, dcTotalEnd - p.dcTotalStart);
        }
        // 简单缩放（如有需要可改用分位数归一化/标准化）
        double energyPenalty = Math.min(1.0, (dE_dc / Math.max(1e-9, ENERGY_SCALE)));
        double greenBonus    = Math.min(1.0, (dG_dc / Math.max(1e-9, ENERGY_SCALE)));

        // 3) 组合
        double reward = - A1 * waitPenalty
                - A2 * execPenalty
                - A3 * energyPenalty
                + A4 * greenBonus;

        if (Double.isNaN(reward) || Double.isInfinite(reward)) reward = 0.0;
        return reward;
    }

    protected double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        // ===== Part 1: 主机特征（每个主机7个特征）=====
        for (Host host : hosts) {
            // 1. 主机处理能力（归一化的MIPS）
            double mips = host.getTotalMipsCapacity();
            normalizer.updateObservation("host_mips", mips);
            state.add(normalizer.normalizeMips(mips));

            // 2. CPU利用率
            double cpuUtil = host.getCpuPercentUtilization();
            state.add(normalizer.normalizeCpuUtilization(cpuUtil));

            // 3. 可用内存比例
            double totalRam = host.getRam().getCapacity();
            double usedRam = host.getRamUtilization();
            double ramAvailableRatio = (totalRam - usedRam) / totalRam;
            state.add(Math.max(0.0, Math.min(1.0, ramAvailableRatio)));

            // 4. 可用带宽比例
            double totalBw = host.getBw().getCapacity();
            double usedBw = host.getBwUtilization();
            double bwAvailableRatio = (totalBw - usedBw) / totalBw;
            state.add(Math.max(0.0, Math.min(1.0, bwAvailableRatio)));

            // 5. 主机是否活跃
            state.add(host.isActive() ? 1.0 : 0.0);

            // 6. 主机上的VM数量（归一化）
            int vmCount = host.getVmList().size();
            state.add(Math.min(vmCount / 10.0, 1.0)); // 假设最多10个VM

            // 7. 主机所在DC的当前绿色比例（若是绿色DC）
            if (host.getDatacenter() instanceof DatacenterGreenAware greenDC) {
                double greenRatio = greenDC.getCurrentGreenEnergyRatio();
                state.add(greenRatio);
            } else {
                state.add(0.0);
            }
        }

        // ===== Part 2: 任务特征 =====
        // 1. CPU需求（归一化）
        double cpuReq = cl.getLength();
        normalizer.updateObservation("cl_cpu_req", cpuReq);
        state.add(normalizer.normalizeCpuRequirement(cpuReq));

        // 2. 内存需求（归一化）
        double memReq = getCloudletMemoryRequirement(cl);
        normalizer.updateObservation("cl_mem_req", memReq);
        state.add(normalizer.normalizeMemRequirement(memReq));

        // 3. 带宽需求（归一化）
        double bwReq = getCloudletBandwidthRequirement(cl);
        normalizer.updateObservation("cl_bw_req", bwReq);
        state.add(Math.min(bwReq / 1000.0, 1.0)); // 假设最大1000 Mbps

        // 4. 任务优先级（如果有）
        double priority = cl.getPriority();
        state.add(priority / 10.0); // 假设优先级0-10

        // ===== Part 3: 上下文信息 =====
        // 1. 当前队列长度
        int queueLength = getCloudletWaitingList().size();
        state.add(normalizer.normalizeQueueLength(queueLength));

        // 2. 数据中心当前负载
        Datacenter dc = getLastSelectedDc();
        if (dc instanceof DatacenterGreenAware greenDC) {
            state.add(greenDC.getOverallLoad());
        } else {
            state.add(0.5); // 默认中等负载
        }

        // 调试和验证
        if (++stateCallCount % 500 == 0) {
            normalizer.printStatistics();
            LOGGER.info("Local state dimension: {} for cloudlet {}", state.size(), cl.getId());
            validateState(state.stream().mapToDouble(Double::doubleValue).toArray());
        }

        return state.stream().mapToDouble(Double::doubleValue).toArray();
    }

    /**
     * 获取任务的内存需求
     */
    private double getCloudletMemoryRequirement(Cloudlet cl) {
        double memUtil = cl.getUtilizationOfRam();

        // 如果是利用率（0-1），转换为实际需求
        if (memUtil <= 1.0) {
            Vm vm = cl.getVm();
            if (vm != null) {
                return memUtil * vm.getRam().getCapacity();
            } else {
                // 默认假设2GB
                return memUtil * 2048;
            }
        }
        return memUtil;
    }

    /**
     * 获取任务的带宽需求
     */
    private double getCloudletBandwidthRequirement(Cloudlet cl) {
        double bwUtil = cl.getUtilizationOfBw();
        if (bwUtil <= 1.0) {
            // 假设基准带宽为1000 Mbps
            return bwUtil * 1000;
        }
        return bwUtil;
    }


    private Vm findBestVm(Host host) {
        List<Vm> hostVms = new ArrayList<>();
        for (Vm vm : getVmCreatedList()) {
            if (vm.getHost().equals(host)) {
                hostVms.add(vm);
            }
        }
        if (hostVms.isEmpty()) return null;
        for (Vm vm : hostVms) if (vm.isIdle()) return vm;
        return hostVms.stream()
                .min(Comparator.comparingDouble(Vm::getCpuPercentUtilization))
                .orElse(hostVms.get(0));
    }
    
    private void validateState(double[] state) {
        for (int i = 0; i < state.length; i++) {
            if (Double.isNaN(state[i]) || Double.isInfinite(state[i])) {
                LOGGER.error("Invalid state value at index {}: {}", i, state[i]);
                state[i] = 0.0;
            }
            if (state[i] < -1.0 || state[i] > 1.0) {
                LOGGER.warn("State value at index {} out of expected range: {}", i, state[i]);
            }
        }
    }

    public int getStateDimension() {
        // hosts.size() * 7 (主机特征) + 4 (任务特征) + 2 (上下文)
        return hosts.size() * 7 + 4 + 2;
    }

    public List<Host> getHosts() {
        return new ArrayList<>(hosts);
    }

    public String getDebugInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocalBroker ").append(getId()).append(" Status:\n");
        sb.append("  Hosts: ").append(hosts.size()).append("\n");
        sb.append("  VMs: ").append(getVmCreatedList().size()).append("\n");
        sb.append("  Cloudlets waiting: ").append(getCloudletWaitingList().size()).append("\n");
        sb.append("  Cloudlets submitted: ").append(getCloudletSubmittedList().size()).append("\n");
        sb.append("  Cloudlets finished: ").append(getCloudletFinishedList().size()).append("\n");
        sb.append("  State dimension: ").append(getStateDimension()).append("\n");
        return sb.toString();
    }
}
