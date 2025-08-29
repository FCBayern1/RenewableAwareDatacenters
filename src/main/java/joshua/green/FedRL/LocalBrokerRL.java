package joshua.green.FedRL;

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

/**
 * Local Broker with Reinforcement Learning control over VM selection.
 * 负责在数据中心内部将任务分配到具体的主机
 */
public class LocalBrokerRL extends DatacenterBrokerSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalBrokerRL.class);

    private final RLClient rlClient;
    private final List<Host> hosts = new ArrayList<>();

    // 状态归一化器
    private final StateNormalizer normalizer = new StateNormalizer();

    // 存储每个任务的决策信息
    @Getter
    private final Map<Long, double[]> stateMap = new HashMap<>();
    @Getter
    private final Map<Long, Integer> actionMap = new HashMap<>();
    @Getter
    private final Map<Long, Double> probLogMap = new HashMap<>();
    @Getter
    private final Map<Long, Double> valueMap = new HashMap<>();

    // 跟踪已完成的任务
    private final Set<Long> finishedCloudlets = new HashSet<>();

    // 调试计数器
    private int stateCallCount = 0;

    public LocalBrokerRL(CloudSimPlus simulation, RLClient rlClient, List<Host> hosts) {
        super(simulation);
        this.rlClient = rlClient;
        this.hosts.addAll(hosts);
    }

    @Override
    public DatacenterBroker submitCloudlet(@NonNull Cloudlet cloudlet) {
        // 构建状态向量
        double[] state = buildState(cloudlet);

        // 通过RL客户端选择动作
        RLClient.ActionResponse result = rlClient.selectActionLocal(state, hosts.size());
        int action = result.action;
        double logProb = result.log_prob;
        double value = result.value;

        // 验证动作有效性
        if (action >= 0 && action < hosts.size()) {
            Host selectedHost = hosts.get(action);
            if (selectedHost != null) {
                // 找到该主机上最合适的VM
                Vm vm = findBestVm(selectedHost);
                if (vm != null) {
                    cloudlet.setVm(vm);
                    LOGGER.debug("Cloudlet {} assigned to VM {} on Host {}",
                            cloudlet.getId(), vm.getId(), selectedHost.getId());
                } else {
                    LOGGER.warn("No suitable VM found on Host {} for Cloudlet {}",
                            selectedHost.getId(), cloudlet.getId());
                }
            }
        } else {
            LOGGER.error("Invalid action {} for {} hosts", action, hosts.size());
        }

        // 存储决策信息
        long cloudletId = cloudlet.getId();
        stateMap.put(cloudletId, state);
        actionMap.put(cloudletId, action);
        probLogMap.put(cloudletId, logProb);
        valueMap.put(cloudletId, value);

        return super.submitCloudlet(cloudlet);
    }

    /**
     * 构建本地调度器的状态向量
     * 包含主机特征和任务特征
     */
    protected double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        // ===== Part 1: 主机特征（每个主机4个特征）=====
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

            // ===== 额外有用信息 =====
            // 5. 主机是否活跃
            state.add(host.isActive() ? 1.0 : 0.0);

            // 6. 主机上的VM数量（归一化）
            int vmCount = host.getVmList().size();
            state.add(Math.min(vmCount / 10.0, 1.0)); // 假设最多10个VM

            // 7. 主机能源效率（如果是绿色数据中心）
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
            // 获取任务将要运行的VM的内存大小
            Vm vm = cl.getVm();
            if (vm != null) {
                return memUtil * vm.getRam().getCapacity();  // 修复：添加 .getCapacity()
            } else {
                // 默认假设2GB
                return memUtil * 2048;
            }
        }

        return memUtil;
    }

    /**
    /**
     * 获取任务的带宽需求
     */
    private double getCloudletBandwidthRequirement(Cloudlet cl) {
        double bwUtil = cl.getUtilizationOfBw();

        // 如果是利用率（0-1），转换为实际需求
        if (bwUtil <= 1.0) {
            // 假设基准带宽为1000 Mbps
            return bwUtil * 1000;
        }

        return bwUtil;
    }

    /**
     * 找到主机上最合适的VM
     * 优先选择空闲的VM，然后选择负载最低的VM
     */
    private Vm findBestVm(Host host) {
        List<Vm> hostVms = new ArrayList<>();

        // 收集该主机上的所有VM
        for (Vm vm : getVmCreatedList()) {
            if (vm.getHost().equals(host)) {
                hostVms.add(vm);
            }
        }

        if (hostVms.isEmpty()) {
            return null;
        }

        // 优先返回空闲的VM
        for (Vm vm : hostVms) {
            if (vm.isIdle()) {
                return vm;
            }
        }

        // 如果没有空闲VM，返回负载最低的VM
        return hostVms.stream()
                .min(Comparator.comparingDouble(vm -> vm.getCpuPercentUtilization()))
                .orElse(hostVms.get(0));
    }

    /**
     * 验证状态向量的有效性
     */
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

    /**
     * 获取状态向量的维度
     * 用于Python端配置
     */
    public int getStateDimension() {
        // 每个主机7个特征 + 任务4个特征 + 上下文2个特征
        return hosts.size() * 7 + 4 + 2;
    }

    /**
     * 获取主机列表
     */
    public List<Host> getHosts() {
        return new ArrayList<>(hosts);
    }

    /**
     * 获取调试信息
     */
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