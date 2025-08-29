package joshua.green.newFedRL;

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

public class LocalBrokerRL extends DatacenterBrokerSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalBrokerRL.class);

    private final RLClient rlClient;
    private final List<Host> hosts = new ArrayList<>();

    // State normalizer
    private final StateNormalizer normalizer = new StateNormalizer();

    // Episode tracking for proper done flags
    private int totalCloudlets = 0;
    private int completedCloudlets = 0;
    private boolean episodeEnding = false;

    // (Retained for debugging/compatibility)
    @Getter private final Map<Long, double[]> stateMap  = new HashMap<>();
    @Getter private final Map<Long, Integer>  actionMap = new HashMap<>();
    @Getter private final Map<Long, Double>   probLogMap = new HashMap<>();
    @Getter private final Map<Long, Double>   valueMap = new HashMap<>();

    // Pending experiences (linked by cloudletId)
    private static class PendingExp {
        double[] state;
        int action;
        double logProb;
        double value;

        double ts;         // Action timestamp (schedule)
        long hostId = -1;  // Selected host ID (optional)
        // Energy snapshots (DC granularity mainly; Host granularity can be extended if implemented)
        double dcGreenStart, dcTotalStart;
        int dcIndex = -1;  // Optional: if you need to track by index
    }
    private final Map<Long, PendingExp> pendingMap = new ConcurrentHashMap<>();

    // Local reward coefficients (can be fine-tuned as needed)
    private static final double A1 = 0.40; // Wait time penalty weight
    private static final double A2 = 0.40; // Execution time penalty weight
    private static final double A3 = 0.20; // Total energy increment penalty (DC)
    private static final double A4 = 0.10; // Green energy increment reward (DC)
    private static final double TIME_SCALE_LOCAL = 100.0; // Scale time to 0~1
    private static final double ENERGY_SCALE = 1.0;       // Scale energy to 0~1 if needed

    // Debug counter
    private int stateCallCount = 0;

    public LocalBrokerRL(CloudSimPlus simulation, RLClient rlClient, List<Host> hosts) {
        super(simulation);
        this.rlClient = rlClient;
        this.hosts.addAll(hosts);
    }

    /**
     * Set total cloudlets for episode tracking
     */
    public void setTotalCloudlets(int total) {
        this.totalCloudlets = total;
        this.completedCloudlets = 0;
        this.episodeEnding = false;
        LOGGER.info("LocalBroker {}: Total cloudlets set to {} for episode", getId(), total);
    }

    /**
     * Signal that the episode is ending
     */
    public void signalEpisodeEnd() {
        this.episodeEnding = true;
        LOGGER.info("LocalBroker {}: Episode ending signal received", getId());
    }

    /**
     * Reset episode tracking for new episode
     */
    public void resetEpisodeTracking() {
        this.completedCloudlets = 0;
        this.episodeEnding = false;
        LOGGER.info("LocalBroker {}: Episode tracking reset", getId());
    }

    @Override
    public DatacenterBroker submitCloudlet(@NonNull Cloudlet cloudlet) {
        // 1) Build local state
        double[] state = buildState(cloudlet);

        // 2) Select action (Host index) through RL client
        RLClient.ActionResponse result = rlClient.selectActionLocal(state, hosts.size());
        int action   = result.action;
        double logProb = result.log_prob;
        double value   = result.value;

        // 3) Validate action and select Host/VM
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

        // 4) Record pending (for action-reward alignment)
        final double now = getSimulation().clock();
        LocalBrokerRL.PendingExp exp = new LocalBrokerRL.PendingExp();
        exp.state = state != null ? state.clone() : null;
        exp.action = action;
        exp.logProb = logProb;
        exp.value = value;
        exp.ts = now;
        if (selectedHost != null) exp.hostId = selectedHost.getId();

        Datacenter dc = getLastSelectedDc(); // Current broker's corresponding DC
        if (dc instanceof DatacenterGreenAware g) {
            exp.dcGreenStart = g.getTotalGreenUsed();
            exp.dcTotalStart = g.getTotalGreenUsed() + g.getTotalBrownUsed();
            // If you maintain DC index, can inject it during construction, simplified here
        }
        pendingMap.put(cloudlet.getId(), exp);

        // 5) Register completion callback: calculate local reward and report with done flag
        cloudlet.addOnFinishListener(info -> {
            Cloudlet finished = info.getCloudlet();
            LocalBrokerRL.PendingExp p = pendingMap.remove(finished.getId());
            if (p == null) {
                LOGGER.warn("Local: pending missing for Cloudlet {}", finished.getId());
                return;
            }

            // Increment completed counter
            completedCloudlets++;

            // Check if this is the last cloudlet or episode is ending
            boolean isDone = (completedCloudlets >= totalCloudlets) || episodeEnding;

            if (isDone) {
                LOGGER.info("LocalBroker {}: Episode done signal for Cloudlet {} (completed: {}/{})",
                        getId(), finished.getId(), completedCloudlets, totalCloudlets);
            }

            double tf = info.getTime();
            double rLocal = computeLocalReward(p, finished, tf);

            // next state: snapshot at completion time
            double[] nextState = buildState(finished);

            try {
                rlClient.storeExperienceLocal(
                        p.state != null ? p.state : new double[]{},
                        p.action,
                        rLocal,
                        nextState,
                        isDone,      // Properly set done flag
                        p.logProb,
                        p.value
                );
            } catch (Exception e) {
                LOGGER.error("storeExperienceLocal failed for Cloudlet {}: {}", finished.getId(), e.getMessage());
            }
        });

        // (Retained) Store decision info: for your old statistics code to continue working
        long cloudletId = cloudlet.getId();
        stateMap.put(cloudletId, state);
        actionMap.put(cloudletId, action);
        probLogMap.put(cloudletId, logProb);
        valueMap.put(cloudletId, value);

        // 6) Submit to parent class (triggers CloudSim scheduling)
        return super.submitCloudlet(cloudlet);
    }

    /**
     * Calculate local reward (calculated on completion, window [ts -> tf])
     * Formula:
     *   r_local = -A1 * norm(T_wait) - A2 * norm(T_exec) - A3 * norm(ΔE_dc) + A4 * norm(ΔG_dc)
     */
    private double computeLocalReward(LocalBrokerRL.PendingExp p, Cloudlet cl, double tf) {
        // 1) Time terms (converted to 0~1 scale)
        double T_wait = Math.max(0.0, cl.getStartWaitTime());
        double T_exec = Math.max(0.0, cl.getTotalExecutionTime());
        double waitPenalty = Math.min(1.0, T_wait / TIME_SCALE_LOCAL);
        double execPenalty = Math.min(1.0, T_exec / TIME_SCALE_LOCAL);

        // 2) DC energy increment (Host granularity unavailable, use DC granularity)
        double dE_dc = 0.0, dG_dc = 0.0;
        Datacenter dc = getLastSelectedDc();
        if (dc instanceof DatacenterGreenAware g) {
            double dcGreenEnd = g.getTotalGreenUsed();
            double dcTotalEnd = g.getTotalGreenUsed() + g.getTotalBrownUsed();
            dG_dc = Math.max(0.0, dcGreenEnd - p.dcGreenStart);
            dE_dc = Math.max(0.0, dcTotalEnd - p.dcTotalStart);
        }
        // Simple scaling (can use percentile normalization/standardization if needed)
        double energyPenalty = Math.min(1.0, (dE_dc / Math.max(1e-9, ENERGY_SCALE)));
        double greenBonus    = Math.min(1.0, (dG_dc / Math.max(1e-9, ENERGY_SCALE)));

        // 3) Combination
        double reward = - A1 * waitPenalty
                - A2 * execPenalty
                - A3 * energyPenalty
                + A4 * greenBonus;

        if (Double.isNaN(reward) || Double.isInfinite(reward)) reward = 0.0;

        LOGGER.debug("LocalBroker {}: Reward for Cloudlet {} = {} (wait: {}, exec: {}, energy: {}, green: {})",
                getId(), cl.getId(), reward, waitPenalty, execPenalty, energyPenalty, greenBonus);

        return reward;
    }

    protected double[] buildState(Cloudlet cl) {
        List<Double> state = new ArrayList<>();

        // ===== Part 1: Host features (7 features per host) =====
        for (Host host : hosts) {
            // 1. Host processing capability (normalized MIPS)
            double mips = host.getTotalMipsCapacity();
            normalizer.updateObservation("host_mips", mips);
            state.add(normalizer.normalizeMips(mips));

            // 2. CPU utilization
            double cpuUtil = host.getCpuPercentUtilization();
            state.add(normalizer.normalizeCpuUtilization(cpuUtil));

            // 3. Available memory ratio
            double totalRam = host.getRam().getCapacity();
            double usedRam = host.getRamUtilization();
            double ramAvailableRatio = (totalRam - usedRam) / totalRam;
            state.add(Math.max(0.0, Math.min(1.0, ramAvailableRatio)));

            // 4. Available bandwidth ratio
            double totalBw = host.getBw().getCapacity();
            double usedBw = host.getBwUtilization();
            double bwAvailableRatio = (totalBw - usedBw) / totalBw;
            state.add(Math.max(0.0, Math.min(1.0, bwAvailableRatio)));

            // 5. Host active status
            state.add(host.isActive() ? 1.0 : 0.0);

            // 6. Number of VMs on host (normalized)
            int vmCount = host.getVmList().size();
            state.add(Math.min(vmCount / 10.0, 1.0)); // Assume max 10 VMs

            // 7. Current green ratio of host's DC (if green DC)
            if (host.getDatacenter() instanceof DatacenterGreenAware greenDC) {
                double greenRatio = greenDC.getCurrentGreenEnergyRatio();
                state.add(greenRatio);
            } else {
                state.add(0.0);
            }
        }

        // ===== Part 2: Task features =====
        // 1. CPU requirement (normalized)
        double cpuReq = cl.getLength();
        normalizer.updateObservation("cl_cpu_req", cpuReq);
        state.add(normalizer.normalizeCpuRequirement(cpuReq));

        // 2. Memory requirement (normalized)
        double memReq = getCloudletMemoryRequirement(cl);
        normalizer.updateObservation("cl_mem_req", memReq);
        state.add(normalizer.normalizeMemRequirement(memReq));

        // 3. Bandwidth requirement (normalized)
        double bwReq = getCloudletBandwidthRequirement(cl);
        normalizer.updateObservation("cl_bw_req", bwReq);
        state.add(Math.min(bwReq / 1000.0, 1.0)); // Assume max 1000 Mbps

        // 4. Task priority (if any)
        double priority = cl.getPriority();
        state.add(priority / 10.0); // Assume priority 0-10

        // ===== Part 3: Context information =====
        // 1. Current queue length
        int queueLength = getCloudletWaitingList().size();
        state.add(normalizer.normalizeQueueLength(queueLength));

        // 2. Current datacenter load
        Datacenter dc = getLastSelectedDc();
        if (dc instanceof DatacenterGreenAware greenDC) {
            state.add(greenDC.getOverallLoad());
        } else {
            state.add(0.5); // Default medium load
        }

        // Debug and validation
        if (++stateCallCount % 500 == 0) {
            normalizer.printStatistics();
            LOGGER.info("Local state dimension: {} for cloudlet {}", state.size(), cl.getId());
            validateState(state.stream().mapToDouble(Double::doubleValue).toArray());
        }

        return state.stream().mapToDouble(Double::doubleValue).toArray();
    }

    /**
     * Get task memory requirement
     */
    private double getCloudletMemoryRequirement(Cloudlet cl) {
        double memUtil = cl.getUtilizationOfRam();

        // If utilization (0-1), convert to actual requirement
        if (memUtil <= 1.0) {
            Vm vm = cl.getVm();
            if (vm != null) {
                return memUtil * vm.getRam().getCapacity();
            } else {
                // Default assume 2GB
                return memUtil * 2048;
            }
        }
        return memUtil;
    }

    /**
     * Get task bandwidth requirement
     */
    private double getCloudletBandwidthRequirement(Cloudlet cl) {
        double bwUtil = cl.getUtilizationOfBw();
        if (bwUtil <= 1.0) {
            // Assume baseline bandwidth 1000 Mbps
            return bwUtil * 1000;
        }
        return bwUtil;
    }

    /**
     * Find best VM on host: prioritize idle, then choose lowest CPU load
     */
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

    /**
     * Validate state vector validity
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

    public int getStateDimension() {
        // hosts.size() * 7 (host features) + 4 (task features) + 2 (context)
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
        sb.append("  Episode Progress: ").append(completedCloudlets).append("/").append(totalCloudlets).append("\n");
        sb.append("  Episode Ending: ").append(episodeEnding).append("\n");
        return sb.toString();
    }
}