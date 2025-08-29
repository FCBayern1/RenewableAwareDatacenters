package joshua.green;

import java.util.HashMap;
import java.util.Map;

public class StateNormalizer {
    // Running statistics for adaptive normalization
    private Map<String, RunningStats> featureStats = new HashMap<>();

    // Minimum observations before using adaptive normalization
    private static final int MIN_OBSERVATIONS = 100;

    // Default scales (used initially)
    private static final double GREEN_SURPLUS_SCALE = 50000.0;
    private static final double MIPS_SCALE = 10000.0;
    private static final double CPU_REQUIREMENT_SCALE = 100000.0;
    private static final double MEM_REQUIREMENT_SCALE = 8192.0;
    private static final double QUEUE_LENGTH_SCALE = 50.0;
    private static final double TIME_SCALE = 3600.0;
    private static final double GREEN_ENERGY_STOCK_SCALE = 100000.0;

    // Running statistics helper class
    private static class RunningStats {
        private double mean = 0.0;
        private double m2 = 0.0;
        private int count = 0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;

        void update(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        double getVariance() {
            return count > 1 ? m2 / (count - 1) : 0.0;
        }

        double getStd() {
            return Math.sqrt(getVariance());
        }
    }

    private RunningStats getOrCreateStats(String feature) {
        return featureStats.computeIfAbsent(feature, k -> new RunningStats());
    }

    public double normalizeGreenSurplus(double surplus) {
        RunningStats stats = getOrCreateStats("green_surplus");
        stats.update(surplus);

        if (stats.count >= MIN_OBSERVATIONS && stats.getStd() > 1e-6) {
            // Z-score normalization with clipping
            double normalized = (surplus - stats.mean) / (stats.getStd() + 1e-8);
            return Math.tanh(normalized / 2.0);  // Smooth clipping using tanh
        } else {
            // Use default scaling initially
            return Math.tanh(surplus / GREEN_SURPLUS_SCALE);
        }
    }

    public double normalizeGreenStock(double stock) {
        RunningStats stats = getOrCreateStats("green_stock");
        stats.update(stock);

        if (stats.count >= MIN_OBSERVATIONS && stats.max > stats.min) {
            // Min-max normalization to [0, 1]
            double normalized = (stock - stats.min) / (stats.max - stats.min + 1e-8);
            return normalized * 2.0 - 1.0;  // Map to [-1, 1]
        } else {
            return 2.0 / (1.0 + Math.exp(-stock / GREEN_ENERGY_STOCK_SCALE)) - 1.0;
        }
    }

    public double normalizeMips(double mips) {
        RunningStats stats = getOrCreateStats("mips");
        stats.update(mips);

        if (stats.count >= MIN_OBSERVATIONS && stats.max > 0) {
            return mips / stats.max;  // Normalize to [0, 1]
        } else {
            return Math.min(mips / MIPS_SCALE, 1.0);
        }
    }

    public double normalizeCpuUtilization(double utilization) {
        // Already in [0, 1] range, no normalization needed
        return Math.max(0.0, Math.min(1.0, utilization));
    }

    public double normalizeCpuRequirement(double requirement) {
        RunningStats stats = getOrCreateStats("cpu_req");
        stats.update(requirement);

        if (stats.count >= MIN_OBSERVATIONS) {
            // Use 95th percentile as upper bound for robustness
            double scale = stats.mean + 2 * stats.getStd();
            return Math.min(requirement / scale, 1.0);
        } else {
            return Math.min(requirement / CPU_REQUIREMENT_SCALE, 1.0);
        }
    }

    public double normalizeMemRequirement(double requirement) {
        RunningStats stats = getOrCreateStats("mem_req");
        stats.update(requirement);

        if (stats.count >= MIN_OBSERVATIONS && stats.max > 0) {
            return requirement / stats.max;
        } else {
            return Math.min(requirement / MEM_REQUIREMENT_SCALE, 1.0);
        }
    }

    public double normalizeQueueLength(int length) {
        RunningStats stats = getOrCreateStats("queue_length");
        stats.update(length);

        if (stats.count >= MIN_OBSERVATIONS) {
            // Use exponential decay for queue length
            double avgQueue = stats.mean;
            return 1.0 - Math.exp(-length / (avgQueue + 1));
        } else {
            return Math.min(length / QUEUE_LENGTH_SCALE, 1.0);
        }
    }

    public double normalizeTime(double time) {
        // Cyclical encoding for time
        double hourOfDay = (time % TIME_SCALE) / TIME_SCALE;
        return Math.sin(2 * Math.PI * hourOfDay);  // Sine encoding for cyclical nature
    }

    public void updateObservation(String feature, double value) {
        getOrCreateStats(feature).update(value);
    }

    public void printStatistics() {
        System.out.println("=== State Normalization Statistics ===");
        for (Map.Entry<String, RunningStats> entry : featureStats.entrySet()) {
            RunningStats stats = entry.getValue();
            if (stats.count > 0) {
                System.out.printf("%s: count=%d, mean=%.2f, std=%.2f, min=%.2f, max=%.2f%n",
                        entry.getKey(), stats.count, stats.mean, stats.getStd(), stats.min, stats.max);
            }
        }
    }
}