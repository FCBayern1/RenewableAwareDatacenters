// 新建文件：StateNormalizer.java
package joshua.green.FedRL;

import java.util.HashMap;
import java.util.Map;

public class StateNormalizer {
    // 归一化参数（可以根据实际数据调整）
    private static final double GREEN_SURPLUS_SCALE = 50000.0;  // 假设盈余范围 [-50kJ, 50kJ]
    private static final double MIPS_SCALE = 10000.0;          // 假设最大MIPS为10000
    private static final double CPU_REQUIREMENT_SCALE = 100000.0; // 任务长度
    private static final double MEM_REQUIREMENT_SCALE = 8192.0;   // 8GB内存
    private static final double QUEUE_LENGTH_SCALE = 50.0;        // 最大队列长度
    private static final double TIME_SCALE = 3600.0;             // 1小时
    private static final double GREEN_ENERGY_STOCK_SCALE = 100000.0; // 库存量

    // 动态统计最大最小值（可选）
    private Map<String, Double> observedMax = new HashMap<>();
    private Map<String, Double> observedMin = new HashMap<>();

    /**
     * 归一化绿色能源盈余/赤字（保留符号）
     * 使用tanh函数，输出范围 [-1, 1]
     */
    public double normalizeGreenSurplus(double surplus) {
        return Math.tanh(surplus / GREEN_SURPLUS_SCALE);
    }

    /**
     * 归一化绿色能源库存（非负值）
     * 使用sigmoid函数，输出范围 [0, 1]
     */
    public double normalizeGreenStock(double stock) {
        return 2.0 / (1.0 + Math.exp(-stock / GREEN_ENERGY_STOCK_SCALE)) - 1.0;
    }

    /**
     * 归一化MIPS（处理能力）
     * 线性归一化到 [0, 1]
     */
    public double normalizeMips(double mips) {
        return Math.min(mips / MIPS_SCALE, 1.0);
    }

    /**
     * 归一化CPU利用率（已经在0-1之间）
     */
    public double normalizeCpuUtilization(double utilization) {
        return Math.max(0.0, Math.min(1.0, utilization));
    }

    /**
     * 归一化任务CPU需求
     */
    public double normalizeCpuRequirement(double requirement) {
        return Math.min(requirement / CPU_REQUIREMENT_SCALE, 1.0);
    }

    /**
     * 归一化内存需求
     */
    public double normalizeMemRequirement(double requirement) {
        return Math.min(requirement / MEM_REQUIREMENT_SCALE, 1.0);
    }

    /**
     * 归一化队列长度
     */
    public double normalizeQueueLength(int length) {
        return Math.min(length / QUEUE_LENGTH_SCALE, 1.0);
    }

    /**
     * 归一化时间
     */
    public double normalizeTime(double time) {
        return (time % TIME_SCALE) / TIME_SCALE; // 循环归一化
    }

    /**
     * 更新观察到的最大最小值（用于动态调整）
     */
    public void updateObservation(String feature, double value) {
        observedMax.put(feature, Math.max(observedMax.getOrDefault(feature, value), value));
        observedMin.put(feature, Math.min(observedMin.getOrDefault(feature, value), value));
    }

    /**
     * 打印统计信息（用于调试和调整参数）
     */
    public void printStatistics() {
        System.out.println("=== State Normalization Statistics ===");
        for (String feature : observedMax.keySet()) {
            System.out.printf("%s: min=%.2f, max=%.2f%n",
                    feature, observedMin.get(feature), observedMax.get(feature));
        }
    }
}