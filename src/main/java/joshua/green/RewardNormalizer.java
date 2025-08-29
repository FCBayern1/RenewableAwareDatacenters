package joshua.green;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class RewardNormalizer {
    private double runningMean = 0;
    private double runningVar = 1;
    private int count = 0;
    private final double epsilon = 1e-8;

    // Increased warmup period
    private final int warmup_episodes = 10;
    private List<Double> warmup_rewards = new ArrayList<>();

    // Add reward buffer for better statistics
    private static final int BUFFER_SIZE = 1000;
    private List<Double> rewardBuffer = new ArrayList<>();

    // Control parameters
    private final double learningRate = 0.001;  // Slower adaptation
    private final boolean useStandardization = true;  // Toggle between standardization and simple scaling

    public double normalize(double reward) {
        // During warmup, collect data without normalization
        if (count < warmup_episodes * 100) {
            warmup_rewards.add(reward);
            count++;
            // Return scaled but not normalized reward during warmup
            return reward * 0.01;  // Gentle scaling
        }

        // Initialize statistics after warmup
        if (count == warmup_episodes * 100) {
            initializeStatistics();
        }

        // Add to buffer for robust statistics
        rewardBuffer.add(reward);
        if (rewardBuffer.size() > BUFFER_SIZE) {
            rewardBuffer.remove(0);
        }

        count++;

        // Update running statistics with slower learning rate
        double delta = reward - runningMean;
        runningMean += learningRate * delta;
        runningVar = (1 - learningRate) * runningVar + learningRate * delta * delta;

        // Normalize
        if (useStandardization) {
            double std = Math.sqrt(runningVar + epsilon);
            double normalized = (reward - runningMean) / std;

            // Softer clipping using tanh
            return Math.tanh(normalized / 2.0) * 2.0;  // Maps roughly to [-2, 2]
        } else {
            // Alternative: simple scaling based on observed range
            double scale = Math.sqrt(runningVar) * 3;  // 3 standard deviations
            if (scale < epsilon) scale = 1.0;
            return reward / scale;
        }
    }

    private void initializeStatistics() {
        if (warmup_rewards.isEmpty()) return;

        // Calculate robust statistics from warmup data
        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        for (double r : warmup_rewards) {
            sum += r;
            min = Math.min(min, r);
            max = Math.max(max, r);
        }

        runningMean = sum / warmup_rewards.size();

        // Calculate variance
        double varSum = 0;
        for (double r : warmup_rewards) {
            varSum += Math.pow(r - runningMean, 2);
        }
        runningVar = varSum / warmup_rewards.size();

        // Add robustness: ensure variance is not too small
        runningVar = Math.max(runningVar, 0.1);

        System.out.printf("Reward Normalizer initialized: mean=%.3f, std=%.3f, range=[%.3f, %.3f]%n",
                runningMean, Math.sqrt(runningVar), min, max);
    }

    public void reset() {
        runningMean = 0;
        runningVar = 1;
        count = 0;
        warmup_rewards.clear();
        rewardBuffer.clear();
    }

    public double getMean() {
        return runningMean;
    }

    public double getStd() {
        return Math.sqrt(runningVar);
    }

    // Get recent statistics for debugging
    public String getStats() {
        if (rewardBuffer.isEmpty()) {
            return "No data yet";
        }

        double recentMean = rewardBuffer.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);

        double recentMin = rewardBuffer.stream()
                .mapToDouble(Double::doubleValue)
                .min()
                .orElse(0.0);

        double recentMax = rewardBuffer.stream()
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(0.0);

        return String.format("Recent stats: mean=%.3f, range=[%.3f, %.3f], running_mean=%.3f, running_std=%.3f",
                recentMean, recentMin, recentMax, runningMean, Math.sqrt(runningVar));
    }
}