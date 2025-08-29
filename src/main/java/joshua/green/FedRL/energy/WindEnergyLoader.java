package joshua.green.FedRL.energy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 风能发电数据加载器
 * 支持真实风机数据
 */
public class WindEnergyLoader implements RenewableEnergyGenerator {

    private double ratedPower; // 额定功率（瓦特）
    private List<WindDataPoint> windData = new ArrayList<>();
    private double cutInSpeed = 3.0;    // 切入风速 m/s
    private double ratedSpeed = 12.0;   // 额定风速 m/s
    private double cutOutSpeed = 25.0;  // 切出风速 m/s

    // 风电数据点
    private static class WindDataPoint {
        LocalDateTime timestamp;
        double windSpeed;       // 风速 m/s
        double windDirection;   // 风向 度
        double powerOutput;     // 输出功率 W
        double temperature;     // 温度 °C

        WindDataPoint(LocalDateTime timestamp, double windSpeed, double windDirection,
                      double powerOutput, double temperature) {
            this.timestamp = timestamp;
            this.windSpeed = windSpeed;
            this.windDirection = windDirection;
            this.powerOutput = powerOutput;
            this.temperature = temperature;
        }
    }

    /**
     * 从CSV文件加载风电数据
     * @param csvPath CSV文件路径（风机数据）
     */
    public WindEnergyLoader(String csvPath) {
        loadWindData(csvPath);
    }

    /**
     * 使用风速模型创建风能发电器
     * @param ratedPower 额定功率（瓦特）
     */
    public WindEnergyLoader(double ratedPower) {
        this.ratedPower = ratedPower;
    }

    /**
     * 加载风机数据
     * 使用你提供的风机CSV格式
     */
    private void loadWindData(String csvPath) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line = br.readLine(); // 跳过表头
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 14) {
                    try {
                        LocalDateTime timestamp = LocalDateTime.parse(parts[0].trim(), formatter);
                        double windSpeed = Double.parseDouble(parts[1].trim());  // Wspd
                        double windDirection = Double.parseDouble(parts[2].trim()); // Wdir
                        double temperature = Double.parseDouble(parts[3].trim()); // Etmp
                        double powerOutput = Double.parseDouble(parts[13].trim()) * 1000; // OT转换为瓦特

                        windData.add(new WindDataPoint(timestamp, windSpeed, windDirection,
                                powerOutput, temperature));
                    } catch (Exception e) {
                        // 跳过无效行
                    }
                }
            }

            windData.sort(Comparator.comparing(w -> w.timestamp));

            // 计算额定功率
            ratedPower = windData.stream()
                    .mapToDouble(w -> w.powerOutput)
                    .max()
                    .orElse(2000000); // 默认2MW

            System.out.println("Loaded " + windData.size() + " wind power records");
            System.out.println("Rated power: " + (ratedPower/1000) + " kW");

        } catch (Exception e) {
            System.err.println("Failed to load wind data: " + e.getMessage());
        }
    }

    @Override
    public double getPowerAtTime(double simulationTime) {
        if (!windData.isEmpty()) {
            // 使用真实数据，10分钟间隔
            int dataIndex = (int)(simulationTime / 600) % windData.size();

            WindDataPoint current = windData.get(dataIndex);
            double power = current.powerOutput;

            // 线性插值
            if (dataIndex + 1 < windData.size()) {
                WindDataPoint next = windData.get(dataIndex + 1);
                double fraction = (simulationTime % 600) / 600.0;
                power = power * (1 - fraction) + next.powerOutput * fraction;
            }

            // 添加短期波动
            double turbulence = (Math.random() - 0.5) * 0.1;
            power *= (1 + turbulence);

            return Math.max(0, power);
        } else {
            // 使用风速模型
            return calculateWindPowerModel(simulationTime);
        }
    }

    /**
     * 风电功率曲线模型
     */
    private double calculateWindPowerModel(double simulationTime) {
        // 生成风速（使用Weibull分布的简化版）
        double baseWindSpeed = 7.0; // 平均风速
        double hourOfDay = (simulationTime % 86400) / 3600;

        // 日夜风速变化
        double diurnalFactor = 1.0 + 0.3 * Math.sin((hourOfDay - 6) * Math.PI / 12);

        // 添加阵风
        double gust = 2 * Math.sin(simulationTime / 300) + Math.random() * 2;

        double windSpeed = baseWindSpeed * diurnalFactor + gust;
        windSpeed = Math.max(0, windSpeed);

        // 风机功率曲线
        if (windSpeed < cutInSpeed) {
            return 0;
        } else if (windSpeed < ratedSpeed) {
            // 立方关系
            double fraction = (windSpeed - cutInSpeed) / (ratedSpeed - cutInSpeed);
            return ratedPower * Math.pow(fraction, 3);
        } else if (windSpeed < cutOutSpeed) {
            return ratedPower;
        } else {
            return 0; // 超过切出风速，停机保护
        }
    }

    @Override
    public double getEnergyForPeriod(double startTime, double duration) {
        double energy = 0;
        double timeStep = Math.min(60, duration); // 60秒步长

        for (double t = startTime; t < startTime + duration; t += timeStep) {
            double powerWatts = getPowerAtTime(t);     // 瓦特
            double stepSeconds = Math.min(timeStep, startTime + duration - t); // 秒
            energy += powerWatts * stepSeconds;         // 焦耳 = 瓦特 × 秒
        }

        return energy; // 返回焦耳
    }

    @Override
    public double getPredictedPower(double currentTime, double lookahead) {
        // 风电预测更不确定，添加不确定性
        double basePower = getPowerAtTime(currentTime + lookahead);
        double uncertainty = 0.2 * basePower * (Math.random() - 0.5);
        return Math.max(0, basePower + uncertainty);
    }

    @Override
    public String getEnergyType() {
        return "Wind";
    }

    @Override
    public double getCapacityFactor() {
        if (!windData.isEmpty()) {
            double avgPower = windData.stream()
                    .mapToDouble(w -> w.powerOutput)
                    .average()
                    .orElse(0);
            return avgPower / ratedPower;
        }
        // 风电容量因子通常在25-40%
        return 0.35;
    }

    /**
     * 获取当前风速（用于监控）
     */
    public double getCurrentWindSpeed(double simulationTime) {
        if (!windData.isEmpty()) {
            int dataIndex = (int)(simulationTime / 600) % windData.size();
            return windData.get(dataIndex).windSpeed;
        }
        return 0;
    }
}