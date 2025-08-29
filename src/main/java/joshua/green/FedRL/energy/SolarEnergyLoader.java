package joshua.green.FedRL.energy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 太阳能发电数据加载器
 * 支持从实际发电厂数据加载
 */
public class SolarEnergyLoader implements RenewableEnergyGenerator {

    private double peakPower; // 峰值功率（瓦特）
    private Map<String, List<SolarDataPoint>> inverterData = new HashMap<>(); // 按逆变器分组
    private List<SolarDataPoint> aggregatedData = new ArrayList<>(); // 聚合后的数据
    private boolean useRealData = false;

    // 太阳能数据点
    private static class SolarDataPoint {
        LocalDateTime timestamp;
        String sourceKey;       // 逆变器ID
        double dcPower;         // 直流功率 W
        double acPower;         // 交流功率 W
        double dailyYield;      // 日发电量
        double totalYield;      // 总发电量

        SolarDataPoint(LocalDateTime timestamp, String sourceKey,
                       double dcPower, double acPower,
                       double dailyYield, double totalYield) {
            this.timestamp = timestamp;
            this.sourceKey = sourceKey;
            this.dcPower = dcPower;
            this.acPower = acPower;
            this.dailyYield = dailyYield;
            this.totalYield = totalYield;
        }
    }

    /**
     * 使用模型创建太阳能发电器
     */
    public SolarEnergyLoader(double peakPower) {
        this.peakPower = peakPower;
        this.useRealData = false;
    }

    /**
     * 从Plant_1_Generation_Data.csv加载太阳能数据
     */
    public SolarEnergyLoader(String csvPath, double peakPower) {
        this.peakPower = peakPower;
        loadSolarPlantData(csvPath);
    }

    /**
     * 加载太阳能发电厂数据
     * CSV格式: DATE_TIME,PLANT_ID,SOURCE_KEY,DC_POWER,AC_POWER,DAILY_YIELD,TOTAL_YIELD
     */
    private void loadSolarPlantData(String csvPath) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line = br.readLine(); // 跳过表头

            // 支持两种日期格式
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            Map<LocalDateTime, Double> timeAggregation = new TreeMap<>();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 7) {
                    try {
                        // 尝试解析日期
                        LocalDateTime timestamp;
                        try {
                            timestamp = LocalDateTime.parse(parts[0].trim(), formatter1);
                        } catch (Exception e1) {
                            timestamp = LocalDateTime.parse(parts[0].trim(), formatter2);
                        }

                        String sourceKey = parts[2].trim();
                        double dcPower = Double.parseDouble(parts[3].trim());
                        double acPower = Double.parseDouble(parts[4].trim());
                        double dailyYield = Double.parseDouble(parts[5].trim());
                        double totalYield = Double.parseDouble(parts[6].trim());

                        SolarDataPoint point = new SolarDataPoint(
                                timestamp, sourceKey, dcPower, acPower, dailyYield, totalYield
                        );

                        // 按逆变器存储
                        inverterData.computeIfAbsent(sourceKey, k -> new ArrayList<>()).add(point);

                        // 聚合同一时间的所有逆变器功率
                        timeAggregation.merge(timestamp, acPower, Double::sum);

                    } catch (Exception e) {
                        System.err.println("Error parsing line: " + line + " - " + e.getMessage());
                    }
                }
            }

            // 创建聚合数据
            for (Map.Entry<LocalDateTime, Double> entry : timeAggregation.entrySet()) {
                aggregatedData.add(new SolarDataPoint(
                        entry.getKey(), "AGGREGATED", 0, entry.getValue(), 0, 0
                ));
            }

            // 按时间排序
            aggregatedData.sort(Comparator.comparing(s -> s.timestamp));

            if (!aggregatedData.isEmpty()) {
                useRealData = true;

                // 计算实际峰值功率
                peakPower = aggregatedData.stream()
                        .mapToDouble(s -> s.acPower)
                        .max()
                        .orElse(peakPower);

                System.out.println("Loaded solar plant data:");
                System.out.println("  Records: " + aggregatedData.size());
                System.out.println("  Inverters: " + inverterData.size());
                System.out.println("  Peak Power: " + (peakPower/1000) + " kW");
                System.out.println("  Date Range: " + aggregatedData.get(0).timestamp +
                        " to " + aggregatedData.get(aggregatedData.size()-1).timestamp);
            }

        } catch (Exception e) {
            System.err.println("Failed to load solar plant data: " + e.getMessage());
            e.printStackTrace();
            useRealData = false;
        }
    }

    @Override
    public double getPowerAtTime(double simulationTime) {
        if (useRealData && !aggregatedData.isEmpty()) {
            // 计算数据索引（15分钟间隔）
            int dataIndex = (int)(simulationTime / 900) % aggregatedData.size();

            if (dataIndex < aggregatedData.size()) {
                SolarDataPoint current = aggregatedData.get(dataIndex);
                double power = current.acPower;

                if (dataIndex + 1 < aggregatedData.size()) {
                    SolarDataPoint next = aggregatedData.get(dataIndex + 1);
                    double fraction = (simulationTime % 900) / 900.0;
                    power = power * (1 - fraction) + next.acPower * fraction;
                }

                return power;
            }
        }

        return calculateSolarPowerModel(simulationTime);
    }

    /**
     * 太阳能发电模型（备用）
     */
    private double calculateSolarPowerModel(double simulationTime) {
        double hourOfDay = (simulationTime % 86400) / 3600;

        // 太阳能只在白天产生（6:00 - 18:00）
        if (hourOfDay < 6 || hourOfDay > 18) {
            return 0;
        }

        // 正弦曲线模拟
        double solarAngle = (hourOfDay - 6) * Math.PI / 12;
        double baseIrradiance = Math.sin(solarAngle) * 1000;

        // 天气影响
        double cloudFactor = 0.7 + 0.3 * Math.random();

        // 温度效率
        double temperature = 25 + 10 * Math.sin(hourOfDay * Math.PI / 24);
        double tempEfficiency = 1 - 0.004 * (temperature - 25);

        double irradiance = baseIrradiance * cloudFactor;
        double efficiency = 0.2 * tempEfficiency;

        return peakPower * (irradiance / 1000) * efficiency;
    }

    @Override
    public double getEnergyForPeriod(double startTime, double duration) {
        double energy = 0;
        double timeStep = Math.min(300, duration); // 5分钟步长

        for (double t = startTime; t < startTime + duration; t += timeStep) {
            double power = getPowerAtTime(t);
            double stepDuration = Math.min(timeStep, startTime + duration - t);
            energy += power * stepDuration;
        }

        return energy;
    }

    @Override
    public double getPredictedPower(double currentTime, double lookahead) {
        return getPowerAtTime(currentTime + lookahead);
    }

    @Override
    public String getEnergyType() {
        return "Solar";
    }

    @Override
    public double getCapacityFactor() {
        if (useRealData && !aggregatedData.isEmpty()) {
            double avgPower = aggregatedData.stream()
                    .mapToDouble(s -> s.acPower)
                    .average()
                    .orElse(0);
            return avgPower / peakPower;
        }
        return 0.2;
    }

    /**
     * 获取特定时间的逆变器级别数据
     */
    public Map<String, Double> getInverterPowerAtTime(double simulationTime) {
        Map<String, Double> inverterPowers = new HashMap<>();

        if (useRealData) {
            int dataIndex = (int)(simulationTime / 900) % aggregatedData.get(0).timestamp.toLocalTime().toSecondOfDay();

            for (Map.Entry<String, List<SolarDataPoint>> entry : inverterData.entrySet()) {
                List<SolarDataPoint> points = entry.getValue();
                if (dataIndex < points.size()) {
                    inverterPowers.put(entry.getKey(), points.get(dataIndex).acPower);
                }
            }
        }

        return inverterPowers;
    }
}