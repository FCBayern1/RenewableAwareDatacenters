package joshua.green.FedRL;

import joshua.green.data.TimedCloudlet;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModel;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.utilizationmodels.UtilizationModelFull;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 从预处理后的Borg traces CSV文件加载Cloudlet
 */
public class ProcessedBorgCloudletLoader {

    /**
     * 从预处理后的CSV文件加载TimedCloudlet列表
     * @param csvFilePath CSV文件路径
     * @param maxCloudlets 最大加载数量（-1表示全部加载）
     * @return TimedCloudlet列表
     */
    public static List<TimedCloudlet> loadFromProcessedCSV(String csvFilePath, int maxCloudlets) {
        List<TimedCloudlet> cloudlets = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            // 读取所有行
            List<String[]> records = reader.readAll();

            if (records.isEmpty()) {
                System.err.println("CSV file is empty");
                return cloudlets;
            }

            // 获取表头
            String[] headers = records.get(0);
            Map<String, Integer> headerMap = createHeaderMap(headers);

            // 处理数据行
            int count = 0;
            for (int i = 1; i < records.size(); i++) {
                if (maxCloudlets > 0 && count >= maxCloudlets) {
                    break;
                }

                String[] row = records.get(i);
                try {
                    TimedCloudlet timedCloudlet = createTimedCloudletFromRow(row, headerMap, count);
                    if (timedCloudlet != null) {
                        cloudlets.add(timedCloudlet);
                        count++;

                        if (count % 100 == 0) {
                            System.out.println("Loaded " + count + " cloudlets...");
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing row " + i + ": " + e.getMessage());
                }
            }

            System.out.println("Successfully loaded " + cloudlets.size() + " cloudlets");

        } catch (IOException | CsvException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
            e.printStackTrace();
        }

        // 按提交时间排序
        cloudlets.sort(Comparator.comparingDouble(TimedCloudlet::getSubmissionTime));

        return cloudlets;
    }

    /**
     * 创建表头映射
     */
    private static Map<String, Integer> createHeaderMap(String[] headers) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            map.put(headers[i].trim(), i);
        }
        return map;
    }

    /**
     * 从CSV行创建TimedCloudlet
     */
    private static TimedCloudlet createTimedCloudletFromRow(String[] row, Map<String, Integer> headerMap, int id) {
        // 提取必要字段
        double submissionTime = getDoubleValue(row, headerMap, "submission_time", 0.0);
        long length = getLongValue(row, headerMap, "length", 1000);
        int pesNumber = getIntValue(row, headerMap, "pes_number", 1);
        long fileSize = getLongValue(row, headerMap, "file_size", 1024);
        long outputSize = getLongValue(row, headerMap, "output_size", 512);

        // 调度参数
        int schedulingClass = getIntValue(row, headerMap, "scheduling_class", 0);
        int priority = getIntValue(row, headerMap, "priority", 100);

        // 利用率参数
        double cpuUtilization = getDoubleValue(row, headerMap, "cpu_utilization", 0.2);
        double memoryUtilization = getDoubleValue(row, headerMap, "memory_utilization", 0.1);
        double bwUtilization = getDoubleValue(row, headerMap, "bw_utilization", 0.1);

        // 资源需求（可选，用于更精确的资源分配）
        double cpuRequest = getDoubleValue(row, headerMap, "cpu_request", 0.1);
        double memoryRequest = getDoubleValue(row, headerMap, "memory_request", 512); // MB

        // 创建Cloudlet
        Cloudlet cloudlet = new CloudletSimple(length, pesNumber)
                .setFileSize(fileSize)
                .setOutputSize(outputSize);

        // 设置ID和优先级
        cloudlet.setId(id);
        cloudlet.setPriority(priority);

        // 创建利用率模型
        UtilizationModel cpuModel = createUtilizationModel(cpuUtilization, schedulingClass);
        UtilizationModel ramModel = createUtilizationModel(memoryUtilization, schedulingClass);
        UtilizationModel bwModel = new UtilizationModelDynamic(bwUtilization);

        cloudlet.setUtilizationModelCpu(cpuModel)
                .setUtilizationModelRam(ramModel)
                .setUtilizationModelBw(bwModel);

        // 如果需要，可以将额外信息存储在Cloudlet的属性中
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("schedulingClass", schedulingClass);
        attributes.put("cpuRequest", cpuRequest);
        attributes.put("memoryRequest", memoryRequest);

        // 处理CPU使用分布（如果存在）
        String cpuDistribution = getStringValue(row, headerMap, "cpu_usage_distribution", "");
        if (!cpuDistribution.isEmpty()) {
            List<Double> distribution = parseCpuDistribution(cpuDistribution);
            if (!distribution.isEmpty()) {
                attributes.put("cpuDistribution", distribution);
                // 可以创建基于分布的动态利用率模型
                cloudlet.setUtilizationModelCpu(createDistributionBasedModel(distribution));
            }
        }

        return new TimedCloudlet(submissionTime, cloudlet);
    }

    /**
     * 根据调度类别创建合适的利用率模型
     */
    private static UtilizationModel createUtilizationModel(double baseUtilization, int schedulingClass) {
        switch (schedulingClass) {
            case 0: // 批处理任务 - 可以使用全部资源
                return new UtilizationModelDynamic(Math.min(baseUtilization * 1.5, 0.9));
            case 1: // 延迟敏感任务 - 稳定的资源使用
                return new UtilizationModelDynamic(baseUtilization);
            case 2: // 正常任务 - 动态调整
                return new UtilizationModelDynamic(baseUtilization)
                        .setUtilizationUpdateFunction(um -> {
                            double time = um.getSimulation().clock();
                            // 模拟负载波动
                            return baseUtilization * (1 + 0.2 * Math.sin(time / 100));
                        });
            case 3: // 高优先级任务 - 保守的资源使用
                return new UtilizationModelDynamic(Math.max(baseUtilization * 0.8, 0.1));
            default:
                return new UtilizationModelDynamic(baseUtilization);
        }
    }

    /**
     * 创建基于CPU使用分布的利用率模型
     */
    private static UtilizationModel createDistributionBasedModel(List<Double> distribution) {
        return new UtilizationModelDynamic(0.0)
                .setUtilizationUpdateFunction(um -> {
                    double time = um.getSimulation().clock();
                    int index = (int)(time / 60) % distribution.size(); // 每分钟一个采样点
                    return distribution.get(index);
                });
    }

    /**
     * 解析CPU使用分布字符串
     */
    private static List<Double> parseCpuDistribution(String distributionStr) {
        List<Double> distribution = new ArrayList<>();

        // 移除方括号
        distributionStr = distributionStr.replaceAll("[\\[\\]]", "").trim();

        if (distributionStr.isEmpty()) {
            return distribution;
        }

        // 分割并解析
        String[] values = distributionStr.split("[,\\s]+");
        for (String value : values) {
            try {
                distribution.add(Double.parseDouble(value.trim()));
            } catch (NumberFormatException e) {
                // 忽略无效值
            }
        }

        return distribution;
    }

    // 辅助方法：安全地获取数值
    private static double getDoubleValue(String[] row, Map<String, Integer> headerMap, String column, double defaultValue) {
        Integer index = headerMap.get(column);
        if (index == null || index >= row.length) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(row[index].trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long getLongValue(String[] row, Map<String, Integer> headerMap, String column, long defaultValue) {
        Integer index = headerMap.get(column);
        if (index == null || index >= row.length) {
            return defaultValue;
        }
        try {
            return Long.parseLong(row[index].trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static int getIntValue(String[] row, Map<String, Integer> headerMap, String column, int defaultValue) {
        Integer index = headerMap.get(column);
        if (index == null || index >= row.length) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(row[index].trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String getStringValue(String[] row, Map<String, Integer> headerMap, String column, String defaultValue) {
        Integer index = headerMap.get(column);
        if (index == null || index >= row.length) {
            return defaultValue;
        }
        return row[index].trim();
    }

    /**
     * 批量创建Cloudlet（不带时间戳）
     */
    public static List<Cloudlet> loadCloudletsFromProcessedCSV(String csvFilePath, int maxCloudlets) {
        List<TimedCloudlet> timedCloudlets = loadFromProcessedCSV(csvFilePath, maxCloudlets);
        return timedCloudlets.stream()
                .map(TimedCloudlet::getCloudlet)
                .collect(Collectors.toList());
    }

    /**
     * 加载并按调度类别分组
     */
    public static Map<Integer, List<TimedCloudlet>> loadGroupedBySchedulingClass(String csvFilePath, int maxCloudlets) {
        List<TimedCloudlet> cloudlets = loadFromProcessedCSV(csvFilePath, maxCloudlets);

        Map<Integer, List<TimedCloudlet>> grouped = new HashMap<>();

        for (TimedCloudlet tc : cloudlets) {
            Cloudlet cl = tc.getCloudlet();
            int schedulingClass = inferSchedulingClass(cl);
            grouped.computeIfAbsent(schedulingClass, k -> new ArrayList<>()).add(tc);
        }

        return grouped;
    }

    private static int inferSchedulingClass(Cloudlet cloudlet) {
        // 根据优先级和其他特征推断调度类别
        int priority = cloudlet.getPriority();
        if (priority < 100) return 0; // 批处理
        if (priority >= 300) return 3; // 高优先级
        if (priority >= 200) return 2; // 正常
        return 1; // 延迟敏感
    }
}