package joshua.green.FedRL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import joshua.green.data.TimedCloudlet;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;

public class GoogleTraceCloudletLoader {

    private static final Random rand = new Random();

    public static List<TimedCloudlet> loadFromGoogleTrace(String csvFilePath, int maxCloudlets, double timeScale) {
        List<TimedCloudlet> cloudlets = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String headerLine = br.readLine(); // 读取表头
            System.out.println("Processing Google trace file: " + csvFilePath);

            String line;
            StringBuilder recordBuilder = new StringBuilder();
            int count = 0;
            int recordNumber = 0;
            boolean inRecord = false;

            while ((line = br.readLine()) != null && count < maxCloudlets) {
                // 判断是否是新记录的开始：
                // 1. 以数字开头
                // 2. 第二个字段也是数字（避免误判cpu_usage_distribution中的数字）
                if (line.matches("^\\d+,\\d+.*") || line.matches("^\\d+,\\d+\\.\\d+E\\+\\d+.*")) {
                    // 处理之前的记录
                    if (recordBuilder.length() > 0) {
                        processRecord(recordBuilder.toString(), count, cloudlets, timeScale);
                        count++;
                        recordBuilder.setLength(0);
                    }
                    // 开始新记录
                    recordBuilder.append(line);
                    inRecord = true;
                } else if (inRecord) {
                    // 这是当前记录的延续
                    recordBuilder.append(" ").append(line.trim());
                }

                if (count > 0 && count % 100 == 0) {
                    System.out.println("Processed " + count + " cloudlets...");
                }
            }

            // 处理最后一条记录
            if (recordBuilder.length() > 0 && count < maxCloudlets) {
                processRecord(recordBuilder.toString(), count, cloudlets, timeScale);
            }

        } catch (Exception e) {
            System.err.println("Error reading file: " + csvFilePath);
            e.printStackTrace();
        }

        System.out.println("Successfully loaded " + cloudlets.size() + " cloudlets from Google trace");

        // 按提交时间排序
        cloudlets.sort(Comparator.comparingDouble(TimedCloudlet::getSubmissionTime));

        return cloudlets;
    }

    private static void processRecord(String record, int count, List<TimedCloudlet> cloudlets, double timeScale) {
        try {
            // 使用逗号分隔，但要处理引号内的逗号
            List<String> parts = parseCSVLine(record);

            if (parts.size() >= 33) {
                // 解析Google跟踪数据
                GoogleTraceTask task = parseGoogleTraceTask(parts);

                // 只处理成功的任务或者需要重试的任务
                if (task.failed == 0 || rand.nextDouble() < 0.3) {
                    Cloudlet cloudlet = createCloudletFromTrace(task, count);
                    double submissionTime = count * 0.1; // 每0.1秒提交一个任务
                    cloudlets.add(new TimedCloudlet(submissionTime, cloudlet));
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }

    // 解析CSV行，处理引号内的逗号
    private static List<String> parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean inBrackets = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"' && (i == 0 || line.charAt(i-1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == '[' && !inQuotes) {
                inBrackets = true;
                current.append(c);
            } else if (c == ']' && !inQuotes) {
                inBrackets = false;
                current.append(c);
            } else if (c == ',' && !inQuotes && !inBrackets) {
                result.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        // 添加最后一个字段
        if (current.length() > 0) {
            result.add(current.toString().trim());
        }

        return result;
    }

    // 解析任务数据
    private static GoogleTraceTask parseGoogleTraceTask(List<String> parts) {
        GoogleTraceTask task = new GoogleTraceTask();

        try {
            // 基础字段解析
            task.recordId = parseInt(parts.get(0));
            task.time = parseDouble(parts.get(1));
            task.schedulingClass = parseInt(parts.get(4));
            task.collectionType = parseInt(parts.get(5));
            task.priority = parseInt(parts.get(6));

            // 解析资源请求（JSON格式，第10个字段）
            if (parts.size() > 10) {
                String resourceStr = parts.get(10);
                task.cpuRequest = extractResourceValue(resourceStr, "cpus");
                task.memoryRequest = extractResourceValue(resourceStr, "memory");
            }

            // 解析时间（第19和20个字段）
            if (parts.size() > 20) {
                task.startTime = parseDouble(parts.get(19)) / 1e9; // 纳秒转换为秒
                task.endTime = parseDouble(parts.get(20)) / 1e9;
            }

            // 解析平均使用情况（第21个字段）
            if (parts.size() > 21) {
                String avgUsageStr = parts.get(21);
                task.avgCpuUsage = extractResourceValue(avgUsageStr, "cpus");
                task.avgMemoryUsage = extractResourceValue(avgUsageStr, "memory");
            }

            // 解析失败标志（第33个字段）
            if (parts.size() > 33) {
                task.failed = parseInt(parts.get(33));
            }

            // 验证数据有效性
            if (task.cpuRequest <= 0) task.cpuRequest = 0.01;
            if (task.memoryRequest <= 0) task.memoryRequest = 0.001;
            if (task.avgCpuUsage < 0) task.avgCpuUsage = 0.1;
            if (task.avgMemoryUsage < 0) task.avgMemoryUsage = 0.05;
            if (task.endTime <= task.startTime) task.endTime = task.startTime + 100;

        } catch (Exception e) {
            // 使用默认值
            task.cpuRequest = 0.01;
            task.memoryRequest = 0.001;
            task.avgCpuUsage = 0.1;
            task.avgMemoryUsage = 0.05;
            task.startTime = 0;
            task.endTime = 100;
            task.failed = 0;
        }

        return task;
    }

    // 安全的整数解析
    private static int parseInt(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            return 0;
        }
    }

    // 处理科学计数法
    private static double parseDouble(String value) {
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    // 提取资源值
    private static double extractResourceValue(String resourceStr, String key) {
        try {
            // 处理格式：{'cpus': 0.020660400390625, 'memory': 0.014434814453125}
            String pattern = "'" + key + "':\\s*([0-9.eE+-]+)";
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher m = p.matcher(resourceStr);

            if (m.find()) {
                return Double.parseDouble(m.group(1));
            }

            // 尝试双引号
            pattern = "\"" + key + "\":\\s*([0-9.eE+-]+)";
            p = java.util.regex.Pattern.compile(pattern);
            m = p.matcher(resourceStr);

            if (m.find()) {
                return Double.parseDouble(m.group(1));
            }

            // 处理None值
            if (resourceStr.contains(key) && resourceStr.contains("None")) {
                return 0.001;
            }

            return 0.01; // 默认值
        } catch (Exception e) {
            return 0.01;
        }
    }

    private static Cloudlet createCloudletFromTrace(GoogleTraceTask task, int id) {
        // 计算任务长度
        long length = calculateTaskLength(task);

        // 计算PE数
        int pes = Math.max(1, Math.min(4, (int)(task.cpuRequest * 10)));

        // 创建利用率模型
        double cpuUtil = Math.min(0.9, Math.max(0.1, task.avgCpuUsage / Math.max(0.001, task.cpuRequest)));
        double ramUtil = Math.min(0.9, Math.max(0.1, task.avgMemoryUsage / Math.max(0.001, task.memoryRequest)));

        Cloudlet cloudlet = new CloudletSimple(length, pes)
                .setFileSize(1024)
                .setOutputSize(512)
                .setUtilizationModelCpu(new UtilizationModelDynamic(cpuUtil))
                .setUtilizationModelRam(new UtilizationModelDynamic(ramUtil))
                .setUtilizationModelBw(new UtilizationModelDynamic(0.1));

        cloudlet.setId(id);
        cloudlet.setPriority(task.priority);

        return cloudlet;
    }

    private static long calculateTaskLength(GoogleTraceTask task) {
        double duration = task.endTime - task.startTime;
        if (duration <= 0 || duration > 1e6) {
            duration = 100; // 默认100秒
        }

        // 基于持续时间和调度类别计算长度
        long baseLength = (long)(duration * 1000); // 毫秒转换

        // 确保长度在合理范围内
        if (baseLength < 1000) baseLength = 1000;
        if (baseLength > 100000) baseLength = 100000;

        switch (task.schedulingClass) {
            case 0: return baseLength * 2;    // 批处理任务
            case 1: return baseLength;         // 延迟敏感
            case 2: return (long)(baseLength * 1.5); // 正常任务
            case 3: return (long)(baseLength * 0.8); // 高优先级
            default: return baseLength;
        }
    }

    // 内部类
    private static class GoogleTraceTask {
        int recordId;
        double time;
        int schedulingClass;
        int priority;
        double cpuRequest;
        double memoryRequest;
        double startTime;
        double endTime;
        double avgCpuUsage;
        double avgMemoryUsage;
        int collectionType;
        int failed;

        GoogleTraceTask() {
            this.cpuRequest = 0.01;
            this.memoryRequest = 0.001;
            this.startTime = 0;
            this.endTime = 100;
            this.avgCpuUsage = 0.1;
            this.avgMemoryUsage = 0.05;
            this.failed = 0;
            this.schedulingClass = 0;
            this.priority = 100;
            this.collectionType = 0;
        }
    }
}