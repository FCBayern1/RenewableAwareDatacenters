package joshua.green.data;

import com.opencsv.CSVReader;
import joshua.green.data.TimedCloudlet;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.utilizationmodels.UtilizationModelFull;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Load Borg-processed Cloudlets from CSV file
 */
public class BorgCloudletLoader {

    /**
     * 从处理后的Borg CSV文件加载带时间戳的Cloudlets
     * CSV格式: id,length,pes_number,file_size,output_size,submission_delay,
     *          priority,cpu_utilization,memory_utilization,expected_duration,
     *          scheduling_class,collection_id
     */
    public static List<TimedCloudlet> loadTimedCloudletsFromBorgCSV(String csvFilePath, int maxCloudlets) {
        List<TimedCloudlet> list = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] line;
            boolean headerSkipped = false;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }

                if (count >= maxCloudlets) break;

                try {
                    // 解析Borg CSV字段
                    int cloudletId = Integer.parseInt(line[0].trim());
                    long length = Long.parseLong(line[1].trim());
                    int pes = Integer.parseInt(line[2].trim());
                    long fileSize = Long.parseLong(line[3].trim());
                    long outputSize = Long.parseLong(line[4].trim());
                    double submissionTime = Double.parseDouble(line[5].trim());
                    int priority = Integer.parseInt(line[6].trim());
                    double cpuUtilization = Double.parseDouble(line[7].trim());
                    double memoryUtilization = Double.parseDouble(line[8].trim());
                    double expectedDuration = Double.parseDouble(line[9].trim());
                    int schedulingClass = Integer.parseInt(line[10].trim());

                    // 创建利用率模型
                    UtilizationModelDynamic cpuModel = createBorgUtilizationModel(cpuUtilization);
                    UtilizationModelDynamic ramModel = createBorgUtilizationModel(memoryUtilization);
                    UtilizationModelDynamic bwModel = new UtilizationModelDynamic(0.1); // 带宽假设10%

                    // 创建Cloudlet
                    Cloudlet cl = new CloudletSimple(length, pes)
                            .setFileSize(fileSize)
                            .setOutputSize(outputSize)
                            .setUtilizationModelCpu(cpuModel)
                            .setUtilizationModelRam(ramModel)
                            .setUtilizationModelBw(bwModel);

                    cl.setId(cloudletId);
                    cl.setPriority(priority);


                    list.add(new TimedCloudlet(submissionTime, cl));
                    count++;

                } catch (Exception ex) {
                    System.err.println("Skipping line: " + ex.getMessage());
                }
            }

            System.out.printf("Loaded %d cloudlets from %s\n", list.size(), csvFilePath);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * 创建基于Borg数据的利用率模型
     * 可以根据需要添加更复杂的逻辑，比如随时间变化
     */
    private static UtilizationModelDynamic createBorgUtilizationModel(double baseUtilization) {
        UtilizationModelDynamic model = new UtilizationModelDynamic(baseUtilization);

        // 可选：添加随时间的波动
        // model.setUtilizationUpdateFunction(modelRef -> {
        //     double time = modelRef.getSimulation().clock();
        //     double variation = 0.1 * Math.sin(time / 100); // ±10%的波动
        //     return Math.max(0, Math.min(1, baseUtilization + variation));
        // });

        return model;
    }

    /**
     * 加载带过滤条件的Cloudlets
     */
    public static List<TimedCloudlet> loadFilteredCloudlets(
            String csvFilePath,
            int maxCloudlets,
            int minPriority,
            int maxPes) {

        List<TimedCloudlet> list = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] line;
            reader.readNext(); // 跳过标题
            int count = 0;

            while ((line = reader.readNext()) != null && count < maxCloudlets) {
                try {
                    int priority = Integer.parseInt(line[6].trim());
                    int pes = Integer.parseInt(line[2].trim());

                    // 应用过滤条件
                    if (priority < minPriority || pes > maxPes) {
                        continue;
                    }

                    // 创建cloudlet（复用上面的代码）
                    TimedCloudlet tc = createTimedCloudletFromLine(line);
                    if (tc != null) {
                        list.add(tc);
                        count++;
                    }

                } catch (Exception ex) {
                    System.err.println("Error processing line: " + ex.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    private static TimedCloudlet createTimedCloudletFromLine(String[] line) {
        try {
            int cloudletId = Integer.parseInt(line[0].trim());
            long length = Long.parseLong(line[1].trim());
            int pes = Integer.parseInt(line[2].trim());
            long fileSize = Long.parseLong(line[3].trim());
            long outputSize = Long.parseLong(line[4].trim());
            double submissionTime = Double.parseDouble(line[5].trim());
            int priority = Integer.parseInt(line[6].trim());
            double cpuUtilization = Double.parseDouble(line[7].trim());
            double memoryUtilization = Double.parseDouble(line[8].trim());

            Cloudlet cl = new CloudletSimple(length, pes)
                    .setFileSize(fileSize)
                    .setOutputSize(outputSize)
                    .setUtilizationModelCpu(new UtilizationModelDynamic(cpuUtilization))
                    .setUtilizationModelRam(new UtilizationModelDynamic(memoryUtilization))
                    .setUtilizationModelBw(new UtilizationModelDynamic(0.1));

            cl.setId(cloudletId);
            cl.setPriority(priority);

            return new TimedCloudlet(submissionTime, cl);

        } catch (Exception e) {
            return null;
        }
    }
}