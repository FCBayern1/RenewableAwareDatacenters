package joshua.green.data;

/**
 * Description:
 * Author: joshua
 * Date: 2025/6/23
 */


import com.opencsv.CSVReader;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Author: joshua
 * Description: Load Cloudlets from a CSV file
 */
public class new_read {

    public static List<TimedCloudlet> loadTimedCloudletsFromCSV(String csvFilePath, int maxCloudlets) {
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
                    //time, cloudletId, length, pes, fileSize, outputSize, utilization_cpu, utilization_ram
                    double time = Double.parseDouble(line[0].trim());
                    int cloudletId = Integer.parseInt(line[1].trim());
                    long length = (long) Math.ceil(Double.parseDouble(line[2].trim()));
                    int pes = Integer.parseInt(line[3].trim());
                    long fileSize = Long.parseLong(line[4].trim());
                    long outputSize = Long.parseLong(line[5].trim());
                    double utilizationCpu = Double.parseDouble(line[6].trim());
                    double utilizationRam = Double.parseDouble(line[7].trim());

                    UtilizationModelDynamic cpuModel = new UtilizationModelDynamic(utilizationCpu);
                    UtilizationModelDynamic ramModel = new UtilizationModelDynamic(utilizationRam);

                    Cloudlet cl = new CloudletSimple(length, pes)
                            .setFileSize(fileSize)
                            .setOutputSize(outputSize)
                            .setUtilizationModelCpu(cpuModel)
                            .setUtilizationModelRam(ramModel)
                            .setUtilizationModelBw(new UtilizationModelDynamic(0.01));

                    cl.setId(cloudletId);

                    list.add(new TimedCloudlet(time, cl));
                    count++;
                } catch (Exception ex) {
                    System.err.println("Skipping line: " + ex.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }



}


