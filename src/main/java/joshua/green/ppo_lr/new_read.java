package joshua.green.ppo_lr;

/**
 * Description:
 * Author: joshua
 * Date: 2025/6/23
 */


import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import joshua.green.data.TimedCloudlet;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;

import java.io.FileReader;
import java.io.IOException;
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
                    double time = Double.parseDouble(line[1].trim());
                    int cloudletId = Integer.parseInt(line[0].trim());
                    long length = (long)Math.ceil(Double.parseDouble(line[2].trim()));
                    int pes = Integer.parseInt(line[3].trim());
                    long ram = Long.parseLong(line[4].trim());
                    long fileSize = Long.parseLong(line[5].trim());
                    long outputSize = Long.parseLong(line[6].trim());
                    String traceStr = line[7].trim();

                    double[] trace = parseCpuTrace(traceStr);
                    UtilizationModelDynamic model = createUtilizationModel(trace);
                    final var utilizationModel = new UtilizationModelDynamic(0.2);

                    Cloudlet cl = new CloudletSimple(length, pes)
                            .setFileSize(fileSize)
                            .setOutputSize(outputSize)
                            .setUtilizationModelCpu(utilizationModel)
                            .setUtilizationModelRam(new UtilizationModelDynamic(0.01))
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


    private static double[] parseCpuTrace(String traceStr) {
        traceStr = traceStr.replace("[", "").replace("]", "").trim();
        if (traceStr.isEmpty()) return new double[]{0.0};

        String[] parts = traceStr.split("[,\\s]+");
        double[] trace = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            trace[i] = Double.parseDouble(parts[i]);
        }
        return trace;
    }

    private static UtilizationModelDynamic createUtilizationModel(double[] trace) {
        UtilizationModelDynamic model = new UtilizationModelDynamic(0.0);

        model.setUtilizationUpdateFunction(modelRef -> {
            int time = (int) Math.floor(modelRef.getSimulation().clock());
            return (time >= 0 && time < trace.length) ? trace[time] : 0.0;
        });

        return model;
    }

    private static Function<UtilizationModelDynamic, Double> createTraceFunction(double[] trace) {
        return model -> {
            int time = (int) Math.floor(model.getSimulation().clock());
            return time >= 0 && time < trace.length ? trace[time] : 0.0;
        };
    }
}


