package joshua.green.bestfit;


import joshua.green.data.TimedCloudlet;
import joshua.green.data.new_read;
import joshua.green.Datacenters.DatacenterGreenAware;


import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;
import org.cloudsimplus.autoscaling.VerticalVmScalingSimple;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.power.models.PowerModelHost;
import org.cloudsimplus.power.models.PowerModelHostSpec;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.resources.Ram;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.util.*;
import joshua.green.inferenceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class simulation_test {
    private static final Logger logger = LoggerFactory.getLogger(simulation_test.class);

    private final inferenceConfig config;

    public simulation_test(inferenceConfig config) {
        this.config = config;
    }

    public static void main(String[] args) {
        inferenceConfig config = new inferenceConfig();
        new simulation_test(config).run();
    }

    public void run() {
        CloudSimPlus simulation = new CloudSimPlus(0.001);
        GlobalBrokerBestFit globalBroker = new GlobalBrokerBestFit(simulation);
        List<Datacenter> datacenters = new ArrayList<>();
        List<DatacenterBrokerSimple> brokers = new ArrayList<>();

        for (int i = 0; i < config.getDcNum(); i++) {
            DatacenterGreenAware dc = createDatacenter(simulation, i, config.getInitialEnergyArray()[i],
                    config.getInitialScaleFactorArray()[i]);
            datacenters.add(dc);
            DatacenterBrokerSimple broker = new DatacenterBrokerSimple(simulation);
            brokers.add(broker);

            globalBroker.addLocalBrokerWithDatacenter(broker, dc);
        }

        List<Vm> vms = createVms();
        List<TimedCloudlet> tcs = new_read.loadTimedCloudletsFromCSV(config.getCloudletFile(), config.getCloudlets());
        globalBroker.setTimedCloudlets(tcs);
        for (int i = 0; i < vms.size(); i++) {
            brokers.get(i % brokers.size()).submitVm(vms.get(i));
        }

        simulation.start();
        printStats(datacenters, brokers);
    }

    private DatacenterGreenAware createDatacenter(CloudSimPlus sim, int dcIndex,
                                                  double initialGreenKwh, double scalingFactor) {

        double[][] serverPowerProfiles = {
                {41.6, 46.7, 52.3, 57.9, 65.4, 73.0, 80.7, 89.5, 99.6, 105, 113},
                {86, 89.4, 92.6, 96, 99.5, 105, 110, 115, 120, 125, 130},
                {93.7, 97, 101, 105, 110, 116, 121, 125, 129, 133, 135},
                {56, 59, 65, 73, 85, 101, 124, 149, 177, 205, 232}
        };

        int[] serverMips = {11700, 20000, 15000, 16000};

        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < config.getHosts(); i++) {
            int serverType = (dcIndex + i) % serverPowerProfiles.length;

            List<Pe> peList = new ArrayList<>();
            int mips = serverMips[serverType];
            for (int j = 0; j < config.getHostPes(); j++) {
                peList.add(new PeSimple(mips));
            }

            long ram = (long)(config.getHostRam() * 1024 * (2 + (serverType % 3) * 0.25));
            long bw = (long)(config.getHostBw() * (2 + (serverType % 2) * 0.5));

            Host host = new HostSimple(ram, bw, config.getHostStorage(), peList);

            PowerModelHost powerModel = new PowerModelHostSpec(serverPowerProfiles[serverType]);
            host.setPowerModel(powerModel);

            hostList.add(host);
        }

        String[] paths = config.getWindDataPath().split(",");
        String csvPath = paths[dcIndex % paths.length];

        DatacenterGreenAware dc = new DatacenterGreenAware(sim, hostList,
                new VmAllocationPolicySimple(), initialGreenKwh, csvPath);

        dc.setGenerationScalingFactor(scalingFactor);

        logger.info("Created DC {} - Initial: {} kWh, Scaling: {}, CSV: {}",
                dcIndex, initialGreenKwh, scalingFactor, csvPath);

        return dc;
    }

    private List<Vm> createVms() {
        List<Vm> list = new ArrayList<>();
        for (int i = 0; i < config.getVms(); i++) {
            var ramScaling = new VerticalVmScalingSimple(Ram.class, 0.20);
            ramScaling.setLowerThresholdFunction(vm -> 0.70);
            ramScaling.setUpperThresholdFunction(vm -> 0.90);
            Vm vm = new VmSimple(config.getHostMips(), config.getVmPes());
            vm.setRam(1024*8).setBw(1000).setSize(10_000);
            vm.setRamVerticalScaling(ramScaling);
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());
            list.add(vm);
        }
        return list;
    }



    private void printStats(List<Datacenter> dcs, List<DatacenterBrokerSimple> brokers) {
        double totalGreen = 0, totalBrown = 0, makespan = 0, executionTime = 0, totalGreenInitial = 0, totalGeneration = 0;
        for (Datacenter dc : dcs) {
            if (dc instanceof DatacenterGreenAware g) {
                totalGreen += g.getTotalGreenUsed();
                totalBrown += g.getTotalBrownUsed();
                totalGreenInitial += g.getInitalGreenEnergy();
                System.out.printf("%s - Green Initial: %.2f%n", dc.getName(), g.getInitalGreenEnergy());
                System.out.printf("%s - Green Gnerated: %.2f%n", dc.getName(), g.getTotalGenerate());
                System.out.printf("%s - Green Used: %.2f%n", dc.getName(), g.getTotalGreenUsed());
                totalGeneration += g.getTotalGenerate();
            }
        }
        int counts = 0;
        for (DatacenterBrokerSimple broker : brokers) {
            for (Cloudlet cl : broker.getCloudletFinishedList()) {
                counts++;
                makespan = Math.max(makespan, cl.getFinishTime());
                executionTime += cl.getFinishTime() - cl.getStartTime();
            }
        }

        double totalEnergy = totalGreen + totalBrown;

        System.out.print("\n=== Best Fit Inference Metrics ===\n");
        System.out.println("Finish Cloudlets number: "+ counts);
        System.out.printf("Green Energy Ratio: %.2f%%\n", (totalEnergy > 0 ? totalGreen / totalEnergy * 100 : 0));
        System.out.printf("Green Energy Utilisation: %.2f%%\n", ((totalGreenInitial+totalGeneration) > 0 ? totalGreen / (totalGreenInitial+totalGeneration) * 100 : 0));
        System.out.printf("Total Makespan: %.2f\n", makespan);
        System.out.printf("Total Execution Time: %.2f\n", executionTime);
        System.out.printf("Total Energy Consumption: %.2f\n", totalEnergy);
    }
}
