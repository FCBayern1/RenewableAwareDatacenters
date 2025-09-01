package joshua.green.ppo_lr;


import lombok.Getter;
import lombok.Setter;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicy;
import org.cloudsimplus.core.Simulation;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.resources.DatacenterStorage;
import org.cloudsimplus.resources.SanStorage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DatacenterGreenAware extends DatacenterSimple {

    private double greenEnergy; // current green energy balance
    private double cumulativeSurplus;
    private final Map<Integer, Double> greenGenerationMap = new HashMap<>(); // time-tick generataion mapping
    private final String greenProfileCsvPath = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/Datacenters/green_generation_continuous.csv";
    private double lastTickGreenGeneration = 0;     // J
    private double lastTickTotalEnergyUsed = 0;     // J
    private double lastTickSurplus = 0;             // surplus = green - used
    private double generationScalingFactor = 1.0;
    private double initalGreenEnergy;
    private double totalGenerate;
    private double totalGreenUsed = 0;
    private double totalBrownUsed = 0;
    private double lastTickTime = 0;
    private double generation = 0;
    private int lastProcessedTick = -1;
    private Map<LocalDateTime, Double> realGenerationMap;


    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList,
                                VmAllocationPolicy vmAllocationPolicy, double initialGreenKWh) {
        super(simulation, hostList, vmAllocationPolicy);
        this.greenEnergy = initialGreenKWh;
        this.initalGreenEnergy = initialGreenKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, VmAllocationPolicy vmAllocationPolicy, double greenEnergyKWh) {
        super(simulation, vmAllocationPolicy);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList, VmAllocationPolicy vmAllocationPolicy, List<SanStorage> storageList, double greenEnergyKWh) {
        super(simulation, hostList, vmAllocationPolicy, storageList);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList, VmAllocationPolicy vmAllocationPolicy, DatacenterStorage storage, double greenEnergyKWh) {
        super(simulation, hostList, vmAllocationPolicy, storage);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    public DatacenterGreenAware(Simulation simulation, List<? extends Host> hostList, double greenEnergyKWh) {
        super(simulation, hostList);
        this.greenEnergy = greenEnergyKWh;
        this.initalGreenEnergy = greenEnergyKWh;
        loadGreenEnergyProfile(greenProfileCsvPath);
    }

    private double generateGreenEnergy(double time) {
        int tick = (int)Math.floor(time); // tick
        double base = greenGenerationMap.getOrDefault(tick, 0.0); // Watt
        double power = base * generationScalingFactor;
        return power;
    }

    public double getSurplusForCurrentTick() {
        return lastTickSurplus;
    }


    @Override
    protected double updateHostsProcessing() {
        double time = getSimulation().clock();
        double interval = time - getLastProcessTime();
        int currentTick = (int) Math.floor(time);
        double minDelay = Double.MAX_VALUE;

        if (currentTick != lastProcessedTick) {
            lastProcessedTick = currentTick;

            double power = generateGreenEnergy(time);
            double addedEnergy = power * interval;         // 单位 J（=Watt × 秒）
            lastTickGreenGeneration = addedEnergy;
            totalGenerate += addedEnergy;
            greenEnergy += addedEnergy;

            double powerWatts = getPowerModel().getPower();
            double energyWS = powerWatts * interval;
            lastTickTotalEnergyUsed = energyWS;

            double greenUsed = Math.min(energyWS, Math.max(greenEnergy, 0));
            double brownUsed = energyWS - greenUsed;

            greenEnergy = Math.max(0, greenEnergy - greenUsed);

            totalGreenUsed += greenUsed;
            totalBrownUsed += brownUsed;

            lastTickSurplus = lastTickGreenGeneration - lastTickTotalEnergyUsed;
            cumulativeSurplus += lastTickSurplus;

            LOGGER.info(String.format("%.2f: DC %d used %.2f J (green: %.2f, brown: %.2f, green left: %.2f), CPU = %.2f",
                    time, getId(), energyWS, greenUsed, brownUsed, greenEnergy, getCurrentCpuUtilization()));
        }

        for (var host : getHostList()) {
            double delay = host.updateProcessing(time);
            minDelay = Math.min(minDelay, delay);
        }

        double minTimeBetweenEvents = getSimulation().clock();
        if (minDelay != 0) {
            minDelay = Math.max(minDelay, minTimeBetweenEvents);
        }

        return minDelay;
    }
    public double getAverageProcessingAbility() {
        if (getHostList().isEmpty()) return 0.0;

        double totalMips = 0;
        int hostCount = getHostList().size();

        for (Host host : getHostList()) {
            totalMips += host.getTotalMipsCapacity();
        }

        return totalMips / hostCount;
    }
    public double getCurrentCpuUtilization() {
        if (getHostList().isEmpty()) return 0.0;

        double totalUtil = 0;
        int hostCount = getHostList().size();

        for (Host host : getHostList()) {
            totalUtil += host.getCpuMipsUtilization()/host.getTotalMipsCapacity();
        }

        return totalUtil / hostCount;
    }

    public long getAverageRam() {
        if (getHostList().isEmpty()) return 0;
        long totalRam = 0;
        int hostCount = getHostList().size();
        for (Host host : getHostList()) {
            totalRam += host.getRam().getCapacity();
        }
        return totalRam/hostCount;
    }
    public double getRamUtilization() {
        if (getHostList().isEmpty()) return 0.0;
        double totalUtil = 0;
        for (Host host : getHostList()) {
            totalUtil += host.getRam().getPercentUtilization();
        }
        return totalUtil / getHostList().size();
    }
    public double getBwUtilization() {
        if (getHostList().isEmpty()) return 0.0;
        double totalBw = 0;
        for (Host host : getHostList()) {
            totalBw += host.getBw().getPercentUtilization();
        }
        return totalBw / getHostList().size();
    }

    public double getTotalPowerDemand() {
        double totalPower = 0.0;

        for (Host host : getHostList()) {
            double util = host.getCpuPercentUtilization(); // ∈ [0,1]
            totalPower += host.getPowerModel().getPower(util);
        }

        return totalPower;
    }


    private void loadGreenEnergyProfile(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // skip header
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int tick = Integer.parseInt(parts[0].trim()); // RELATIVE_TIME_SEC
                double power = Double.parseDouble(parts[1].trim()); // YIELD
                greenGenerationMap.put(tick, power);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error loading green energy profile", e);
        }
    }

}
