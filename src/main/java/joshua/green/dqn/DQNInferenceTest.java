package joshua.green.dqn;

import joshua.green.Datacenters.DatacenterGreenAware;
import joshua.green.data.TimedCloudlet;
import joshua.green.data.read;

import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.power.models.PowerModelHostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Description:
 * Author: joshua
 * Date: 2025/6/22
 */
public class DQNInferenceTest {
    private static final Logger logger = LoggerFactory.getLogger(DQNInferenceTest.class);

    private static final int DC_NUM = 10, HOSTS = 4, HOST_PES = 8, HOST_MIPS = 1000;
    private static final long HOST_RAM = 1024 * 1024 * 16, HOST_BW = 10_000, HOST_STORAGE = 1_000_000;
    private static final int VMS = 40, VM_PES = 2, CLOUDLETS = 1000;

    private final RLClient globalRlClient = new RLClient("global");
    private final List<RLClient> localRlClients = new ArrayList<>();

    public static void main(String[] args) {
        new DQNInferenceTest().run();
    }

    public void run(){
        CloudSimPlus simulation = new CloudSimPlus(0.001);
        globalRlClient.loadModel();
        GlobalBrokerRL globalBroker = new GlobalBrokerRL(simulation, globalRlClient);
        List<Datacenter> dcs = new ArrayList<>();
        List<LocalBrokerRL> brokers = new ArrayList<>();

        for (int i =0; i < DC_NUM; i++){
            Datacenter dc = createDatacenter(simulation, i);
            dcs.add(dc);
            RLClient localRlClient = new RLClient("local_" + i);
            localRlClient.loadModel();
            localRlClients.add(localRlClient);
            List<Host> hosts = dc.getHostList();
            var broker = new LocalBrokerRL(simulation, localRlClient, hosts);
            broker.setVmDestructionDelayFunction(vm -> 5000.0);
            broker.setDatacenterMapper((last, vm) -> dc);
            brokers.add(broker);
        }
        globalBroker.addLocalBrokers(brokers);

        // Create VMs and Cloudlets
        List<Vm> vmList = createVms();
        globalBroker.setVmList(vmList);


        List<TimedCloudlet> cloudletList = read.loadTimedCloudletsFromCSV(
                "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/data/cloudsim_cloudlets.csv",
                CLOUDLETS);

        Cloudlet dummy = new CloudletSimple(1, 1, new UtilizationModelDynamic(0.01));
        dummy.setFileSize(1).setOutputSize(1).setId(0);
        globalBroker.submitCloudlet(dummy, 0);
        scheduleCloudlets(simulation, globalBroker, cloudletList, dcs);
        simulation.start();
        printStats(dcs, brokers);
    }

    private DatacenterGreenAware createDatacenter(CloudSimPlus sim, int index) {
        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(HOST_MIPS + index * 100));
        }
        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < HOSTS; i++) {
            Host host = new HostSimple((long)(HOST_RAM * (1 + (index % 3) * 0.25)),
                    (long)(HOST_BW * (1 + (index % 2) * 0.5)),
                    HOST_STORAGE, peList);
            host.setPowerModel(new PowerModelHostSimple(50 + index * 5, 35 + index * 2));
            hostList.add(host);
        }
        return new DatacenterGreenAware(sim, hostList, new VmAllocationPolicySimple(), Math.random() * 100000);
    }

    private List<Vm> createVms() {
        List<Vm> list = new ArrayList<>();
        for (int i = 0; i < VMS; i++) {
            Vm vm = new VmSimple(HOST_MIPS, VM_PES);
            vm.setRam(1024).setBw(1000).setSize(10000);
            list.add(vm);
        }
        return list;
    }
    private void scheduleCloudlets(CloudSimPlus sim, GlobalBrokerRL globalBroker, List<TimedCloudlet> cloudlets, List<Datacenter> dcs) {
        List<TimedCloudlet> submitted = new ArrayList<>();

        sim.addOnClockTickListener(info -> {
            double now = sim.clock();
            for (TimedCloudlet tc : cloudlets) {
                if (!submitted.contains(tc) && tc.getSubmissionTime() <= now) {
                    Cloudlet cl = tc.getCloudlet();
                    double[] globalState = globalBroker.buildState(cl);
                    int globalAction = globalRlClient.selectAction(globalState, globalBroker.getLocalBrokers().size());
                    globalBroker.submitCloudlet(tc.getCloudlet(), globalAction);
                    submitted.add(tc);
                    System.out.printf("Submitted Cloudlet %d at %.2f\n", tc.getCloudlet().getId(), now);
                    long id = cl.getId();
                    System.out.printf("Cloudlet %d submitted at %.2f to broker %d%n", id, now, globalAction);
                }
            }
        });
    }

    private void printStats(List<Datacenter> dcs, List<LocalBrokerRL> brokers) {
        double totalGreen = 0, totalBrown = 0, makespan = 0, executionTime = 0;
        for (Datacenter dc : dcs) {
            if (dc instanceof DatacenterGreenAware g) {
                totalGreen += g.getTotalGreenUsed();
                totalBrown += g.getTotalBrownUsed();
            }
        }
        for (LocalBrokerRL broker : brokers) {
            for (Cloudlet cl : broker.getCloudletFinishedList()) {
                makespan = Math.max(makespan, cl.getFinishTime());
                executionTime += cl.getFinishTime() - cl.getStartTime();
            }
        }
        double totalEnergy = totalGreen + totalBrown;
        System.out.printf("\n=== DQN Inference Metrics ===\n");
        System.out.printf("Green Energy Ratio: %.2f%%\n", (totalEnergy > 0 ? totalGreen / totalEnergy * 100 : 0));
        System.out.printf("Total Makespan: %.2f\n", makespan);
        System.out.printf("Total Execution Time: %.2f\n", executionTime);
        System.out.printf("Total Energy Consumption: %.2f\n", totalEnergy);
    }
}
