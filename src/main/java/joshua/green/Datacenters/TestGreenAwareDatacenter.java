package joshua.green.Datacenters;

import joshua.green.Datacenters.DatacenterGreenAware;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicySimple;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.vm.VmScheduler;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;
import org.cloudsimplus.power.models.PowerModelHostSimple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Description:
 * Author: joshua
 * Date: 2025/4/11
 */
public class TestGreenAwareDatacenter {
    public static void main(String[] args) {
        CloudSimPlus simulation = new CloudSimPlus(0.001);

        final var peList = new ArrayList<Pe>(8);
        for (int i = 0; i < 8; i++) {
            peList.add(new PeSimple(1000));
        }
        final var host = new HostSimple(2048, 10000, 1000000, peList);
        final var powerModel = new PowerModelHostSimple(50,35);
        powerModel.setStartupPower(5);
        powerModel.setShutDownPower(3);
        host.setPowerModel(powerModel);
        final var vmScheduler = new VmSchedulerTimeShared();
        host.setId(1).setVmScheduler(vmScheduler);
        host.enableUtilizationStats();
        List<Host> hostList = Collections.singletonList(host);
        final var dc = new DatacenterGreenAware(simulation, hostList, new VmAllocationPolicySimple(), 1000);
        DatacenterBrokerSimple broker = new DatacenterBrokerSimple(simulation);
        broker.setDatacenterMapper((lastDc, vm) -> dc);

        Vm vm = new VmSimple(500, 4);
        vm.setRam(512).setBw(1000).setSize(1024);
        broker.submitVm(vm);

        final var utilizationModel = new UtilizationModelDynamic(0.5);
        CloudletSimple cloudlet = new CloudletSimple(4000,1, utilizationModel);

        cloudlet.setFileSize(300).setOutputSize(300);
        broker.submitCloudlet(cloudlet);

        simulation.start();

        System.out.println("\nCloudlet Execution Result:");
        final var cloudletFinishedList = broker.getCloudletFinishedList();
        System.out.println("Cloudlet finished:"+ cloudletFinishedList.size());
        new CloudletsTableBuilder(cloudletFinishedList).build();





    }
}
