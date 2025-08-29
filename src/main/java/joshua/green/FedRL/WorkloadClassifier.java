package joshua.green.FedRL;

import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;

/**
 * 工作负载分类器
 * 根据Google跟踪数据对任务进行分类
 */
public class WorkloadClassifier {

    public enum WorkloadType {
        BATCH_COMPUTE,      // 批处理计算任务
        WEB_SERVICE,        // Web服务
        DATA_ANALYTICS,     // 数据分析
        ML_TRAINING,        // 机器学习训练
        STREAM_PROCESSING   // 流处理
    }

    /**
     * 根据任务特征分类
     */
    public static WorkloadType classifyBySchedulingClass(int schedulingClass, int priority, double cpuRequest) {
        if (schedulingClass == 0 && priority < 100) {
            return WorkloadType.BATCH_COMPUTE;
        } else if (schedulingClass >= 2 && cpuRequest < 0.1) {
            return WorkloadType.WEB_SERVICE;
        } else if (cpuRequest > 0.5 && priority > 200) {
            return WorkloadType.ML_TRAINING;
        } else if (schedulingClass == 1) {
            return WorkloadType.DATA_ANALYTICS;
        } else {
            return WorkloadType.STREAM_PROCESSING;
        }
    }

    /**
     * 根据任务类型调整Cloudlet资源
     */
    public static void adjustResourcesByType(Cloudlet cloudlet, WorkloadType type) {
        switch (type) {
            case ML_TRAINING:
                // ML训练需要更多计算资源
                cloudlet.setLength(cloudlet.getLength() * 2);
                cloudlet.setUtilizationModelCpu(new UtilizationModelDynamic(0.8));
                break;
            case WEB_SERVICE:
                // Web服务需要快速响应
                cloudlet.setPriority(cloudlet.getPriority() + 100);
                cloudlet.setUtilizationModelCpu(new UtilizationModelDynamic(0.3));
                cloudlet.setUtilizationModelBw(new UtilizationModelDynamic(0.4));
                break;
            case DATA_ANALYTICS:
                // 数据分析需要更多内存
                cloudlet.setUtilizationModelRam(new UtilizationModelDynamic(0.8));
                cloudlet.setUtilizationModelCpu(new UtilizationModelDynamic(0.6));
                break;
            case BATCH_COMPUTE:
                // 批处理可以使用更多资源但优先级低
                cloudlet.setPriority(cloudlet.getPriority() - 50);
                cloudlet.setUtilizationModelCpu(new UtilizationModelDynamic(0.9));
                break;
            case STREAM_PROCESSING:
                // 流处理需要稳定的资源
                cloudlet.setUtilizationModelCpu(new UtilizationModelDynamic(0.5));
                cloudlet.setUtilizationModelBw(new UtilizationModelDynamic(0.6));
                break;
        }
    }
}