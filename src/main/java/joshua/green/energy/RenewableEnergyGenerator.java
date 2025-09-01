package joshua.green.energy;

/**
 * 可再生能源生成器接口
 */
public interface RenewableEnergyGenerator {

    /**
     * 获取指定时间的功率输出（瓦特）
     * @param simulationTime 仿真时间（秒）
     * @return 功率（瓦特）
     */
    double getPowerAtTime(double simulationTime);

    /**
     * 获取时间段内的总能量（焦耳）
     * @param startTime 开始时间（秒）
     * @param duration 持续时间（秒）
     * @return 能量（焦耳）
     */
    double getEnergyForPeriod(double startTime, double duration);


}