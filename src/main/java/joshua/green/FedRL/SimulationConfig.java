package joshua.green.FedRL;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import lombok.Getter;

/**
 * 仿真配置类，支持命令行参数
 */
@Getter
public class SimulationConfig {

    @Parameter(names = {"-h", "--help"}, help = true, description = "显示帮助信息")
    private boolean help = false;

    // 基础配置
    @Parameter(names = {"--episodes"}, description = "运行的episode数量")
    private int episodes = 20;

    @Parameter(names = {"--dc-num"}, description = "数据中心数量")
    private int dcNum = 10;

    @Parameter(names = {"--hosts"}, description = "每个数据中心的主机数量")
    private int hosts = 4;

    @Parameter(names = {"--host-pes"}, description = "每个主机的PE数量")
    private int hostPes = 8;

    @Parameter(names = {"--host-mips"}, description = "主机基础MIPS")
    private int hostMips = 1000;

    @Parameter(names = {"--host-ram"}, description = "主机RAM (MB)")
    private long hostRam = 16384; // 16GB in MB

    @Parameter(names = {"--host-bw"}, description = "主机带宽")
    private long hostBw = 10000;

    @Parameter(names = {"--host-storage"}, description = "主机存储")
    private long hostStorage = 1000000;

    @Parameter(names = {"--vms"}, description = "虚拟机总数")
    private int vms = 40;

    @Parameter(names = {"--vm-pes"}, description = "每个VM的PE数")
    private int vmPes = 2;

    @Parameter(names = {"--cloudlets"}, description = "任务数量")
    private int cloudlets = 2000;

    // Python服务器配置
    @Parameter(names = {"--python-host"}, description = "Python服务器地址")
    private String pythonHost = "localhost";

    @Parameter(names = {"--python-port"}, description = "Python服务器端口")
    private int pythonPort = 5001;

    // 文件路径
    @Parameter(names = {"--cloudlet-file"}, description = "Cloudlet CSV文件路径")
    private String cloudletFile = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/data/borg_traces_data.csv";

    @Parameter(names = {"--processed-cloudlet-file"}, description = "Cloudlet CSV")
    private String processedCloudletFile = "/Users/joshua/CloudsimPlus_py4j/Fed_RL/borg_cloudlets_processed.csv";

    @Parameter(names = {"--use-processed-data"}, description = "是否使用预处理的数据")
    private boolean useProcessedData = false;
    @Parameter(names = {"--output-dir"}, description = "输出目录")
    private String outputDir = "output";

    @Parameter(names = {"--log-dir"}, description = "日志目录")
    private String logDir = "logs";

    // 仿真参数
    @Parameter(names = {"--sim-step"}, description = "仿真时间步长")
    private double simStep = 0.001;

    @Parameter(names = {"--green-factor-min"}, description = "绿色能源生成因子最小值")
    private double greenFactorMin = 0.5;

    @Parameter(names = {"--green-factor-max"}, description = "绿色能源生成因子最大值")
    private double greenFactorMax = 2.5;

    @Parameter(names = {"--green-initial-min"}, description = "初始绿色能源最小值")
    private double greenInitialMin = 1000;

    @Parameter(names = {"--green-initial-max"}, description = "初始绿色能源最大值")
    private double greenInitialMax = 4000;

    // 调试选项
    @Parameter(names = {"--debug"}, description = "启用调试模式")
    private boolean debug = false;

    @Parameter(names = {"--verbose"}, description = "详细输出")
    private boolean verbose = false;

    @Parameter(names = {"--dry-run"}, description = "试运行（不执行实际仿真）")
    private boolean dryRun = false;

    @Parameter(names = {"--wind-data"}, description = "风电数据文件路径（多个文件用逗号分隔）")
    private String windDataPath = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/Datacenters/Turbine_1_2021.csv," +
            "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/Datacenters/Turbine_57_2021.csv," +
            "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/Datacenters/Turbine_124_2021.csv";

    @Parameter(names = {"--solar-data"}, description = "太阳能数据文件路径")
    private String solarDataPath = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/Datacenters/Plant_1_Generation_Data.csv";

    @Parameter(names = {"--use-real-energy"}, description = "使用真实能源数据")
    private boolean useRealEnergyData = false;

    @Parameter(names = {"--solar-peak"}, description = "太阳能峰值功率(W)")
    private double solarPeakPower = 1000000;

    @Parameter(names = {"--hybrid-energy"}, description = "启用混合能源模式（太阳能+风能）")
    private boolean enableHybridEnergy = false;
    /**
     * 解析命令行参数
     */
    public static SimulationConfig parse(String[] args) {
        SimulationConfig config = new SimulationConfig();
        JCommander commander = JCommander.newBuilder()
                .addObject(config)
                .build();

        commander.setProgramName("rltest");
        commander.parse(args);

        if (config.isHelp()) {
            commander.usage();
            System.exit(0);
        }

        return config;
    }

    /**
     * 打印配置信息
     */
    public void printConfig() {
        String separator = repeat("=", 70);
        System.out.println(separator);
        System.out.println("FEDERATED RL GREEN ENERGY SCHEDULING SIMULATION");
        System.out.println(separator);
        System.out.println("Configuration:");
        System.out.println("  Episodes: " + episodes);
        System.out.println("  Data Centers: " + dcNum);
        System.out.println("  Hosts per DC: " + hosts);
        System.out.println("  VMs: " + vms);
        System.out.println("  Cloudlets: " + cloudlets);
        System.out.println("\nPython Server:");
        System.out.println("  Host: " + pythonHost);
        System.out.println("  Port: " + pythonPort);
        System.out.println("\nFiles:");
        System.out.println("  Cloudlet File: " + cloudletFile);
        System.out.println("  Output Dir: " + outputDir);
        System.out.println(separator + "\n");
    }

    /**
     * 重复字符串的辅助方法
     */
    private static String repeat(String str, int count) {
        // Java 11+ 可以使用 str.repeat(count)
        // 对于旧版本 Java：
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}