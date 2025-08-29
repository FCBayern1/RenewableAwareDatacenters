package joshua.green;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import lombok.Getter;
/**
 * Description:
 * Author: joshua
 * Date: 2025/8/11
 */
@Getter
public class inferenceConfig {
    @Parameter(names = {"-h", "--help"}, help = true, description = "help info")
    private boolean help = false;

    @Parameter(names = {"--dc-num"}, description = "number of dcs")
    public int dcNum = 10;

    @Parameter(names = {"--hosts"}, description = "number of hosts of each dc")
    private int hosts = 25;

    @Parameter(names = {"--host-pes"}, description = "number of pes of each host")
    private int hostPes = 12;

    @Parameter(names = {"--host-mips"}, description = "HOST MIPS")
    private int hostMips = 10000;

    @Parameter(names = {"--host-ram"}, description = "HOST RAM (MB)")
    private long hostRam = 1024*4*10; // 16GB in MB

    @Parameter(names = {"--host-bw"}, description = "HOST BW")
    private long hostBw = 10000;

    @Parameter(names = {"--host-storage"}, description = "HOST STORAGE")
    private long hostStorage = 100000;

    @Parameter(names = {"--vms"}, description = "Total Number of VMs")
    private int vms = 10 * hosts;


    @Parameter(names = {"--vm-pes"}, description = "每个VM的PE数")
    private int vmPes = 5;

    @Parameter(names = {"--cloudlets"}, description = "任务数量")
    private int cloudlets = 10000;

    @Parameter(names = {"--python-host"}, description = "Python服务器地址")
    private String pythonHost = "localhost";

    @Parameter(names = {"--python-port"}, description = "Python服务器端口")
    private int pythonPort = 5001;

    // 文件路径
    @Parameter(names = {"--cloudlet-file"}, description = "Cloudlet CSV Path")
    private String cloudletFile = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/data/processed_cloudlet_data.csv";

    @Parameter(names = {"--sim-step"}, description = "仿真时间步长")
    private double simStep = 0.001;

    @Parameter(names = {"--green-factor-min"}, description = "绿色能源生成因子最小值")
    private double greenFactorMin = 0.001;

    @Parameter(names = {"--green-factor-max"}, description = "绿色能源生成因子最大值")
    private double greenFactorMax = 0.01;

    @Parameter(names = {"--green-initial-min"}, description = "初始绿色能源最小值")
    private double greenInitialMin = 0.1;

    @Parameter(names = {"--green-initial-max"}, description = "初始绿色能源最大值")
    private double greenInitialMax = 1;

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

    @Parameter(names = {"--initial-energy"}, description = "initial energy array")
    private double[] initialEnergyArray = {0.8, 0.05, 0.3, 0.01, 0.02, 0.5, 0.1, 0.03, 0.4, 0.2};

    @Parameter(names = {"--initial-scale-factor"}, description = "initial scale factor array")
    private double[] initialScaleFactorArray = {0.015, 0.001, 0.008, 0.002, 0.003, 0.012, 0.005, 0.002, 0.01, 0.006};

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

    public void printConfig() {
        String separator = repeat("=", 70);
        System.out.println(separator);
        System.out.println("SCHEDULING SIMULATION");
        System.out.println(separator);
        System.out.println("Configuration:");
        System.out.println("  Data Centers: " + dcNum);
        System.out.println("  Hosts per DC: " + hosts);
        System.out.println("  VMs: " + vms);
        System.out.println("  Cloudlets: " + cloudlets);
        System.out.println("\nPython Server:");
        System.out.println("  Host: " + pythonHost);
        System.out.println("  Port: " + pythonPort);
        System.out.println("\nFiles:");
        System.out.println("  Cloudlet File: " + cloudletFile);
        System.out.println(separator + "\n");
    }

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
