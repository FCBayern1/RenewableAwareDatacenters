package joshua.green.data;

/**
 * Description:
 * Author: joshua
 * Date: 2025/7/29
 */
// TestFileReader.java
import java.io.BufferedReader;
import java.io.FileReader;

public class test {
    public static void main(String[] args) {
        String filePath = "/Users/joshua/Downloads/RenewableAwareDatacenters/src/main/java/joshua/green/data/borg_traces_data.csv";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // 读取前5行
            for (int i = 0; i < 5; i++) {
                String line = br.readLine();
                if (line == null) break;

                System.out.println("Line " + i + " length: " + line.length());
                System.out.println("First 100 chars: " + line.substring(0, Math.min(100, line.length())));

                // 检查分隔符
                String[] tabSplit = line.split("\t");
                String[] commaSplit = line.split(",");

                System.out.println("Tab-separated fields: " + tabSplit.length);
                System.out.println("Comma-separated fields: " + commaSplit.length);
                System.out.println("---");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}