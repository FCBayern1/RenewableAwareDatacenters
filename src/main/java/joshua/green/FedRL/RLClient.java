package joshua.green.FedRL;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RLClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RLClient.class);
    private static final Gson gson = new Gson();
    private final Random rnd = new Random();
    private final String agentId;
    private final String baseUrl;

    /**
     * 新构造函数，支持自定义服务器地址
     */
    public RLClient(String agentId, String host, int port) {
        this.agentId = agentId;
        this.baseUrl = String.format("http://%s:%d", host, port);
        LOGGER.info("RLClient {} connecting to {}", agentId, this.baseUrl);
    }

    /**
     * 保留原有构造函数以保持向后兼容性
     */
    public RLClient(String agentId) {
        this(agentId, "localhost", 5000);
    }

    public void triggerTrain() {
        try {
            URL url = new URL(baseUrl + "/trigger_train");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.getInputStream().close();
            LOGGER.info("Training triggered");
        } catch (Exception e) {
            LOGGER.error("Error triggering train: {}", e.getMessage());
        }
    }

    public ActionResponse selectAction(double[] state, int actionSpace) {
        try {
            String payload = gson.toJson(state);
            URL url = new URL(baseUrl + "/select_action");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            // 写入请求体
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
            }

            // 读取并解析 JSON 响应
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                ActionResponse res = gson.fromJson(br, ActionResponse.class);
                return res;
            }
        } catch (Exception e) {
            LOGGER.error("Error in selectAction: {}", e.getMessage());
            ActionResponse fallback = new ActionResponse();
            fallback.action = rnd.nextInt(actionSpace);
            fallback.log_prob = 0.0;
            fallback.value = 0.0;
            return fallback;
        }
    }

    public ActionResponse selectActionLocal(double[] state, int actionSpace) {
        try {
            URL url = new URL(baseUrl + "/select_action_local");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Map<String, Object> payload = new HashMap<>();
            payload.put("broker_id", agentId);
            payload.put("state", state);

            String jsonPayload = gson.toJson(payload);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                ActionResponse res = gson.fromJson(br, ActionResponse.class);
                return res;
            }
        } catch (Exception e) {
            LOGGER.error("Error in selectActionLocal: {}", e.getMessage());
            ActionResponse fallback = new ActionResponse();
            fallback.action = rnd.nextInt(actionSpace);
            fallback.log_prob = 0.0;
            fallback.value = 0.0;
            return fallback;
        }
    }

    public void storeExperience(double[] state, int action, double reward, double[] nextState, boolean done, double logProb, double value) {
        try {
            // 使用Map代替ObjectNode
            Map<String, Object> payload = new HashMap<>();
            payload.put("state", state);
            payload.put("action", action);
            payload.put("reward", reward);
            payload.put("nextState", nextState);
            payload.put("done", done);
            payload.put("log_prob", logProb);
            payload.put("value", value);

            // 使用gson转换
            String jsonPayload = gson.toJson(payload);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[StoreExperience] Payload to Python: {}", jsonPayload);
            }

            URL url = new URL(baseUrl + "/store_experience");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }

            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in storeExperience (PPO): {}", e.getMessage());
        }
    }

    public void storeExperienceLocal(double[] state, int action, double reward, double[] nextState, boolean done, double logProb, double value) {
        try {
            URL url = new URL(baseUrl + "/store_experience_local");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Map<String, Object> payload = new HashMap<>();

            String brokerId = agentId;
            if (agentId.startsWith("local_")) {
                brokerId = agentId.replace("local_", "");
            }

            payload.put("broker_id", brokerId);
            payload.put("state", state);
            payload.put("action", action);
            payload.put("reward", reward);
            payload.put("nextState", nextState);
            payload.put("done", done);
            payload.put("log_prob", logProb);
            payload.put("value", value);

            String jsonPayload = gson.toJson(payload);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }

            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in storeExperienceLocal: {}", e.getMessage());
        }
    }

    public void startEpisode() {
        try {
            URL url = new URL(baseUrl + "/start_episode");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in startEpisode: {}", e.getMessage());
        }
    }

    public void endEpisode() {
        try {
            URL url = new URL(baseUrl + "/end_episode");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in endEpisode: {}", e.getMessage());
        }
    }

    public void loadModel() {
        try {
            URL url = new URL(baseUrl + "/load_model");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            String json = String.format("{\"agent_id\": \"%s\"}", agentId);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                LOGGER.error("Failed to load model for agent {}, response code: {}", agentId, responseCode);
            } else {
                LOGGER.info("Model loaded successfully for agent {}", agentId);
            }
        } catch (IOException e) {
            LOGGER.error("Error in loadModel: {}", e.getMessage());
        }
    }

    public void logEpisodeMetrics(int episode, double totalInitial, double greenUsed, double totalUsed,
                                  double greenRatio, double ratio, double totalReward,
                                  double totalGreenEnergyResource, double totalSurplus, double makespan) {
        try {
            URL url = new URL(baseUrl + "/log_episode_metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            // 使用Map和Gson替代ObjectNode
            Map<String, Object> payload = new HashMap<>();
            payload.put("episode", episode);
            payload.put("totalInitial", totalInitial);
            payload.put("greenUsed", greenUsed);
            payload.put("totalUsed", totalUsed);
            payload.put("greenRatio", greenRatio);
            payload.put("ratio", ratio);
            payload.put("totalReward", totalReward);
            payload.put("totalGreenEnergyResource", totalGreenEnergyResource);
            payload.put("totalSurplus", totalSurplus);
            payload.put("makespan", makespan);

            String jsonPayload = gson.toJson(payload);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                LOGGER.error("Failed to send episode metrics to Python server, response code: {}", responseCode);
                // 读取错误响应
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        LOGGER.error("Error response: {}", line);
                    }
                }
            } else {
                LOGGER.info("Episode metrics sent successfully to Python server.");
            }

        } catch (IOException e) {
            LOGGER.error("Error in logEpisodeMetrics: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    public void clearLogFile() {
        try {
            URL url = new URL(baseUrl + "/clear_log");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                LOGGER.error("Failed to clear Python log file, code {}", responseCode);
            } else {
                LOGGER.info("Cleared Python log file successfully.");
            }
        } catch (IOException e) {
            LOGGER.error("Error in clearLogFile: {}", e.getMessage());
        }
    }

    /**
     * 检查Python服务器是否可用
     */
    public boolean checkConnection() {
        try {
            URL url = new URL(baseUrl + "/health");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                LOGGER.info("Successfully connected to Python server at {}", baseUrl);
                return true;
            } else {
                LOGGER.error("Python server returned status code: {}", responseCode);
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to connect to Python server at {}: {}", baseUrl, e.getMessage());
            return false;
        }
    }

    public static class ActionResponse {
        public int action;
        public double log_prob;
        public double value;
    }
}