package joshua.green.ppo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Map;
import java.util.Random;


public class RLClient {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(RLClient.class);
    private static final String BASE_URL = "http://localhost:5000";
    private static final Gson gson = new Gson();
    private final Random rnd = new Random();
    private final String agentId;

    public RLClient(String agentId) {
        this.agentId = agentId;
    }

    public void triggerTrain() {
        try {
            URL url = new URL(BASE_URL + "/trigger_train");
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
            URL url = new URL(BASE_URL + "/select_action");
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
//                LOGGER.info("Received action: {}, log_prob: {}, value: {}", res.action, res.log_prob, res.value);
                int action = res.action;
                double logProb = res.log_prob;
                double value = res.value;
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


    public void storeExperience(double[] state, int action, double reward, double[] nextState, boolean done, double logProb, double value) {
        System.out.println("Calling storeExperienceGlobal: action=" + action + ", reward=" + reward);
        try {
            ObjectNode payload = mapper.createObjectNode();
            payload.putPOJO("state", state);
            payload.put("action", action);
            payload.put("reward", reward);
            payload.putPOJO("nextState", nextState);
            payload.put("done", done);

            payload.put("log_prob", logProb);
            payload.put("value", value);

            LOGGER.info("[StoreExperience] Payload to Python: {}", mapper.writeValueAsString(payload));

            URL url = new URL(BASE_URL + "/store_experience");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                mapper.writeValue(os, payload);
            }

            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in storeExperience (PPO): {}", e.getMessage());
        }
    }


    public ActionResponse selectActionLocal(double[] state, int actionSpace) {
        try {
            URL url = new URL(BASE_URL + "/select_action_local");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            ObjectNode payload = mapper.createObjectNode();
            payload.put("broker_id", agentId);
            payload.putPOJO("state", state);

            try (OutputStream os = conn.getOutputStream()) {
                mapper.writeValue(os, payload);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                ActionResponse res = mapper.readValue(br, ActionResponse.class);
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



    public void storeExperienceLocal(double[] state, int action, double reward, double[] nextState, boolean done, double logProb, double value) {
        System.out.println("Calling storeExperienceLocal: action=" + action + ", reward=" + reward);

        try {
            URL url = new URL(BASE_URL + "/store_experience_local");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            ObjectNode payload = mapper.createObjectNode();

            int brokerId = Integer.parseInt(agentId.replace("local_", ""));
            payload.put("broker_id", brokerId);
            payload.putPOJO("state", state);
            payload.put("action", action);
            payload.put("reward", reward);
            payload.putPOJO("nextState", nextState);
            payload.put("done", done);
            payload.put("log_prob", logProb);
            payload.put("value", value);

            try (OutputStream os = conn.getOutputStream()) {
                mapper.writeValue(os, payload);
            }

            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in storeExperienceLocal: {}", e.getMessage());
        }
    }


    public void startEpisode() {
        try {
            URL url = new URL(BASE_URL + "/start_episode");
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
            URL url = new URL(BASE_URL + "/end_episode");
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
            URL url = new URL(BASE_URL + "/load_model");
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

    public void logEpisodeMetrics( int episode, double totalInitial, double greenUsed, double totalUsed, double greenRatio, double ratio, double totalReward, double totalGreenEnergyResource, double totalSurplus, double makespan) {
        try {
            URL url = new URL(BASE_URL + "/log_episode_metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            String payload = String.format(
                    "{\"episode\": %d, \"greenInitial\": %.4f, \"greenUsed\": %.4f, \"totalUsed\": %.4f, \"greenRatio\": %.4f, \"ratio\": %.4f, \"totalReward\": %.4f, \"totalGreenEnergyResource\": %.4f, \"totalSurplus\": %.4f, \"makespan\": %.4f}",
                    episode, totalInitial, greenUsed, totalUsed, greenRatio, ratio, totalReward, totalGreenEnergyResource, totalSurplus, makespan
            );

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = payload.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                LOGGER.error("Failed to send episode metrics to Python server, response code: {}", responseCode);
            } else {
                LOGGER.info("Episode metrics sent successfully to Python server.");
            }

        } catch (IOException e) {
            LOGGER.error("Error in logEpisodeMetrics: {}", e.getMessage());
        }
    }

    public void clearLogFile() {
        try {
            URL url = new URL(BASE_URL + "/clear_log");
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


    public class ActionResponse {
        public int action;
        public double log_prob;
        public double value;
    }


}
