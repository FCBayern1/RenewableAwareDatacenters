package joshua.green.dqn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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

    public int selectAction(double[] state, int actionSpace) {
        try {
            String payload = gson.toJson(state);
            URL url = new URL(BASE_URL + "/select_action");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                return Integer.parseInt(br.readLine().trim());
            }
        } catch (Exception e) {
            LOGGER.error("Error in selectAction: {}", e.getMessage());
            return rnd.nextInt(actionSpace);
        }
    }

    public void storeExperience(double[] state, int action, double reward, double[] nextState) {
        try {
            Experience exp = new Experience(state, action, reward, nextState);
            String payload = gson.toJson(exp);

            URL url = new URL(BASE_URL + "/store_experience");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
            }

            conn.getInputStream().close();
            conn.disconnect();
        } catch (Exception e) {
            LOGGER.error("Error in storeExperience: {}", e.getMessage());
        }
    }

    public int selectActionLocal(double[] state, int actionSpace) {
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
                return Integer.parseInt(br.readLine().trim());
            }
        } catch (Exception e) {
            LOGGER.error("Error in selectActionLocal: {}", e.getMessage());
            return rnd.nextInt(actionSpace);
        }
    }

    public void storeExperienceLocal(double[] state, int action, double reward, double[] nextState) {
        try {
            URL url = new URL(BASE_URL + "/store_experience_local");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            ObjectNode payload = mapper.createObjectNode();
            payload.put("broker_id", Integer.parseInt(agentId));
            payload.putPOJO("state", state);
            payload.put("action", action);
            payload.put("reward", reward);
            payload.putPOJO("nextState", nextState);

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


    private static class Experience {
        double[] state;
        int action;
        double reward;
        double[] nextState;

        Experience(double[] state, int action, double reward, double[] nextState) {
            this.state = state;
            this.action = action;
            this.reward = reward;
            this.nextState = nextState;
        }
    }
}
