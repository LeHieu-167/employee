package com.employee.service;

import com.employee.config.KafkaConnectConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Service để monitor và health check Kafka Connect HDFS integration
 */
@Service
public class KafkaConnectMonitoringService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectMonitoringService.class);

    @Autowired
    private KafkaConnectConfig kafkaConnectConfig;

    @Autowired
    private WebClient kafkaConnectWebClient;

    @Autowired
    private ObjectMapper objectMapper;

    private Map<String, Object> lastHealthStatus = new HashMap<>();
    private LocalDateTime lastHealthCheck = LocalDateTime.now();

    /**
     * Scheduled health check - chạy mỗi 5 phút
     */
    @Scheduled(fixedRate = 300000) // 5 minutes
    public void performHealthCheck() {
        logger.info("Performing Kafka Connect health check...");
        
        checkKafkaConnectHealth()
            .subscribe(
                health -> {
                    lastHealthStatus = health;
                    lastHealthCheck = LocalDateTime.now();
                    
                    if ((Boolean) health.get("isHealthy")) {
                        logger.info("Kafka Connect health check passed");
                    } else {
                        logger.warn("Kafka Connect health check failed: {}", health.get("issues"));
                    }
                },
                error -> {
                    logger.error("Health check failed with error: {}", error.getMessage());
                    lastHealthStatus.put("isHealthy", false);
                    lastHealthStatus.put("error", error.getMessage());
                    lastHealthCheck = LocalDateTime.now();
                }
            );
    }

    /**
     * Thực hiện health check toàn diện
     */
    public Mono<Map<String, Object>> checkKafkaConnectHealth() {
        return Mono.fromCallable(() -> {
            Map<String, Object> healthStatus = new HashMap<>();
            healthStatus.put("timestamp", LocalDateTime.now());
            healthStatus.put("isHealthy", true);
            healthStatus.put("issues", new HashMap<String, String>());
            
            return healthStatus;
        })
        .flatMap(healthStatus -> {
            // Check Kafka Connect server
            return checkKafkaConnectServer()
                .flatMap(serverHealth -> {
                    if (!serverHealth) {
                        ((Map<String, String>) healthStatus.get("issues")).put("server", "Kafka Connect server not reachable");
                        healthStatus.put("isHealthy", false);
                    }
                    
                    // Check connector status
                    return checkConnectorHealth();
                })
                .map(connectorHealth -> {
                    healthStatus.put("connector", connectorHealth);
                    
                    if (connectorHealth.containsKey("error")) {
                        ((Map<String, String>) healthStatus.get("issues")).put("connector", (String) connectorHealth.get("error"));
                        healthStatus.put("isHealthy", false);
                    }
                    
                    return healthStatus;
                });
        });
    }

    /**
     * Kiểm tra Kafka Connect server có hoạt động không
     */
    private Mono<Boolean> checkKafkaConnectServer() {
        return kafkaConnectWebClient
            .get()
            .uri("/")
            .retrieve()
            .bodyToMono(String.class)
            .map(response -> true)
            .onErrorReturn(false);
    }

    /**
     * Kiểm tra trạng thái connector
     */
    private Mono<Map<String, Object>> checkConnectorHealth() {
        return kafkaConnectWebClient
            .get()
            .uri("/connectors/{connectorName}/status", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .map(response -> {
                Map<String, Object> connectorStatus = new HashMap<>();
                try {
                    JsonNode statusNode = objectMapper.readTree(response);
                    
                    String state = statusNode.path("connector").path("state").asText();
                    connectorStatus.put("state", state);
                    connectorStatus.put("isRunning", "RUNNING".equals(state));
                    
                    // Check tasks
                    JsonNode tasks = statusNode.path("tasks");
                    if (tasks.isArray() && tasks.size() > 0) {
                        JsonNode firstTask = tasks.get(0);
                        String taskState = firstTask.path("state").asText();
                        connectorStatus.put("taskState", taskState);
                        connectorStatus.put("taskRunning", "RUNNING".equals(taskState));
                        
                        if (!"RUNNING".equals(taskState)) {
                            String trace = firstTask.path("trace").asText();
                            connectorStatus.put("error", "Task not running: " + taskState + ", trace: " + trace);
                        }
                    } else {
                        connectorStatus.put("error", "No tasks found for connector");
                    }
                    
                } catch (Exception e) {
                    connectorStatus.put("error", "Failed to parse connector status: " + e.getMessage());
                }
                
                return connectorStatus;
            })
            .onErrorReturn(createErrorMap("Failed to get connector status"));
    }

    /**
     * Lấy metrics của connector
     */
    public Mono<Map<String, Object>> getConnectorMetrics() {
        return kafkaConnectWebClient
            .get()
            .uri("/connectors/{connectorName}/status", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .map(response -> {
                Map<String, Object> metrics = new HashMap<>();
                try {
                    JsonNode statusNode = objectMapper.readTree(response);
                    
                    // Extract metrics from status
                    metrics.put("connectorName", kafkaConnectConfig.getConnectorName());
                    metrics.put("connectorState", statusNode.path("connector").path("state").asText());
                    
                    JsonNode tasks = statusNode.path("tasks");
                    if (tasks.isArray() && tasks.size() > 0) {
                        JsonNode firstTask = tasks.get(0);
                        metrics.put("taskId", firstTask.path("id").asText());
                        metrics.put("taskState", firstTask.path("state").asText());
                        metrics.put("workerId", firstTask.path("worker_id").asText());
                    }
                    
                } catch (Exception e) {
                    metrics.put("error", "Failed to parse metrics: " + e.getMessage());
                }
                
                return metrics;
            })
            .onErrorReturn(createErrorMap("Failed to get connector metrics"));
    }

    /**
     * Lấy thông tin chi tiết về connector configuration
     */
    public Mono<Map<String, Object>> getConnectorConfig() {
        return kafkaConnectWebClient
            .get()
            .uri("/connectors/{connectorName}/config", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .map(response -> {
                Map<String, Object> config = new HashMap<>();
                try {
                    JsonNode configNode = objectMapper.readTree(response);
                    config.put("config", configNode);
                } catch (Exception e) {
                    config.put("error", "Failed to parse config: " + e.getMessage());
                }
                return config;
            })
            .onErrorReturn(createErrorMap("Failed to get connector config"));
    }

    /**
     * Lấy trạng thái health hiện tại
     */
    public Map<String, Object> getCurrentHealthStatus() {
        Map<String, Object> status = new HashMap<>(lastHealthStatus);
        status.put("lastChecked", lastHealthCheck);
        return status;
    }

    /**
     * Kiểm tra xem system có healthy không
     */
    public boolean isSystemHealthy() {
        return !lastHealthStatus.isEmpty() && 
               (Boolean) lastHealthStatus.getOrDefault("isHealthy", false);
    }

    /**
     * Helper method để tạo error map
     */
    private Map<String, Object> createErrorMap(String message) {
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("error", message);
        return errorMap;
    }

    /**
     * Restart connector nếu có vấn đề
     */
    public Mono<String> restartConnectorIfNeeded() {
        return checkConnectorHealth()
            .flatMap(connectorHealth -> {
                if (connectorHealth.containsKey("error") || 
                    !Boolean.TRUE.equals(connectorHealth.get("isRunning"))) {
                    
                    logger.warn("Connector is not healthy, attempting restart...");
                    return kafkaConnectWebClient
                        .post()
                        .uri("/connectors/{connectorName}/restart", kafkaConnectConfig.getConnectorName())
                        .retrieve()
                        .bodyToMono(String.class)
                        .doOnSuccess(result -> logger.info("Connector restart completed"))
                        .doOnError(error -> logger.error("Failed to restart connector: {}", error.getMessage()));
                } else {
                    return Mono.just("Connector is healthy, no restart needed");
                }
            });
    }
}
