package com.employee.service;

import com.employee.config.KafkaConnectConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;

import java.util.HashMap;
import java.util.Map;

@Configuration
class WebClientConfig {
    @Bean
    public WebClient kafkaConnectWebClient(@Value("${kafka.connect.url}") String kafkaConnectUrl) {
        return WebClient.builder()
                .baseUrl(kafkaConnectUrl)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10MB
                .build();
    }
}

@Service
public class KafkaConnectHdfsService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectHdfsService.class);

    @Autowired
    private KafkaConnectConfig kafkaConnectConfig;

    @Autowired
    private WebClient kafkaConnectWebClient;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Khởi tạo HDFS Sink Connector khi ứng dụng khởi động
     */
    @PostConstruct
    public void initializeHdfsConnector() {
        logger.info("Starting HDFS connector initialization...");
        
        // Kiểm tra Kafka Connect server trước
        checkKafkaConnectServer()
            .flatMap(serverRunning -> {
                if (!serverRunning) {
                    logger.warn("Kafka Connect server is not running at {}. Skipping connector initialization.", 
                               kafkaConnectConfig.getKafkaConnectUrl());
                    return Mono.just("Kafka Connect server not available");
                }
                
                logger.info("Kafka Connect server is running, checking connector...");
                
                // Kiểm tra xem connector đã tồn tại chưa
                return checkConnectorExists()
                    .flatMap(exists -> {
                        if (exists) {
                            logger.info("HDFS connector already exists, checking status...");
                            return getConnectorStatus();
                        } else {
                            logger.info("Creating new HDFS connector...");
                            return createHdfsConnector();
                        }
                    });
            })
            .subscribe(
                result -> logger.info("HDFS connector initialization completed: {}", result),
                error -> logger.error("Failed to initialize HDFS connector: {}", error.getMessage())
            );
    }
    
    /**
     * Kiểm tra Kafka Connect server có chạy không
     */
    private Mono<Boolean> checkKafkaConnectServer() {
        return kafkaConnectWebClient
            .get()
            .uri("/")
            .retrieve()
            .bodyToMono(String.class)
            .map(response -> {
                logger.info("Kafka Connect server is running");
                return true;
            })
            .onErrorReturn(false);
    }

    /**
     * Tạo HDFS Sink Connector mới
     */
    public Mono<String> createHdfsConnector() {
        return createHdfsConnector(false); // Mặc định sử dụng JSON format
    }

    /**
     * Tạo HDFS Sink Connector với tùy chọn format
     * @param useAvro true để sử dụng Avro format, false để sử dụng JSON
     */
    public Mono<String> createHdfsConnector(boolean useAvro) {
        try {
            Map<String, Object> connectorConfig = useAvro ? 
                kafkaConnectConfig.createAvroHdfsSinkConnectorConfig() :
                kafkaConnectConfig.createHdfsSinkConnectorConfig();

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("config", connectorConfig);
            String jsonBody = objectMapper.writeValueAsString(requestBody);

            return kafkaConnectWebClient
                .post()
                .uri("/connectors")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonBody)
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), response -> {
                    return response.bodyToMono(String.class)
                        .flatMap(errorBody -> {
                            logger.error("Client error creating connector: {}", errorBody);
                            throw new RuntimeException("Failed to create connector: " + errorBody);
                        });
                })
                .onStatus(status -> status.is5xxServerError(), response -> {
                    return response.bodyToMono(String.class)
                        .flatMap(errorBody -> {
                            logger.error("Server error creating connector: {}", errorBody);
                            throw new RuntimeException("Server error creating connector: " + errorBody);
                        });
                })
                .bodyToMono(String.class)
                .doOnSuccess(response -> {
                    logger.info("Successfully created HDFS connector: {}", response);
                })
                .doOnError(error -> {
                    logger.error("Failed to create HDFS connector: {}", error.getMessage());
                });

        } catch (Exception e) {
            logger.error("Error preparing connector configuration: {}", e.getMessage());
            return Mono.error(e);
        }
    }

    /**
     * Kiểm tra trạng thái của connector
     */
    public Mono<String> getConnectorStatus() {
        return kafkaConnectWebClient
            .get()
            .uri("/connectors/{connectorName}/status", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(status -> {
                logger.info("Connector status: {}", status);
            })
            .doOnError(error -> {
                logger.error("Failed to get connector status: {}", error.getMessage());
            });
    }

    /**
     * Kiểm tra xem connector đã tồn tại chưa
     */
    public Mono<Boolean> checkConnectorExists() {
        return kafkaConnectWebClient
            .get()
            .uri("/connectors/{connectorName}", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .map(response -> {
                logger.info("Connector {} exists", kafkaConnectConfig.getConnectorName());
                return true;
            })
            .onErrorResume(error -> {
                logger.debug("Connector {} does not exist: {}", kafkaConnectConfig.getConnectorName(), error.getMessage());
                return Mono.just(false);
            });
    }

    /**
     * Xóa connector
     */
    public Mono<String> deleteConnector() {
        return kafkaConnectWebClient
            .delete()
            .uri("/connectors/{connectorName}", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(response -> {
                logger.info("Successfully deleted connector: {}", response);
            })
            .doOnError(error -> {
                logger.error("Failed to delete connector: {}", error.getMessage());
            });
    }

    /**
     * Tạm dừng connector
     */
    public Mono<String> pauseConnector() {
        return kafkaConnectWebClient
            .put()
            .uri("/connectors/{connectorName}/pause", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(response -> {
                logger.info("Successfully paused connector");
            })
            .doOnError(error -> {
                logger.error("Failed to pause connector: {}", error.getMessage());
            });
    }

    /**
     * Tiếp tục chạy connector
     */
    public Mono<String> resumeConnector() {
        return kafkaConnectWebClient
            .put()
            .uri("/connectors/{connectorName}/resume", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(response -> {
                logger.info("Successfully resumed connector");
            })
            .doOnError(error -> {
                logger.error("Failed to resume connector: {}", error.getMessage());
            });
    }

    /**
     * Restart connector
     */
    public Mono<String> restartConnector() {
        return kafkaConnectWebClient
            .post()
            .uri("/connectors/{connectorName}/restart", kafkaConnectConfig.getConnectorName())
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(response -> {
                logger.info("Successfully restarted connector");
            })
            .doOnError(error -> {
                logger.error("Failed to restart connector: {}", error.getMessage());
            });
    }

    /**
     * Lấy danh sách tất cả connectors
     */
    public Mono<String> getAllConnectors() {
        return kafkaConnectWebClient
            .get()
            .uri("/connectors")
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(connectors -> {
                logger.info("Available connectors: {}", connectors);
            })
            .doOnError(error -> {
                logger.error("Failed to get connectors list: {}", error.getMessage());
            });
    }
}
