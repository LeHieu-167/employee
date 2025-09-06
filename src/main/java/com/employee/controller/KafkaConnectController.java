package com.employee.controller;

import com.employee.service.KafkaConnectHdfsService;
import com.employee.service.KafkaConnectMonitoringService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.HashMap;

/**
 * Controller để quản lý và monitor Kafka Connect HDFS integration
 */
@RestController
@RequestMapping("/api/kafka-connect")
public class KafkaConnectController {

    @Autowired
    private KafkaConnectHdfsService kafkaConnectHdfsService;

    @Autowired
    private KafkaConnectMonitoringService monitoringService;

    /**
     * Lấy trạng thái health của Kafka Connect
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = monitoringService.getCurrentHealthStatus();
        
        if (monitoringService.isSystemHealthy()) {
            return ResponseEntity.ok(health);
        } else {
            return ResponseEntity.status(503).body(health); // Service Unavailable
        }
    }

    /**
     * Thực hiện health check ngay lập tức
     */
    @PostMapping("/health/check")
    public Mono<ResponseEntity<Map<String, Object>>> performHealthCheck() {
        return monitoringService.checkKafkaConnectHealth()
            .map(health -> {
                if ((Boolean) health.get("isHealthy")) {
                    return ResponseEntity.ok(health);
                } else {
                    return ResponseEntity.status(503).body(health);
                }
            });
    }

    /**
     * Lấy trạng thái connector
     */
    @GetMapping("/connector/status")
    public Mono<ResponseEntity<String>> getConnectorStatus() {
        return kafkaConnectHdfsService.getConnectorStatus()
            .map(ResponseEntity::ok)
            .onErrorReturn(ResponseEntity.status(500).body("Failed to get connector status"));
    }

    /**
     * Lấy metrics của connector
     */
    @GetMapping("/connector/metrics")
    public Mono<ResponseEntity<Map<String, Object>>> getConnectorMetrics() {
        return monitoringService.getConnectorMetrics()
            .map(ResponseEntity::ok)
            .onErrorResume(error -> {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("error", "Failed to get metrics");
                return Mono.just(ResponseEntity.status(500).body(errorMap));
            });
    }

    /**
     * Lấy configuration của connector
     */
    @GetMapping("/connector/config")
    public Mono<ResponseEntity<Map<String, Object>>> getConnectorConfig() {
        return monitoringService.getConnectorConfig()
            .map(ResponseEntity::ok)
            .onErrorResume(error -> {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("error", "Failed to get config");
                return Mono.just(ResponseEntity.status(500).body(errorMap));
            });
    }

    /**
     * Tạo hoặc recreate HDFS connector
     */
    @PostMapping("/connector/create")
    public Mono<ResponseEntity<String>> createConnector(@RequestParam(defaultValue = "false") boolean useAvro) {
        return kafkaConnectHdfsService.createHdfsConnector(useAvro)
            .map(result -> ResponseEntity.ok("Connector created successfully: " + result))
            .onErrorReturn(ResponseEntity.status(500).body("Failed to create connector"));
    }

    /**
     * Xóa connector
     */
    @DeleteMapping("/connector")
    public Mono<ResponseEntity<String>> deleteConnector() {
        return kafkaConnectHdfsService.deleteConnector()
            .map(result -> ResponseEntity.ok("Connector deleted successfully"))
            .onErrorReturn(ResponseEntity.status(500).body("Failed to delete connector"));
    }

    /**
     * Tạm dừng connector
     */
    @PostMapping("/connector/pause")
    public Mono<ResponseEntity<String>> pauseConnector() {
        return kafkaConnectHdfsService.pauseConnector()
            .map(result -> ResponseEntity.ok("Connector paused successfully"))
            .onErrorReturn(ResponseEntity.status(500).body("Failed to pause connector"));
    }

    /**
     * Tiếp tục chạy connector
     */
    @PostMapping("/connector/resume")
    public Mono<ResponseEntity<String>> resumeConnector() {
        return kafkaConnectHdfsService.resumeConnector()
            .map(result -> ResponseEntity.ok("Connector resumed successfully"))
            .onErrorReturn(ResponseEntity.status(500).body("Failed to resume connector"));
    }

    /**
     * Restart connector
     */
    @PostMapping("/connector/restart")
    public Mono<ResponseEntity<String>> restartConnector() {
        return kafkaConnectHdfsService.restartConnector()
            .map(result -> ResponseEntity.ok("Connector restarted successfully"))
            .onErrorReturn(ResponseEntity.status(500).body("Failed to restart connector"));
    }

    /**
     * Auto-restart connector nếu cần
     */
    @PostMapping("/connector/auto-restart")
    public Mono<ResponseEntity<String>> autoRestartConnector() {
        return monitoringService.restartConnectorIfNeeded()
            .map(ResponseEntity::ok)
            .onErrorReturn(ResponseEntity.status(500).body("Failed to auto-restart connector"));
    }

    /**
     * Lấy danh sách tất cả connectors
     */
    @GetMapping("/connectors")
    public Mono<ResponseEntity<String>> getAllConnectors() {
        return kafkaConnectHdfsService.getAllConnectors()
            .map(ResponseEntity::ok)
            .onErrorReturn(ResponseEntity.status(500).body("Failed to get connectors"));
    }

    /**
     * Kiểm tra xem connector có tồn tại không
     */
    @GetMapping("/connector/exists")
    public Mono<ResponseEntity<Map<String, Boolean>>> checkConnectorExists() {
        return kafkaConnectHdfsService.checkConnectorExists()
            .map(exists -> {
                Map<String, Boolean> result = new HashMap<>();
                result.put("exists", exists);
                return ResponseEntity.ok(result);
            })
            .onErrorResume(error -> {
                Map<String, Boolean> result = new HashMap<>();
                result.put("exists", false);
                return Mono.just(ResponseEntity.status(500).body(result));
            });
    }
}
