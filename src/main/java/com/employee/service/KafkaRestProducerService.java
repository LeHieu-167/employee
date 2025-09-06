package com.employee.service;

import com.employee.config.KafkaRestConfig;
import com.employee.dto.EmployeeDto;
import com.employee.entity.Employee;
import java.util.Base64;    
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.*;

@Service
public class KafkaRestProducerService {

    private final RestTemplate restTemplate;
    private final HttpHeaders kafkaRestHeaders;
    private final KafkaRestConfig kafkaRestConfig;
    private final ObjectMapper objectMapper;

    @Autowired
    public KafkaRestProducerService(RestTemplate restTemplate, 
                           HttpHeaders kafkaRestHeaders, 
                           KafkaRestConfig kafkaRestConfig,
                           ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.kafkaRestHeaders = kafkaRestHeaders;
        this.kafkaRestConfig = kafkaRestConfig;
        this.objectMapper = objectMapper;
    }

    // Gửi string message
    public void sendMessage(String message) {
        sendToTopic("employee-topic", message);
    }

    // Gửi EmployeeDto object
    public void sendEmployee(EmployeeDto employeeDto) {
        try {
            Map<String, Object> employeeValue = new HashMap<>();
            employeeValue.put("id", employeeDto.getId());
            employeeValue.put("name", employeeDto.getName());
            employeeValue.put("email", employeeDto.getEmail());

            sendValueToTopic("employee-topic", employeeValue);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize employee", e);
        }
    }

    // Gửi Employee entity object
    public void sendEmployeeEntity(Employee employee) {
        try {
            Map<String, Object> employeeValue = new HashMap<>();
            employeeValue.put("id", employee.getId());
            employeeValue.put("name", employee.getName());
            employeeValue.put("email", employee.getEmail());

            sendValueToTopic("employee-topic", employeeValue);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize employee entity", e);
        }
    }

    // Gửi file data
    public void sendFileData(String filename, byte[] fileData) {
        try {
            Map<String, Object> fileInfo = new HashMap<>();
            fileInfo.put("filename", filename);
            fileInfo.put("data", Base64.getEncoder().encodeToString(fileData));
            fileInfo.put("timestamp", System.currentTimeMillis());
            
            String fileJson = objectMapper.writeValueAsString(fileInfo);
            sendToTopic("file-topic", fileJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize file data", e);
        }
    }

    // Gửi JSON object
    public void sendJsonObject(Object obj) {
        try {
            sendValueToTopic("employee-topic", obj);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    private void sendToTopic(String topic, String message) {
        sendValueToTopic(topic, message);
    }

    private void sendValueToTopic(String topic, Object value) {
        String topicUrl = kafkaRestConfig.getProducerUrl(topic);

        Map<String, Object> record = new HashMap<>();
        record.put("value", value);

        Map<String, Object> payload = new HashMap<>();
        payload.put("records", Collections.singletonList(record));

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, kafkaRestHeaders);

        ResponseEntity<String> response = restTemplate.postForEntity(topicUrl, request, String.class);
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Failed to send message to Kafka REST Proxy: " + response.getBody());
        }
    }
}
