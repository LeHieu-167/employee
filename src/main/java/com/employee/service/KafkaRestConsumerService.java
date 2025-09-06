package com.employee.service;

import com.employee.config.KafkaRestConfig;
import com.employee.dto.EmployeeDto;
import com.employee.entity.Employee;
import com.employee.repository.EmployeeRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
public class KafkaRestConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaRestConsumerService.class);

    private final RestTemplate restTemplate;
    private final KafkaRestConfig kafkaRestConfig;
    private final ObjectMapper objectMapper;
    private final EmployeeRepository employeeRepository;

    @Autowired
    public KafkaRestConsumerService(RestTemplate restTemplate,
                                   KafkaRestConfig kafkaRestConfig,
                                   ObjectMapper objectMapper,
                                   EmployeeRepository employeeRepository) {
        this.restTemplate = restTemplate;
        this.kafkaRestConfig = kafkaRestConfig;
        this.objectMapper = objectMapper;
        this.employeeRepository = employeeRepository;
    }

    public List<String> consumeEmployeeMessages() {
        try {
            String consumerUrl = kafkaRestConfig.getConsumerUrl("employee-group", "employee-consumer-1");
            ResponseEntity<String> response = restTemplate.getForEntity(consumerUrl, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = objectMapper.readTree(response.getBody());
                logger.info("Consumed employee messages: {}", jsonNode);
                
                // Process each message and save to database
                processEmployeeMessages(response.getBody());
                
                return List.of(response.getBody());
            }
        } catch (Exception e) {
            logger.error("Failed to consume employee messages: {}", e.getMessage());
        }
        return List.of();
    }

    public List<String> consumeFileMessages() {
        try {
            String consumerUrl = kafkaRestConfig.getConsumerUrl("file-group", "file-consumer-1");
            ResponseEntity<String> response = restTemplate.getForEntity(consumerUrl, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = objectMapper.readTree(response.getBody());
                logger.info("Consumed file messages: {}", jsonNode);
                return List.of(response.getBody());
            }
        } catch (Exception e) {
            logger.error("Failed to consume file messages: {}", e.getMessage());
        }
        return List.of();
    }
    
    private void processEmployeeMessages(String messagesJson) {
        try {
            JsonNode messages = objectMapper.readTree(messagesJson);
            if (messages.isArray()) {
                for (JsonNode message : messages) {
                    if (message.has("value")) {
                        String value = message.get("value").asText();
                        try {
                            // Try to parse as EmployeeDto
                            EmployeeDto employeeDto = objectMapper.readValue(value, EmployeeDto.class);
                            saveEmployeeToDatabase(employeeDto);
                        } catch (Exception e) {
                            logger.warn("Message is not an Employee object: {}", value);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to process employee messages: {}", e.getMessage());
        }
    }
    
    private void saveEmployeeToDatabase(EmployeeDto employeeDto) {
        try {
            Employee employee = new Employee();
            employee.setName(employeeDto.getName());
            employee.setEmail(employeeDto.getEmail());
            
            Employee savedEmployee = employeeRepository.save(employee);
            logger.info("Saved employee to database via REST consumer: {}", savedEmployee);
            
        } catch (Exception e) {
            logger.error("Failed to save employee to database via REST consumer: {}", e.getMessage());
        }
    }
} 