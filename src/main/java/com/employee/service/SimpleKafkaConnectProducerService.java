package com.employee.service;

import com.employee.dto.EmployeeEvent;
import com.employee.dto.FileEvent;
import com.employee.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Simplified producer service để gửi structured events tới Kafka Connect
 * Không phụ thuộc vào WebFlux
 */
@Service
public class SimpleKafkaConnectProducerService {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConnectProducerService.class);
    
    private static final String EMPLOYEE_EVENTS_TOPIC = "employee-events";
    private static final String FILE_EVENTS_TOPIC = "file-events";
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Gửi employee create event
     */
    public void sendEmployeeCreateEvent(Employee employee) {
        try {
            EmployeeEvent event = EmployeeEvent.createEvent(
                employee.getId(), 
                employee.getName(), 
                employee.getEmail(), 
                "REST_API"
            );
            
            sendEmployeeEvent(event);
            logger.info("Sent employee create event for ID: {}", employee.getId());
            
        } catch (Exception e) {
            logger.error("Failed to send employee create event: {}", e.getMessage());
        }
    }
    
    /**
     * Gửi employee update event
     */
    public void sendEmployeeUpdateEvent(Employee employee) {
        try {
            EmployeeEvent event = EmployeeEvent.updateEvent(
                employee.getId(), 
                employee.getName(), 
                employee.getEmail(), 
                "REST_API"
            );
            
            sendEmployeeEvent(event);
            logger.info("Sent employee update event for ID: {}", employee.getId());
            
        } catch (Exception e) {
            logger.error("Failed to send employee update event: {}", e.getMessage());
        }
    }
    
    /**
     * Gửi employee delete event
     */
    public void sendEmployeeDeleteEvent(Long employeeId) {
        try {
            EmployeeEvent event = EmployeeEvent.deleteEvent(employeeId, "REST_API");
            sendEmployeeEvent(event);
            logger.info("Sent employee delete event for ID: {}", employeeId);
            
        } catch (Exception e) {
            logger.error("Failed to send employee delete event: {}", e.getMessage());
        }
    }
    
    /**
     * Gửi employee event tới Kafka
     */
    private void sendEmployeeEvent(EmployeeEvent event) {
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            String key = event.getEmployeeId() != null ? event.getEmployeeId().toString() : event.getEventId();
            
            kafkaTemplate.send(EMPLOYEE_EVENTS_TOPIC, key, eventJson);
            logger.debug("Successfully sent employee event: {}", event.getEventType());
            
        } catch (Exception e) {
            logger.error("Error sending employee event: {}", e.getMessage());
        }
    }
    
    /**
     * Gửi file upload event
     */
    public void sendFileUploadEvent(String fileName, String filePath, Long fileSize, 
                                   String contentType, String uploadedBy) {
        try {
            FileEvent event = FileEvent.uploadEvent(fileName, filePath, fileSize, 
                contentType, uploadedBy, "WEB_UI");
            sendFileEvent(event);
            logger.info("Sent file upload event for: {}", fileName);
            
        } catch (Exception e) {
            logger.error("Failed to send file upload event: {}", e.getMessage());
        }
    }
    
    /**
     * Gửi file delete event
     */
    public void sendFileDeleteEvent(String fileName, String filePath) {
        try {
            FileEvent event = FileEvent.deleteEvent(fileName, filePath, "API");
            sendFileEvent(event);
            logger.info("Sent file delete event for: {}", fileName);
            
        } catch (Exception e) {
            logger.error("Failed to send file delete event: {}", e.getMessage());
        }
    }
    
    /**
     * Gửi file event tới Kafka
     */
    private void sendFileEvent(FileEvent event) {
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            String key = event.getFileName();
            
            kafkaTemplate.send(FILE_EVENTS_TOPIC, key, eventJson);
            logger.debug("Successfully sent file event: {}", event.getEventType());
            
        } catch (Exception e) {
            logger.error("Error sending file event: {}", e.getMessage());
        }
    }
}
