package com.employee.consumer;

import com.employee.dto.EmployeeDto;
import com.employee.entity.Employee;
import com.employee.repository.EmployeeRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EmployeeConsumer {
    private static final Logger logger = LoggerFactory.getLogger(EmployeeConsumer.class);
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private EmployeeRepository employeeRepository;

    @KafkaListener(topics = "employee-topic", groupId = "employee-group")
    public void listenEmployeeTopic(String message) {
        logger.info("Received message from employee-topic: {}", message);
        
        try {
            // Thử parse thành EmployeeDto
            EmployeeDto employeeDto = objectMapper.readValue(message, EmployeeDto.class);
            logger.info("Parsed as Employee: {}", employeeDto);
            
            // Lưu vào database
            saveEmployeeToDatabase(employeeDto);
            
        } catch (Exception e) {
            logger.error("Failed to process employee message: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "file-topic", groupId = "file-group")
    public void listenFileTopic(String message) {
        logger.info("Received message from file-topic: {}", message);
        
        try {
            // Parse file info
            Map<String, Object> fileInfo = objectMapper.readValue(message, Map.class);
            String filename = (String) fileInfo.get("filename");
            String timestamp = fileInfo.get("timestamp").toString();
            
            logger.info("File info - Name: {}, Timestamp: {}", filename, timestamp);
            
            // Có thể lưu thông tin file vào database nếu cần
            // saveFileInfoToDatabase(fileInfo);
            
        } catch (Exception e) {
            logger.error("Failed to parse file info: {}", e.getMessage());
        }
    }
    
    private void saveEmployeeToDatabase(EmployeeDto employeeDto) {
        try {
            Employee employee = new Employee();
            employee.setName(employeeDto.getName());
            employee.setEmail(employeeDto.getEmail());
            
            Employee savedEmployee = employeeRepository.save(employee);
            logger.info("Saved employee to database: {}", savedEmployee);
            
        } catch (Exception e) {
            logger.error("Failed to save employee to database: {}", e.getMessage());
        }
    }
}
