package com.employee.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * DTO cho Employee events được gửi tới Kafka Connect HDFS
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmployeeEvent {
    
    private String eventId;
    private String eventType; // CREATE, UPDATE, DELETE
    private Long employeeId;
    private String employeeName;
    private String employeeEmail;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;
    
    private String source; // Nguồn tạo event (REST_API, BATCH_PROCESS, etc.)
    private String version; // Version của schema
    
    // Metadata cho HDFS partitioning
    private String year;
    private String month;
    private String day;
    private String hour;
    
    /**
     * Constructor để tạo event từ Employee entity
     */
    public EmployeeEvent(String eventType, Long employeeId, String employeeName, String employeeEmail, String source) {
        this.eventId = java.util.UUID.randomUUID().toString();
        this.eventType = eventType;
        this.employeeId = employeeId;
        this.employeeName = employeeName;
        this.employeeEmail = employeeEmail;
        this.timestamp = LocalDateTime.now();
        this.source = source;
        this.version = "1.0";
        
        // Set partition fields
        this.year = String.valueOf(timestamp.getYear());
        this.month = String.format("%02d", timestamp.getMonthValue());
        this.day = String.format("%02d", timestamp.getDayOfMonth());
        this.hour = String.format("%02d", timestamp.getHour());
    }
    
    /**
     * Factory method để tạo CREATE event
     */
    public static EmployeeEvent createEvent(Long employeeId, String employeeName, String employeeEmail, String source) {
        return new EmployeeEvent("CREATE", employeeId, employeeName, employeeEmail, source);
    }
    
    /**
     * Factory method để tạo UPDATE event
     */
    public static EmployeeEvent updateEvent(Long employeeId, String employeeName, String employeeEmail, String source) {
        return new EmployeeEvent("UPDATE", employeeId, employeeName, employeeEmail, source);
    }
    
    /**
     * Factory method để tạo DELETE event
     */
    public static EmployeeEvent deleteEvent(Long employeeId, String source) {
        return new EmployeeEvent("DELETE", employeeId, null, null, source);
    }
}
