package com.employee.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * DTO cho File events được gửi tới Kafka Connect HDFS
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileEvent {
    
    private String eventId;
    private String eventType; // UPLOAD, DELETE, PROCESS
    private String fileName;
    private String filePath;
    private Long fileSize;
    private String contentType;
    private String checksum; // MD5 hoặc SHA256
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;
    
    private String uploadedBy; // User hoặc system
    private String source; // Nguồn upload (WEB_UI, API, BATCH, etc.)
    private String version; // Version của schema
    
    // Metadata cho HDFS partitioning
    private String year;
    private String month;
    private String day;
    private String hour;
    
    // File processing metadata
    private String processingStatus; // PENDING, PROCESSING, COMPLETED, FAILED
    private String errorMessage; // Nếu có lỗi
    
    /**
     * Constructor để tạo file event
     */
    public FileEvent(String eventType, String fileName, String filePath, Long fileSize, 
                     String contentType, String uploadedBy, String source) {
        this.eventId = java.util.UUID.randomUUID().toString();
        this.eventType = eventType;
        this.fileName = fileName;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.contentType = contentType;
        this.uploadedBy = uploadedBy;
        this.source = source;
        this.timestamp = LocalDateTime.now();
        this.version = "1.0";
        this.processingStatus = "PENDING";
        
        // Set partition fields
        this.year = String.valueOf(timestamp.getYear());
        this.month = String.format("%02d", timestamp.getMonthValue());
        this.day = String.format("%02d", timestamp.getDayOfMonth());
        this.hour = String.format("%02d", timestamp.getHour());
    }
    
    /**
     * Factory method để tạo UPLOAD event
     */
    public static FileEvent uploadEvent(String fileName, String filePath, Long fileSize, 
                                       String contentType, String uploadedBy, String source) {
        return new FileEvent("UPLOAD", fileName, filePath, fileSize, contentType, uploadedBy, source);
    }
    
    /**
     * Factory method để tạo DELETE event
     */
    public static FileEvent deleteEvent(String fileName, String filePath, String source) {
        return new FileEvent("DELETE", fileName, filePath, null, null, null, source);
    }
    
    /**
     * Factory method để tạo PROCESS event
     */
    public static FileEvent processEvent(String fileName, String filePath, String source) {
        FileEvent event = new FileEvent("PROCESS", fileName, filePath, null, null, null, source);
        event.setProcessingStatus("PROCESSING");
        return event;
    }
}
