# Kafka Connect HDFS Integration Guide

## Tổng Quan

Ứng dụng đã được cập nhật để sử dụng Kafka Connect với HDFS Sink Connector, cho phép tự động chuyển dữ liệu từ Kafka topics vào HDFS một cách hiệu quả và có cấu trúc.

## Kiến Trúc Mới

### Luồng Dữ Liệu
```
Application → Kafka Topics → Kafka Connect HDFS Sink → HDFS
     ↓
  Database
```

### Các Thành Phần Chính

1. **Structured Events**: `EmployeeEvent`, `FileEvent` - Định dạng dữ liệu có cấu trúc
2. **Kafka Connect Producer**: `KafkaConnectProducerService` - Gửi structured events
3. **HDFS Connector Service**: `KafkaConnectHdfsService` - Quản lý lifecycle connector
4. **Monitoring Service**: `KafkaConnectMonitoringService` - Giám sát và health check
5. **REST API**: `KafkaConnectController` - Quản lý connector qua API

## Cấu Hình

### Application Properties
```properties
# HDFS Configuration
hdfs.uri=hdfs://localhost:9000
hdfs.user=dr.who
hdfs.base.path=/employee-data

# Kafka Connect Configuration
kafka.connect.url=http://localhost:8083
kafka.connect.hdfs.connector.name=hdfs-sink-connector
kafka.connect.hdfs.topics=employee-events,file-events

# Schema Registry Configuration (for Avro)
schema.registry.url=http://localhost:8081

# HDFS Sink Configuration
kafka.connect.hdfs.flush.size=1000
kafka.connect.hdfs.rotate.interval.ms=60000
kafka.connect.hdfs.partition.duration.ms=3600000
kafka.connect.hdfs.path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
```

### Kafka Topics

- `employee-events`: Structured employee events (CREATE, UPDATE, DELETE)
- `file-events`: Structured file events (UPLOAD, DELETE, PROCESS)

## Structured Events

### EmployeeEvent
```json
{
  "eventId": "uuid",
  "eventType": "CREATE|UPDATE|DELETE",
  "employeeId": 123,
  "employeeName": "John Doe",
  "employeeEmail": "john@example.com",
  "timestamp": "2024-01-01T10:00:00",
  "source": "REST_API",
  "version": "1.0",
  "year": "2024",
  "month": "01",
  "day": "01",
  "hour": "10"
}
```

### FileEvent
```json
{
  "eventId": "uuid",
  "eventType": "UPLOAD|DELETE|PROCESS",
  "fileName": "document.pdf",
  "filePath": "/uploads/document.pdf",
  "fileSize": 1024000,
  "contentType": "application/pdf",
  "checksum": "md5hash",
  "timestamp": "2024-01-01T10:00:00",
  "uploadedBy": "user123",
  "source": "WEB_UI",
  "version": "1.0",
  "processingStatus": "PENDING"
}
```

## HDFS Partitioning

Dữ liệu được phân vùng theo thời gian:
```
/employee-data/
├── employee-events/
│   ├── year=2024/month=01/day=01/hour=10/
│   │   ├── employee-events+0+0000000000.json
│   │   └── employee-events+0+0000001000.json
│   └── year=2024/month=01/day=01/hour=11/
└── file-events/
    └── year=2024/month=01/day=01/hour=10/
        └── file-events+0+0000000000.json
```

## API Endpoints

### Monitoring & Health Check
- `GET /api/kafka-connect/health` - Lấy trạng thái health
- `POST /api/kafka-connect/health/check` - Thực hiện health check
- `GET /api/kafka-connect/connector/metrics` - Lấy metrics

### Connector Management
- `GET /api/kafka-connect/connector/status` - Trạng thái connector
- `POST /api/kafka-connect/connector/create?useAvro=false` - Tạo connector
- `DELETE /api/kafka-connect/connector` - Xóa connector
- `POST /api/kafka-connect/connector/pause` - Tạm dừng
- `POST /api/kafka-connect/connector/resume` - Tiếp tục
- `POST /api/kafka-connect/connector/restart` - Restart
- `POST /api/kafka-connect/connector/auto-restart` - Auto restart nếu cần

### Discovery
- `GET /api/kafka-connect/connectors` - Danh sách connectors
- `GET /api/kafka-connect/connector/exists` - Kiểm tra tồn tại
- `GET /api/kafka-connect/connector/config` - Lấy configuration

## Cài Đặt & Triển Khai

### 1. Cài Đặt Kafka Connect
```bash
# Download Confluent Platform hoặc Kafka Connect
wget https://packages.confluent.io/archive/7.4/confluent-7.4.0.tar.gz
tar -xzf confluent-7.4.0.tar.gz

# Download HDFS Connector
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-hdfs/versions/10.2.0/confluentinc-kafka-connect-hdfs-10.2.0.zip
```

### 2. Cấu Hình Kafka Connect
```properties
# connect-distributed.properties
bootstrap.servers=localhost:9092
group.id=connect-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.topic=connect-offsets
config.storage.topic=connect-configs
status.storage.topic=connect-status
plugin.path=/path/to/kafka-connect-hdfs
```

### 3. Khởi Động Services
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Start HDFS
start-dfs.sh

# Start Kafka Connect
bin/connect-distributed.sh config/connect-distributed.properties

# Start Schema Registry (nếu dùng Avro)
bin/schema-registry-start.sh config/schema-registry.properties
```

### 4. Khởi Động Application
```bash
mvn spring-boot:run
```

## Monitoring & Troubleshooting

### Health Check Tự Động
- Chạy mỗi 5 phút
- Kiểm tra Kafka Connect server
- Kiểm tra trạng thái connector và tasks
- Auto-restart nếu cần

### Logs Quan Trọng
- `KafkaConnectHdfsService`: Lifecycle management
- `KafkaConnectMonitoringService`: Health checks
- `KafkaConnectProducerService`: Event publishing

### Troubleshooting Common Issues

1. **Connector không khởi động**
   - Kiểm tra HDFS connection
   - Kiểm tra topic permissions
   - Xem logs trong Kafka Connect

2. **Data không xuất hiện trong HDFS**
   - Kiểm tra flush.size và rotate.interval.ms
   - Kiểm tra HDFS permissions
   - Kiểm tra partition path format

3. **Performance Issues**
   - Tăng số lượng tasks
   - Tối ưu flush.size
   - Sử dụng Avro thay vì JSON

## Tính Năng Nâng Cao

### Avro Support
```java
// Tạo connector với Avro format
POST /api/kafka-connect/connector/create?useAvro=true
```

### Custom Event Types
```java
// Gửi custom events
kafkaConnectProducerService.sendCustomEvent("custom-topic", "key", customEvent);
```

### Batch Processing
- Connector tự động batch messages theo flush.size
- Time-based rotation theo rotate.interval.ms
- Partition theo thời gian cho performance tốt hơn

## Lợi Ích So Với Cách Cũ

1. **Tự Động Hóa**: Không cần manual HDFS operations
2. **Scalability**: Kafka Connect xử lý backpressure và retry
3. **Monitoring**: Built-in health checks và metrics
4. **Partitioning**: Tự động partition theo thời gian
5. **Format Support**: JSON và Avro
6. **Fault Tolerance**: Auto-restart và error handling
7. **Performance**: Batch processing và parallel tasks

## Migration Path

1. **Phase 1**: Dual write (cả legacy và Kafka Connect)
2. **Phase 2**: Verify data consistency
3. **Phase 3**: Switch to Kafka Connect only
4. **Phase 4**: Remove legacy HDFS code

Hiện tại đang ở Phase 1 - cả hai cách đều hoạt động song song.
