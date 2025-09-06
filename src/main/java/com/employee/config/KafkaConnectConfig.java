package com.employee.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConnectConfig {

    @Value("${kafka.connect.url}")
    private String kafkaConnectUrl;

    @Value("${kafka.connect.hdfs.connector.name}")
    private String connectorName;

    @Value("${kafka.connect.hdfs.topics}")
    private String topics;

    @Value("${hdfs.uri}")
    private String hdfsUri;

    @Value("${hdfs.base.path}")
    private String hdfsBasePath;

    @Value("${kafka.connect.hdfs.flush.size}")
    private int flushSize;

    @Value("${kafka.connect.hdfs.rotate.interval.ms}")
    private long rotateIntervalMs;

    @Value("${kafka.connect.hdfs.partition.duration.ms}")
    private long partitionDurationMs;

    @Value("${kafka.connect.hdfs.path.format}")
    private String pathFormat;

    // WebClient bean will be created when WebFlux is available

    /**
     * Tạo cấu hình cho HDFS Sink Connector
     * @return Map chứa cấu hình connector
     */
    public Map<String, Object> createHdfsSinkConnectorConfig() {
        Map<String, Object> config = new HashMap<>();
        
        // Connector configuration
        config.put("name", connectorName);
        config.put("connector.class", "io.confluent.connect.hdfs3.Hdfs3SinkConnector");
        config.put("tasks.max", "1");
        
        // Topics configuration
        config.put("topics", topics);
        config.put("topics.dir", hdfsBasePath);
        
        // HDFS configuration
        config.put("hdfs.url", hdfsUri);
        config.put("hadoop.conf.dir", "/etc/hadoop/conf");
        
        // Data format configuration
        config.put("format.class", "io.confluent.connect.hdfs3.json.JsonFormat");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter.schemas.enable", "false");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        
        // Partitioning configuration
        config.put("partitioner.class", "io.confluent.connect.hdfs3.partitioner.TimeBasedPartitioner");
        config.put("partition.duration.ms", partitionDurationMs);
        config.put("path.format", pathFormat);
        config.put("locale", "US");
        config.put("timezone", "Asia/Ho_Chi_Minh");
        
        // File rotation configuration
        config.put("flush.size", flushSize);
        config.put("rotate.interval.ms", rotateIntervalMs);
        
        // Schema configuration
        config.put("schema.compatibility", "NONE");
        
        return config;
    }

    /**
     * Tạo cấu hình cho HDFS Sink Connector với Avro format
     * @return Map chứa cấu hình connector với Avro
     */
    public Map<String, Object> createAvroHdfsSinkConnectorConfig() {
        Map<String, Object> config = createHdfsSinkConnectorConfig();
        
        // Override format for Avro
        config.put("format.class", "io.confluent.connect.hdfs3.avro.AvroFormat");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", "${schema.registry.url}");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", "${schema.registry.url}");
        
        return config;
    }

    // Getters for configuration values
    public String getKafkaConnectUrl() {
        return kafkaConnectUrl;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public String getTopics() {
        return topics;
    }

    public String getHdfsUri() {
        return hdfsUri;
    }

    public String getHdfsBasePath() {
        return hdfsBasePath;
    }
}
