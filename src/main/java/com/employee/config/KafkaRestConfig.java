package com.employee.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

@Configuration
public class KafkaRestConfig {

    @Value("${kafka.rest.proxy.url:http://localhost:8082}")
    private String kafkaRestProxyBaseUrl;

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public HttpHeaders kafkaRestHeaders() {
        HttpHeaders headers = new HttpHeaders();
        // Kafka REST Proxy mong đợi các media cụ thể JSON produce API
        headers.setContentType(MediaType.valueOf("application/vnd.kafka.json.v2+json"));
        headers.setAccept(java.util.Arrays.asList(
                MediaType.valueOf("application/vnd.kafka.v2+json"),
                MediaType.APPLICATION_JSON
        ));
        return headers;
    }

    public String getConsumerUrl(String groupId, String consumerId) {
        return kafkaRestProxyBaseUrl + "/consumers/" + groupId + "/instances/" + consumerId + "/records";
    }

    public String getProducerUrl(String topic) {
        return kafkaRestProxyBaseUrl + "/topics/" + topic;
    }
}