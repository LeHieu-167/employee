//package com.employee.producer;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.support.SendResult;
//import org.springframework.stereotype.Service;
//
//import java.util.concurrent.CompletableFuture;
//
//@Service
//public class EmployeeProducer {
//    private static final Logger logger = LoggerFactory.getLogger(EmployeeProducer.class);
//    private static final String TOPIC = "employee-topic";
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//    public void sendMessage(String key, String value) {
//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, key, value);
//        future.thenAccept(result -> {
//            logger.info(String.format("\n\n Produced event to topic %s: key = %-10s value = %s \n\n",
//                    TOPIC, key, value));
//        }).exceptionally(ex -> {
//            ex.printStackTrace();
//            return null;
//        });
//    }
//}
