package com.tarento.nsdc.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tarento.nsdc.entity.CourseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    public void push(String topic, Object courseData) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String message = mapper.writeValueAsString(courseData);
            kafkaTemplate.send(topic, message);
        } catch (Exception e) {
            System.out.println("Issue with sending message to kafka topic");
        }
    }
}
