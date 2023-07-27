package com.tarento.nsdc.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tarento.nsdc.entity.CourseEntity;
import com.tarento.nsdc.entity.CourseEntityV2;
import com.tarento.nsdc.repo.ICourseRepo;
import com.tarento.nsdc.repo.ICourseRepoV2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Random;
import java.util.UUID;

@Component
public class CourseConsumer {

    @Autowired
    private ICourseRepo courseRepo;

    @Autowired
    private ICourseRepoV2 courseRepoV2;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "course", groupId = "course-consumer")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("Course Dumping::processMessage: Received event to initiate course dumping...");
        String courseJson = consumerRecord.value();
        try {
            if (!StringUtils.isEmpty(courseJson)) {
                System.out.println("Received message:: " + courseJson);
                CourseEntity course = objectMapper.readValue(courseJson, CourseEntity.class);
                courseRepo.save(course);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics = "courseV2", groupId = "course-consumerV2")
    public void consumeV2(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("Course Dumping::processMessage: Received event to initiate course dumping...");
        String courseJson = consumerRecord.value();
        try {
            if (!StringUtils.isEmpty(courseJson)) {
                System.out.println("Received message:: " + courseJson);
                CourseEntityV2 course = objectMapper.readValue(courseJson, CourseEntityV2.class);
                courseRepoV2.save(course);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
