package com.tarento.nsdc.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tarento.nsdc.entity.CourseEntity;
import com.tarento.nsdc.entity.CourseEntityV2;
import com.tarento.nsdc.repo.ICourseRepo;
import com.tarento.nsdc.repo.ICourseRepoV2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;



@Component
public class CourseConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CourseConsumer.class);

    @Autowired
    private ICourseRepo courseRepo;

    @Autowired
    private ICourseRepoV2 courseRepoV2;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "course", groupId = "course-consumer")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        logger.info("Course Dumping::processMessage: Received event to initiate course dumping...");
        String courseJson = consumerRecord.value();
        try {
            if (!StringUtils.isEmpty(courseJson)) {
                logger.info("Received message:: " + courseJson);
                CourseEntity course = objectMapper.readValue(courseJson, CourseEntity.class);
                courseRepo.save(course);
            }
        } catch (Exception e) {
            logger.error("Issue while storing data in db::");
        }
    }

    @KafkaListener(topics = "courseV2", groupId = "course-consumerV2")
    public void consumeV2(ConsumerRecord<String, String> consumerRecord) {
        logger.info("Course Dumping::processMessage: Received event to initiate course dumping...");
        String courseJson = consumerRecord.value();
        try {
            if (!StringUtils.isEmpty(courseJson)) {
                logger.info("Received message:: " + courseJson);
                CourseEntityV2 course = objectMapper.readValue(courseJson, CourseEntityV2.class);
                courseRepoV2.save(course);
            }
        } catch (Exception e) {
            logger.error("Issue while storing data in db::");
        }
    }
}
