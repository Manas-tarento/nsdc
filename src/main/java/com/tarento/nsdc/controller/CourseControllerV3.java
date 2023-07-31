package com.tarento.nsdc.controller;

import com.tarento.nsdc.service.ICourseServiceV3;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/course/v3")
public class CourseControllerV3 {

    @Autowired
    private ICourseServiceV3 courseServiceV3;

    @GetMapping("/read")
    public ResponseEntity<?> getCourse(@RequestParam String fileName) {
        courseServiceV3.processIncomingFile(fileName);
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }
}
