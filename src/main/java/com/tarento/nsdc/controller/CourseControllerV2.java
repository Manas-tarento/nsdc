package com.tarento.nsdc.controller;

import com.tarento.nsdc.service.ICourseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/course/v2")
public class CourseControllerV2 {

    @Autowired
    private ICourseService courseService;

    @GetMapping("/read")
    public ResponseEntity<?> getCourse(@RequestParam String fileName) {
        courseService.processIncomingFile(fileName);
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }
}
