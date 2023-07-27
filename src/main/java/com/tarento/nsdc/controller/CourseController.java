package com.tarento.nsdc.controller;

import com.tarento.nsdc.service.ICourseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/course/v1")
public class CourseController {

    @Autowired
    private ICourseService courseService;

    @GetMapping("/read")
    public ResponseEntity<?> getCourse(@RequestParam String fileName) {
        courseService.processIncomingFile(fileName);
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }
}
