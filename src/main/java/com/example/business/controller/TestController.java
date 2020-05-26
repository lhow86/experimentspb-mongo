package com.example.business.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("test")
public class TestController {

    @RequestMapping(value = "test", method = RequestMethod.GET)
    public void test() {
        log.info("test");
    }

}
