package com.silence.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestParam("param") String jsonLog){
        log.info(jsonLog);
        kafkaTemplate.send("ods_base_log",jsonLog);
        return "success";
    }
}
