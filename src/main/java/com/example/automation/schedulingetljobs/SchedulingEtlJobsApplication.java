package com.example.automation.schedulingetljobs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SchedulingEtlJobsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulingEtlJobsApplication.class, args);
    }
}
