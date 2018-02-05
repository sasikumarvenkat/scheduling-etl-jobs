package com.example.automation.schedulingetljobs.taks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.automation.schedulingetljobs.constants.SchedulerConstants;

@Component
public class ScheduledTasks {

	private static final Logger LOGGER = LogManager.getLogger(ScheduledTasks.class);

	@Scheduled(fixedDelay = 1000)
	public void testScheduled() {
		LOGGER.info("Test Method Scheduled on Fixed Delay");
	}

	@Scheduled(cron = SchedulerConstants.CRON_EXPRESSION_EVERY_MINUTE)
	public void sampleCronScheduled() {
		LOGGER.info("Sample Test Method Scheduled based on CRON Job");
	}

}
