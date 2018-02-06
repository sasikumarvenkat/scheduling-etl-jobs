package com.example.automation.schedulingetljobs.taks;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.automation.schedulingetljobs.constants.SchedulerConstants;

@Component
public class ScheduledTasks {

	private static final Logger LOGGER = LogManager.getLogger(ScheduledTasks.class);

	@Scheduled(fixedDelay = 1000)
	public void testScheduled() {
		LOGGER.info("Info: Test Method Scheduled on Fixed Delay");
	}

	@Scheduled(cron = SchedulerConstants.CRON_EXPRESSION_EVERY_MINUTE)
	public void sampleCronScheduled() {
		LOGGER.info("Info: Sample Test Method Scheduled based on CRON Job");
	}

	@Scheduled(fixedDelay = 2000)
	public void scheduleSparkETLJob() {
		LOGGER.info("Info: Scheduling the Apache Spark ETL workloads");
		Process spark;
		try {
			spark = new SparkLauncher().setAppResource("/usr/local/spark/examples/jar/spark*.jar")
					.setMainClass("org.apache.spark.examples.SparkPi").setMaster("local").setAppName("Spark Pi")
					.addSparkArg("100").setConf(SparkLauncher.DRIVER_MEMORY, "2g").launch();

			spark.waitFor();
			LOGGER.info("Spark Job Is Alive: " + spark.isAlive());
		} catch (IOException e) {
			LOGGER.info(e);
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOGGER.info(e);
			e.printStackTrace();
		}

	}

}
