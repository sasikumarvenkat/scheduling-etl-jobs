package com.example.automation.schedulingetljobs.taks;

import com.example.automation.schedulingetljobs.constants.SchedulerConstants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

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
    public void scheduleSparkPiApp() {
        LOGGER.info("Info: Scheduling the Apache Spark ETL workloads");
        Process spark;
        try {
            spark = new SparkLauncher().setAppResource("/usr/local/spark/examples/jars/spark-examples_2.11-2.2.1.jar")
                    .setMainClass("org.apache.spark.examples.SparkPi").setMaster("local").setAppName("Spark Pi")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "1g").addAppArgs("100").launch();
            LOGGER.info("Spark Pi Started.");
            InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
            Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
            inputThread.start();

            InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
            Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
            errorThread.start();

            System.out.println("Waiting for finish...");
            int exitCode = spark.waitFor();
            System.out.println("Finished! Exit code:" + exitCode);
            LOGGER.info("Spark Job Is Alive: " + spark.isAlive());

        } catch (IOException e) {
            LOGGER.info(e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOGGER.info(e);
            e.printStackTrace();
        }

    }

    @Scheduled(cron = SchedulerConstants.CRON_EXPRESSION_EVERY_DAY)
    public void scheduleSparkWordCount() {
        LOGGER.info("Info: Scheduling the Apache Spark ETL workloads - Word Count");
        Process spark;
        try {
            spark = new SparkLauncher().setAppResource("sparktest_2.11-0.1.jar")
                    .setMainClass("com.example.test").setMaster("local").setAppName("Spark Word Count")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "1g").addAppArgs("/home/sasi/Documents/spark/input.txt").
                            addAppArgs("/home/sasi/Documents/sasi/output1").launch();
            LOGGER.info("Spark Pi Started.");
            InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
            Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
            inputThread.start();

            InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
            Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
            errorThread.start();

            System.out.println("Waiting for finish...");
            int exitCode = spark.waitFor();
            System.out.println("Finished! Exit code:" + exitCode);
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
