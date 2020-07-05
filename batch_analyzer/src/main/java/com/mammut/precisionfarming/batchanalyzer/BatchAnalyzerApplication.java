package com.mammut.precisionfarming.batchanalyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ApplicationContext;

@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@Slf4j
public class BatchAnalyzerApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(BatchAnalyzerApplication.class, args);
        SparkBatchAnalyser sparkBatchAnalyser = applicationContext.getBean(SparkBatchAnalyser.class);

        //sparkBatchAnalyser.execAll();

        new Thread(() -> {
            while(true){
                log.info("last3DaysTrend");
                sparkBatchAnalyser.last3DaysTrend();
                try {
                    Thread.sleep(600000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            while(true){
                log.info("daysToWater");
                sparkBatchAnalyser.daysToWater();
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            while(true){
                log.info("top10HotDays");
                sparkBatchAnalyser.top10HotDays();
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            while(true){
                log.info("top10ColdDays");
                sparkBatchAnalyser.top10ColdDays();
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            while(true){
                log.info("top10HumidDays");
                sparkBatchAnalyser.top10HumidDays();
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

}
