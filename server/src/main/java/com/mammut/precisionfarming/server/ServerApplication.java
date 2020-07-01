package com.mammut.precisionfarming.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ApplicationContext;

@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class ServerApplication {

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext applicationContext = SpringApplication.run(ServerApplication.class, args);
//        SparkStreamingListener sparkStreamingListener = applicationContext.getBean(SparkStreamingListener.class);
//        sparkStreamingListener.listen();
        SparkBatchAnalyser sparkBatchAnalyser = applicationContext.getBean(SparkBatchAnalyser.class);
        sparkBatchAnalyser.exec();
    }

}
