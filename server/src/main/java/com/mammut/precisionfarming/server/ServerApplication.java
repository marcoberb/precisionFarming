package com.mammut.precisionfarming.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class ServerApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(ServerApplication.class, args);
        SparkStreamingListener sparkStreamingListener = applicationContext.getBean(SparkStreamingListener.class);
        sparkStreamingListener.listen();
    }

}
