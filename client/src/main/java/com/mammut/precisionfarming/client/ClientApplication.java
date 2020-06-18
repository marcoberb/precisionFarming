package com.mammut.precisionfarming.client;

import com.mammut.precisionfarming.client.service.ClientService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
public class ClientApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(ClientApplication.class, args);
        ClientService clientService = applicationContext.getBean(ClientService.class);

        new Thread(() -> {
            clientService.generateData("meteo_2020");
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_1_2020");
        }).start();
    }
}
