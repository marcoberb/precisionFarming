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
            clientService.generateData("meteo_2020_gennaio_marzo", 1);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_1_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_2_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_3_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_4_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_5_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_6_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_7_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_8_2020", 3);
        }).start();

        new Thread(() -> {
            clientService.generateData("suolo_9_2020", 3);
        }).start();
    }
}
