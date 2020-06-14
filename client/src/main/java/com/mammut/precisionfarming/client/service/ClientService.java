package com.mammut.precisionfarming.client.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ClientService {

    @Autowired
    private MsgPublisherPort msgPublisherPort;

    @Scheduled(fixedDelay = 1000)
    public void generateData(){
        log.info("generate");
        msgPublisherPort.publishToServer("prova");
    }
}


/*    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet(){
        String[] allBeanNames = applicationContext.getBeanDefinitionNames();
        for(String beanName : allBeanNames) {
            System.out.println(beanName);
        }
    }*/
