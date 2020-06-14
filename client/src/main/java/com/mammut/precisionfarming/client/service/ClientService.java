package com.mammut.precisionfarming.client.service;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;

@Service
@Slf4j
public class ClientService {

    @Autowired
    private MsgPublisherPort msgPublisherPort;

    public void generateData(){
        String csvFile = "client/meteo_2020.csv";

        CSVReader reader = null;
        try {
            CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build();
            reader = new CSVReaderBuilder(new FileReader(csvFile)).withCSVParser(csvParser).build();

            String[] line;
            while ((line = reader.readNext()) != null) {
                String output = "id_dato: " + line[0] + ", data_ora: " + line[1] + " , temp1_media: " + line[2];
                System.out.println(output);
                log.info("generate");
                msgPublisherPort.publishToServer(output);

                Thread.sleep(1000);
            }
        } catch (IOException | CsvValidationException | InterruptedException e) {
            e.printStackTrace();
        }
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
