package com.mammut.precisionfarming.client.service;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;

@Service
@Slf4j
public class ClientService {

    @Autowired
    private MsgPublisherPort msgPublisherPort;

    public void generateData(String source, int sec){
        String csvFile = "data/" + source + ".csv";

        CSVReader reader = null;
        try {
            CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build();
            reader = new CSVReaderBuilder(new FileReader(csvFile)).withCSVParser(csvParser).withSkipLines(1).build();

            String[] line;
            while ((line = reader.readNext()) != null) {
                String output = String.join(";", line);

                log.info("Sending {}", output);
                msgPublisherPort.publishToServer(source, output);

                Thread.sleep(sec*1000);
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
