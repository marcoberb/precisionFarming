package com.mammut.precisionfarming.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

@Component
@Slf4j
public class SparkStreamingListener implements Serializable {

    private static final Pattern SEPARATOR = Pattern.compile(";");

    @Value("${kafka.hostname}")
    private String hostname;
    @Value("${kafka.groupid}")
    private String groupid;
    @Value("${kafka.channels.in}")
    private String channels;


    public void listen() throws InterruptedException {

        // Spark Streaming configuration
        SparkConf sparkConf = new SparkConf().setAppName("server").setMaster("local");
        log.info("SparkConf set for {}", sparkConf);
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        log.info("JavaStreamingContext set {}", jsc);

        // Kafka configuration
        List<String> topicsList = Arrays.asList(channels.split(","));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Connect to Kafka meteo topic
        HashSet<String> meteoTopic = new HashSet<String>(Collections.singleton(topicsList.get(0)));
        JavaInputDStream<ConsumerRecord<String, String>> meteoMessages =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(meteoTopic, kafkaParams)
                );

        // Connect to Kafka suolo topic
        HashSet<String> suoloTopic = new HashSet<String>(Collections.singleton(topicsList.get(1)));
        JavaInputDStream<ConsumerRecord<String, String>> suoloMessages =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(suoloTopic, kafkaParams)
                );

        // Receive meteo data
        JavaDStream<String> meteoLines = meteoMessages.map(ConsumerRecord::value);
        meteoLines.print();

        // Receive suolo data
        JavaDStream<String> suoloLines = suoloMessages.map(ConsumerRecord::value);
        suoloLines.print();

        // Split meteo fields
        JavaDStream<String> meteoFields = meteoLines.flatMap(line -> Arrays.asList(SEPARATOR.split(line)).iterator());
        meteoFields.print();

        // Split suolo fields
        JavaDStream<String> suoloFields = suoloLines.flatMap(line -> Arrays.asList(SEPARATOR.split(line)).iterator());
        suoloLines.print();

        jsc.start();
        jsc.awaitTermination();

    }

}
