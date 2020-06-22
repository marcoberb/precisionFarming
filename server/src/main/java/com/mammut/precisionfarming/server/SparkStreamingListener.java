package com.mammut.precisionfarming.server;

import com.mongodb.spark.MongoSpark;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;
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
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("server")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precisionFarming.raw")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27011/precisionFarming.raw")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

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
        //meteoLines.print();

        // Receive suolo data
        JavaDStream<String> suoloLines = suoloMessages.map(ConsumerRecord::value);
        //suoloLines.print();

        // Split meteo fields
        // JavaDStream<String> meteoFields = meteoLines.flatMap(line -> Arrays.asList(SEPARATOR.split(line)).iterator());
        meteoLines.foreachRDD(rdd -> {
            JavaRDD<Document> meteoDocuments = rdd.map(line -> {
                List<String> fields = Arrays.asList(SEPARATOR.split(line));
                log.info("Linea RDD: " + Arrays.toString(fields.toArray()));

                String id = fields.get(0);
                String timestamp = fields.get(1);
                String temperature = fields.get(2);
                String humidity = fields.get(5);
                String rain = fields.get(9);
                String wind_dir = fields.get(10);
                String wind_speed = fields.get(12);
                String pressure = fields.get(16);
                String radiation = fields.get(19);

                Document document = new Document("_id", id);
                document.put("timestamp", timestamp);
                document.put("temperature", temperature);
                document.put("humidity", humidity);
                document.put("rain", rain);
                document.put("wind_dir", wind_dir);
                document.put("wind_speed", wind_speed);
                document.put("pressure", pressure);
                document.put("radiation", radiation);

                return document;
            });

            MongoSpark.save(meteoDocuments);
        });
        //meteoLines.print();
        //meteoFields.print();

        // Split suolo fields
        //JavaDStream<String> suoloFields = suoloLines.flatMap(line -> Arrays.asList(SEPARATOR.split(line)).iterator());
        //suoloLines.print();

        jsc.start();
        jsc.awaitTermination();
    }

}
