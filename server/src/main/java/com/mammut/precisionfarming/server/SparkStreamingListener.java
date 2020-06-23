package com.mammut.precisionfarming.server;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
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
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(javaSparkContext, Durations.seconds(1));

        // Kafka configuration
        List<String> topicsList = Arrays.asList(channels.split(","));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<JavaDStream> streams = this.getDStreams(jsc, kafkaParams, topicsList);

        this.saveRawData(streams, topicsList);

        jsc.start();
        jsc.awaitTermination();
    }

    private List<JavaDStream> getDStreams(JavaStreamingContext jsc, Map<String, Object> kafkaParams, List<String> topicsList) {
        // Connect to Kafka topic
        HashSet<String> topicSet = new HashSet<String>(topicsList);
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicSet, kafkaParams)
                );

        List<JavaDStream> dStreams = new ArrayList<>();
        for (String topic: topicsList) {
            // Get data relative to this topic
            JavaDStream<ConsumerRecord<String, String>> topicMessages = messages.filter(record -> record.topic().equals(topic));

            // Receive data
            JavaDStream<String> lines = topicMessages.map(ConsumerRecord::value);

            // Split fields
            JavaDStream<List<String>> fields = lines.map(line -> Arrays.asList(SEPARATOR.split(line)));

            dStreams.add(fields);
        }

        return dStreams;
    }

    private void saveRawData(List<JavaDStream> streams, List<String> topicsList) {
        for (int i=0; i<topicsList.size(); i++) {
            JavaDStream<List<String>> stream = streams.get(i);
            String topic = topicsList.get(i);

            Map<String, Integer> fieldToIndex = new HashMap<>();
            if (topic.contains("meteo")) {
                // Get mapping from meteo field names to index in the row string
                getMeteoFieldToIndex(fieldToIndex);
            }
            else if (topic.contains("suolo")) {
                // Get mapping from suolo field names to index in the row string
                getSuoloFieldToIndex(fieldToIndex);
            }

            // Set mongo write options
            Map<String, String> mongoOptions = new HashMap<>();
            mongoOptions.put("uri", "mongodb://localhost:27011/");
            mongoOptions.put("database", "precisionFarmingRaw");
            mongoOptions.put("collection", topic);
            WriteConfig writeConfig = WriteConfig.create(mongoOptions);

            // Save raw data to mongo
            stream.foreachRDD(rdd -> {
                JavaRDD<Document> documents = rdd.map(fields -> {
                    log.info("Linea RDD: " + Arrays.toString(fields.toArray()));

                    Document document = this.getDocument(fieldToIndex, fields);

                    return document;
                });

                MongoSpark.save(documents, writeConfig);
            });
        }
    }

    private void getMeteoFieldToIndex(Map<String, Integer> fieldToIndex) {
        fieldToIndex.put("_id", 0);
        fieldToIndex.put("timestamp", 1);
        fieldToIndex.put("temperature", 2);
        fieldToIndex.put("humidity", 5);
        fieldToIndex.put("rain", 9);
        fieldToIndex.put("wind_dir", 10);
        fieldToIndex.put("wind_speed", 12);
        fieldToIndex.put("pressure", 16);
        fieldToIndex.put("radiation", 19);
    }


    private void getSuoloFieldToIndex(Map<String, Integer> fieldToIndex) {
        fieldToIndex.put("_id", 0);
        fieldToIndex.put("timestamp", 3);
        fieldToIndex.put("water_0", 4);
        fieldToIndex.put("temperature_0", 6);
        fieldToIndex.put("water_1", 8);
        fieldToIndex.put("temperature_1", 10);
    }

    private Document getDocument(Map<String, Integer> fieldToIndex, List<String> fields) {
        Document document = new Document();
        for (Map.Entry<String, Integer> field: fieldToIndex.entrySet()) {
            document.put(field.getKey(), fields.get(field.getValue()));
        }

        return document;
    }

}
