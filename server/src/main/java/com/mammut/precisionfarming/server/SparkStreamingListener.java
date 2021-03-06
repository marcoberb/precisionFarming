package com.mammut.precisionfarming.server;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

@Component
@Slf4j
public class SparkStreamingListener implements Serializable {

    private final Pattern SEPARATOR = Pattern.compile(";");

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
        JavaStreamingContext jsc = new JavaStreamingContext(javaSparkContext, Durations.seconds(12));

        // Kafka configuration
        List<String> topicsList = Arrays.asList(channels.split(","));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<JavaDStream> streams = this.getDStreams(jsc, kafkaParams, topicsList);

        this.saveRawData(streams, topicsList);

        this.sicknessAlert(streams, topicsList);

        this.dontIrrigate(streams, topicsList);

        this.waterTemperatureRatio(streams, topicsList);

        this.combinedSicknessAlert(streams, topicsList);

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

    private void combinedSicknessAlert(List<JavaDStream> streams, List<String> topicsList) {
        final float thresholdTemperature = -1;
        final double thresholdWaterContent = 0.5;
        for (int i=0; i<topicsList.size(); i++) {
            JavaDStream<List<String>> stream = streams.get(i);
            String topic = topicsList.get(i);

            Map<String, Integer> fieldToIndex = new HashMap<>();
            if(topic.contains("suolo")) {
                // Get mapping from suolo field names to index in the row string
                this.getSuoloFieldToIndex(fieldToIndex);

                JavaDStream<Tuple3<String, Double, Double>> waterTempAvg = stream.mapToPair(fields -> {
                    String timestamp = fields.get(fieldToIndex.get("timestamp"));
                    String hour = timestamp.substring(0, timestamp.length() - 6);
                    Double waterContent0 = Double.valueOf(fields.get(fieldToIndex.get("water_0")));
                    Double waterContent1 = Double.valueOf(fields.get(fieldToIndex.get("water_1")));
                    Double temperature0 = Double.valueOf(fields.get(fieldToIndex.get("temperature_0")));
                    Double temperature1 = Double.valueOf(fields.get(fieldToIndex.get("temperature_1")));

                    return new Tuple2<>(hour, new Tuple3<>((waterContent0 + waterContent1) / 2, (temperature0 + temperature1) / 2, 1));
                })
                .reduceByKey((left, right) -> {
                    Double waterContent = left._1() + right._1();
                    Double temperature = left._2() + right._2();
                    Integer count = left._3() + right._3();

                    return new Tuple3<>(waterContent, temperature, count);
                })
                .map(pair -> {
                    String hour = pair._1();
                    Double avgWaterContent = pair._2()._1() / pair._2()._3();
                    Double avgTemperature = pair._2()._2() / pair._2()._3();

                    return new Tuple3<>(hour, avgWaterContent, avgTemperature);
                })
                .filter(triple -> (triple._3() > thresholdTemperature) && (triple._2() < thresholdWaterContent));

                // Set mongo write options
                Map<String, String> mongoOptions = new HashMap<>();
                mongoOptions.put("uri", "mongodb://localhost:27011/");
                mongoOptions.put("database", "precisionFarmingStreamingAnalysis");
                mongoOptions.put("collection", "sicknessAlert_" + topic);
                WriteConfig writeConfig = WriteConfig.create(mongoOptions);

                waterTempAvg.foreachRDD(rdd -> {
                    JavaRDD<Document> documents = rdd.map(fields -> {
                        Document document = new Document();

                        document.put("hour", fields._1());
                        document.put("lowWaterContent", fields._2());
                        document.put("highTemperature", fields._3());

                        return document;
                    });

                    MongoSpark.save(documents, writeConfig);
                });
            }
        }
    }

    private void waterTemperatureRatio(List<JavaDStream> streams, List<String> topicsList) {
        for (int i=0; i<topicsList.size(); i++) {
            JavaDStream<List<String>> stream = streams.get(i);
            String topic = topicsList.get(i);

            Map<String, Integer> fieldToIndex = new HashMap<>();
            if(topic.contains("suolo")) {
                // Get mapping from suolo field names to index in the row string
                this.getSuoloFieldToIndex(fieldToIndex);

                JavaDStream<Tuple2<String, Double>> waterTempAvg = stream.mapToPair(fields -> {
                    String timestamp = fields.get(fieldToIndex.get("timestamp"));
                    String hour = timestamp.substring(0, timestamp.length() - 6);
                    Double waterContent0 = Double.valueOf(fields.get(fieldToIndex.get("water_0")));
                    Double waterContent1 = Double.valueOf(fields.get(fieldToIndex.get("water_1")));
                    Double temperature0 = Double.valueOf(fields.get(fieldToIndex.get("temperature_0")));
                    Double temperature1 = Double.valueOf(fields.get(fieldToIndex.get("temperature_1")));

                    return new Tuple2<>(hour, new Tuple3<>((waterContent0 + waterContent1) / 2, (temperature0 + temperature1) / 2, 1));
                })
                .reduceByKey((left, right) -> {
                    Double waterContent = left._1() + right._1();
                    Double temperature = left._2() + right._2();
                    Integer count = left._3() + right._3();

                    return new Tuple3<>(waterContent, temperature, count);
                })
                .map(pair -> {
                    String hour = pair._1();
                    Double avgWaterContent = pair._2()._1() / pair._2()._3();
                    Double avgTemperature = pair._2()._2() / pair._2()._3();

                    return new Tuple2<>(hour, avgWaterContent / avgTemperature);
                });

                // Set mongo write options
                Map<String, String> mongoOptions = new HashMap<>();
                mongoOptions.put("uri", "mongodb://localhost:27011/");
                mongoOptions.put("database", "precisionFarmingStreamingAnalysis");
                mongoOptions.put("collection", "waterTemperatureRatio_" + topic);
                WriteConfig writeConfig = WriteConfig.create(mongoOptions);

                waterTempAvg.foreachRDD(rdd -> {
                    JavaRDD<Document> documents = rdd.map(fields -> {
                        Document document = new Document();

                        document.put("hour", fields._1());
                        document.put("waterTemperatureRatio", fields._2());

                        return document;
                    });

                    MongoSpark.save(documents, writeConfig);
                });
            }
        }
    }

    private void dontIrrigate(List<JavaDStream> streams, List<String> topicsList) {
        final float thresholdRain = -1;
        for (int i=0; i<topicsList.size(); i++) {
            JavaDStream<List<String>> stream = streams.get(i);
            String topic = topicsList.get(i);

            Map<String, Integer> fieldToIndex = new HashMap<>();
            if(topic.contains("meteo")) {
                // Get mapping from meteo field names to index in the row string
                this.getMeteoFieldToIndex(fieldToIndex);

                JavaDStream<Tuple2<String, Float>> rainAvg = stream.mapToPair(fields -> {
                    String timestamp = fields.get(fieldToIndex.get("data_ora"));
                    String hour = timestamp.substring(0, timestamp.length() - 6);
                    Float rain = Float.valueOf(fields.get(fieldToIndex.get("pioggia_mm")));

                    return new Tuple2<>(hour, new Tuple2<>(rain, 1));
                })
                .reduceByKey((left, right) -> {
                    Float rain = left._1() + right._1();
                    Integer count = left._2() + right._2();

                    return new Tuple2<>(rain, count);
                })
                .map(pair -> {
                    String hour = pair._1();
                    Float avgRain = pair._2()._1() / pair._2()._2();

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
                    Date firstHour = dateFormat.parse(hour);
                    Date lastHour = Date.from(firstHour.toInstant().plus(Duration.ofHours(12)));

                    String interval = firstHour.toString() + " -> " + lastHour.toString();

                    return new Tuple2<>(interval, avgRain);
                })
                .filter(pair -> pair._2() > thresholdRain);

                // Set mongo write options
                Map<String, String> mongoOptions = new HashMap<>();
                mongoOptions.put("uri", "mongodb://localhost:27011/");
                mongoOptions.put("database", "precisionFarmingStreamingAnalysis");
                mongoOptions.put("collection", "dontIrrigate");
                WriteConfig writeConfig = WriteConfig.create(mongoOptions);

                rainAvg.foreachRDD(rdd -> {
                    JavaRDD<Document> documents = rdd.map(fields -> {
                        Document document = new Document();

                        document.put("interval", fields._1());
                        document.put("rain", fields._2());

                        return document;
                    });

                    MongoSpark.save(documents, writeConfig);
                });
            }
        }
    }

    private void sicknessAlert(List<JavaDStream> streams, List<String> topicsList) {
        final float thresholdTemperature = -1;
        final double thresholdWaterContent = 0.249;
        for (int i=0; i<topicsList.size(); i++) {
            JavaDStream<List<String>> stream = streams.get(i);
            String topic = topicsList.get(i);

            Map<String, Integer> fieldToIndex = new HashMap<>();
            if(topic.contains("meteo")) {
                // Get mapping from meteo field names to index in the row string
                this.getMeteoFieldToIndex(fieldToIndex);

                JavaDStream<Tuple2<String, Float>> tempAvg = stream.mapToPair(fields -> {
                    String timestamp = fields.get(fieldToIndex.get("data_ora"));
                    String hour = timestamp.substring(0, timestamp.length() - 6);
                    Float temperature = Float.valueOf(fields.get(fieldToIndex.get("temp1_media")));

                    return new Tuple2<>(hour, new Tuple2<>(temperature, 1));
                })
                .reduceByKey((left, right) -> {
                    Float temperature = left._1() + right._1();
                    Integer count = left._2() + right._2();

                    return new Tuple2<>(temperature, count);
                })
                .map(pair -> {
                    String hour = pair._1();
                    Float avgTemperature = pair._2()._1() / pair._2()._2();

                    return new Tuple2<>(hour, avgTemperature);
                })
                .filter(pair -> pair._2() > thresholdTemperature);

                // Set mongo write options
                Map<String, String> mongoOptions = new HashMap<>();
                mongoOptions.put("uri", "mongodb://localhost:27011/");
                mongoOptions.put("database", "precisionFarmingStreamingAnalysis");
                mongoOptions.put("collection", "temperatureAlert");
                WriteConfig writeConfig = WriteConfig.create(mongoOptions);

                tempAvg.foreachRDD(rdd -> {
                    JavaRDD<Document> documents = rdd.map(fields -> {
                        Document document = new Document();

                        document.put("hour", fields._1());
                        document.put("highTemperature", fields._2());

                        return document;
                    });

                    MongoSpark.save(documents, writeConfig);
                });
            }
            else if(topic.contains("suolo")) {
                // Get mapping from suolo field names to index in the row string
                this.getSuoloFieldToIndex(fieldToIndex);

                JavaDStream<Tuple2<String, Double>> tempAvg = stream.mapToPair(fields -> {
                    String timestamp = fields.get(fieldToIndex.get("timestamp"));
                    String hour = timestamp.substring(0, timestamp.length() - 6);
                    Double waterContent0 = Double.valueOf(fields.get(fieldToIndex.get("water_0")));
                    Double waterContent1 = Double.valueOf(fields.get(fieldToIndex.get("water_1")));

                    return new Tuple2<>(hour, new Tuple2<>((waterContent0 + waterContent1) / 2, 1));
                })
                        .reduceByKey((left, right) -> {
                            Double waterContent = left._1() + right._1();
                            Integer count = left._2() + right._2();

                            return new Tuple2<>(waterContent, count);
                        })
                        .map(pair -> {
                            String hour = pair._1();
                            Double avgWaterContent = pair._2()._1() / pair._2()._2();

                            return new Tuple2<>(hour, avgWaterContent);
                        })
                        .filter(pair -> pair._2() < thresholdWaterContent);

                // Set mongo write options
                Map<String, String> mongoOptions = new HashMap<>();
                mongoOptions.put("uri", "mongodb://localhost:27011/");
                mongoOptions.put("database", "precisionFarmingStreamingAnalysis");
                mongoOptions.put("collection", "waterContentAlert_" + topic);
                WriteConfig writeConfig = WriteConfig.create(mongoOptions);

                tempAvg.foreachRDD(rdd -> {
                    JavaRDD<Document> documents = rdd.map(fields -> {
                        Document document = new Document();

                        document.put("hour", fields._1());
                        document.put("lowWaterContent", fields._2());

                        return document;
                    });

                    MongoSpark.save(documents, writeConfig);
                });
            }
        }
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
        fieldToIndex.put("data_ora", 1);
        fieldToIndex.put("temp1_media", 2);
        fieldToIndex.put("temp1_min", 3);
        fieldToIndex.put("temp1_max", 4);
        fieldToIndex.put("ur1_media", 5);
        fieldToIndex.put("ur1_min", 6);
        fieldToIndex.put("ur1_max", 7);
        fieldToIndex.put("temp1_ur1_n_letture", 8);
        fieldToIndex.put("pioggia_mm", 9);
        fieldToIndex.put("wind_dir", 10);
        fieldToIndex.put("wind_dir_n_letture", 11);
        fieldToIndex.put("wind_speed_media", 12);
        fieldToIndex.put("wind_speed_min", 13);
        fieldToIndex.put("wind_speed_max", 14);
        fieldToIndex.put("wind_speed_n_letture", 15);
        fieldToIndex.put("pressione_mbar", 16);
        fieldToIndex.put("pressione_standard_mbar", 17);
        fieldToIndex.put("pressione_n_letture", 18);
        fieldToIndex.put("rad_W/mq_array", 19);
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
