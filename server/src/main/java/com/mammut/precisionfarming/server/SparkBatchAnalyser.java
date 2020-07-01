package com.mammut.precisionfarming.server;

import com.mongodb.Mongo;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class SparkBatchAnalyser {

    public void exec() {

        this.daysToWater();
        this.top10HumidDays();
        this.top10HotDays();

    }

    //calcola quali sono stati i 10 giogni piu caldi
    private void top10HotDays() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("server_batch")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precision_farming.meteo")
                //.config("spark.mongodb.output.uri", "mongodb://localhost:27011/precision_farming.top10HotDays")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        //log.info("numdoc: " + rdd.count());

        List<Document> top10HotDays = rdd
                .mapToPair(measure -> new Tuple2<>(measure.getString("data_ora").substring(0, 8), new Tuple2<>(measure.getDouble("temp1_media"), 1)))
                .reduceByKey((l, r) -> new Tuple2<>(l._1 + r._1, l._2 + r._2))
                .map(day -> {
                    Document d = new Document();
                    d.put("day", day._1);
                    d.put("avg_temp", day._2._1 / day._2._2);
                    return d;
                })
                .sortBy(d -> d.get("avg_temp"),false, 1)
                .take(10);

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27011");
        MongoDatabase precision_farmingDB = mongoClient.getDatabase("precision_farming");
        MongoCollection<Document> top10HotDaysCollection = precision_farmingDB.getCollection("top10HotDays");

        top10HotDaysCollection.insertMany(top10HotDays);

        mongoClient.close();
        spark.close();
    }

    //calcola quali sono stati i 10 giorni piu umidi
    private void top10HumidDays() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("server_batch")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precision_farming.meteo")
                //.config("spark.mongodb.output.uri", "mongodb://localhost:27011/precision_farming.top10HumidDays")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        //log.info("numdoc: " + rdd.count());

        List<Document> top10HumidDays = rdd
                .mapToPair(measure -> new Tuple2<>(measure.getString("data_ora").substring(0, 8), new Tuple2<>(measure.getDouble("ur1_media"), 1)))
                .reduceByKey((l, r) -> new Tuple2<>(l._1 + r._1, l._2 + r._2))
                .map(day -> {
                    Document d = new Document();
                    d.put("day", day._1);
                    d.put("avg_humidity", day._2._1 / day._2._2);
                    return d;
                })
                .sortBy(d -> d.get("avg_humidity"),false, 1)
                .take(10);

/*        List<Tuple2<String, Double>> top10HumidDays = rdd
                .mapToPair(measure -> new Tuple2<>(measure.getString("data_ora").substring(0, 8), new Tuple2<>(measure.getDouble("ur1_media"), 1)))
                .reduceByKey((l, r) -> new Tuple2<>(l._1 + r._1, l._2 + r._2))
                .map(day -> new Tuple2<>(day._1, day._2._1 / day._2._2))
                .sortBy(d -> d._2(), false, 1).takeOrdered(10);

        log.info("finali:" + Arrays.toString(top10HumidDays.toArray()));*/ //non funziona perche non ci sta comparator su tuple2


        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27011");
        MongoDatabase precision_farmingDB = mongoClient.getDatabase("precision_farming");
        MongoCollection<Document> top10HumidDaysCollection = precision_farmingDB.getCollection("top10HumidDays");

        top10HumidDaysCollection.insertMany(top10HumidDays);

        mongoClient.close();
        spark.close();
    }

    //calcola in quali giorni bisognava annaffiare il campo
    private void daysToWater() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("server_batch")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precision_farming.meteo")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27011/precision_farming.daysToWaterField")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        JavaRDD<Document> daysToWater = rdd
                .mapToPair(measure -> new Tuple2<>(measure.getString("data_ora").substring(0, 8), new Tuple2<>(measure.getDouble("temp1_media"), Float.valueOf(measure.get("pioggia_mm").toString()))))
                .groupByKey()
                .map(day -> {

                    AtomicInteger totHighTemp = new AtomicInteger();
                    AtomicBoolean isRained = new AtomicBoolean(false);

                    day._2.forEach(measure -> {
                        if (measure._1 > 18)
                            totHighTemp.getAndIncrement();

                        if (measure._2 > 0)
                            isRained.set(true);
                    });

                    return new Tuple3<>(day._1, totHighTemp.intValue(), isRained.get());
                })
                .filter(day -> day._2() > 5 && !day._3())
                .map(day -> {
                    Document d = new Document();
                    d.put("day",day._1());
                    d.put("totTempOver18",day._2());
                    d.put("isRained",day._3());
                    return d;
                });

        MongoSpark.save(daysToWater);

        jsc.close();
        spark.close();
    }
}
