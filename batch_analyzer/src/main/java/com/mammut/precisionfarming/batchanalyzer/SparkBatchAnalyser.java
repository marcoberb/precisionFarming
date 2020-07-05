package com.mammut.precisionfarming.batchanalyzer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Precision;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class SparkBatchAnalyser {

    public void execAll() {

        this.last3DaysTrend();
        this.daysToWater();
        this.top10HumidDays();
        this.top10HotDays();
        this.top10ColdDays();

    }

    //restituisce temperatura e umidità dei 3 gg passati e una flag che indica se il trend è calore in aumento
    void last3DaysTrend() {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yy");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("last3DaysTrend")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precisionFarmingRaw.iotData_meteo_2020_gennaio_marzo")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        log.info(String.valueOf(rdd.count()));

        List<Tuple3<Date, Double, Double>> last3days = rdd
                .mapToPair(measure ->
                        new Tuple2<>(simpleDateFormat.parse(measure.getString("data_ora").substring(0, 8)),
                        new Tuple3<>(measure.getDouble("temp1_media"), measure.getDouble("ur1_media"), 1)))
                .reduceByKey((l, r) -> new Tuple3<>(l._1() + r._1(), l._2() + r._2(), l._3() + r._3())) //non servirebbe il count delle tuple perche tanto devono essere 288 per passare il filtro
                .filter(day -> day._2._3() == 288) //288 è il numero massimo di misurazioni in un giorno
                .sortByKey(false)
                .map(day -> new Tuple3<>(day._1, day._2._1() / day._2._3(), day._2._2() / day._2._3()))
                .take(3);

        double prev_temp = Double.MIN_VALUE;
        double prev_hum = Double.MAX_VALUE;
        boolean isGettingHotter = true;

        for (Tuple3<Date, Double, Double> day: last3days) {
            double temp = day._2();
            double hum = day._3();
            if(temp > prev_temp && hum < prev_hum) {
                prev_temp = temp;
                prev_hum = hum;
            }
            else
                isGettingHotter = false;
        }

        Document last3daysTrend = new Document();
        last3daysTrend.put("day_minus_1", simpleDateFormat.format(last3days.get(0)._1()));
        last3daysTrend.put("avg_temp1", Precision.round(last3days.get(0)._2(), 2));
        last3daysTrend.put("avg_hum1", Precision.round(last3days.get(0)._3(), 2));
        last3daysTrend.put("day_minus_2", simpleDateFormat.format(last3days.get(1)._1()));
        last3daysTrend.put("avg_temp2", Precision.round(last3days.get(1)._2(), 2));
        last3daysTrend.put("avg_hum2", Precision.round(last3days.get(1)._3(), 2));
        last3daysTrend.put("day_minus_3", simpleDateFormat.format(last3days.get(2)._1()));
        last3daysTrend.put("avg_temp3", Precision.round(last3days.get(2)._2(), 2));
        last3daysTrend.put("avg_hum3", Precision.round(last3days.get(2)._3(), 2));
        last3daysTrend.put("isGettingHotter", isGettingHotter);

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27011");
        MongoDatabase precision_farmingDB = mongoClient.getDatabase("precisionFarmingBatchResults");
        MongoCollection<Document> last3DaysTrendCollection = precision_farmingDB.getCollection("last3DaysTrend");

        last3DaysTrendCollection.insertOne(last3daysTrend);

        mongoClient.close();
        spark.close();
    }

    //calcola in quali giorni bisognava annaffiare il campo
    void daysToWater() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("server_batch")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precisionFarmingRaw.iotData_meteo_2020_gennaio_marzo")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27011/precisionFarmingBatchResults.daysToWaterField")
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
                        if (measure._1 > 22)
                            totHighTemp.getAndIncrement();

                        if (measure._2 > 0)
                            isRained.set(true);
                    });

                    return new Tuple3<>(day._1, totHighTemp.intValue(), isRained.get());
                })
                .filter(day -> day._2() > 10 && !day._3())
                .map(day -> {
                    Document d = new Document();
                    d.put("day",day._1());
                    d.put("totTempOver22",day._2());
                    d.put("isRained",day._3());
                    return d;
                });

        MongoSpark.save(daysToWater);

        jsc.close();
        spark.close();
    }

    //calcola quali sono stati i 10 giogni piu caldi
    void top10HotDays() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("top10HotDays")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precisionFarmingRaw.iotData_meteo_2020_gennaio_marzo")
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
                    d.put("avg_temp", Precision.round(day._2._1 / day._2._2,2));
                    return d;
                })
                .sortBy(d -> d.get("avg_temp"),false, 1)
                .take(10);

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27011");
        MongoDatabase precision_farmingDB = mongoClient.getDatabase("precisionFarmingBatchResults");
        MongoCollection<Document> top10HotDaysCollection = precision_farmingDB.getCollection("top10HotDays");

        top10HotDaysCollection.insertMany(top10HotDays);

        mongoClient.close();
        spark.close();
    }

    //calcola quali sono stati i 10 giorni piu freddi
    void top10ColdDays(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("top10ColdDays")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precisionFarmingRaw.iotData_meteo_2020_gennaio_marzo")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        //log.info("numdoc: " + rdd.count());

        List<Document> top10ColdDays = rdd
                .mapToPair(measure -> new Tuple2<>(measure.getString("data_ora").substring(0, 8), new Tuple2<>(measure.getDouble("temp1_media"), 1)))
                .reduceByKey((l, r) -> new Tuple2<>(l._1 + r._1, l._2 + r._2))
                .map(day -> {
                    Document d = new Document();
                    d.put("day", day._1);
                    d.put("avg_temp", Precision.round(day._2._1 / day._2._2, 2));
                    return d;
                })
                .sortBy(d -> d.get("avg_temp"),true, 1)
                .take(10);

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27011");
        MongoDatabase precision_farmingDB = mongoClient.getDatabase("precisionFarmingBatchResults");
        MongoCollection<Document> top10HotDaysCollection = precision_farmingDB.getCollection("top10ColdDays");

        top10HotDaysCollection.insertMany(top10ColdDays);

        mongoClient.close();
        spark.close();
    }

    //calcola quali sono stati i 10 giorni piu umidi
    void top10HumidDays() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("server_batch")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27011/precisionFarmingRaw.iotData_meteo_2020_gennaio_marzo")
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
                    d.put("avg_humidity", Precision.round(day._2._1 / day._2._2,2));
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
        MongoDatabase precision_farmingDB = mongoClient.getDatabase("precisionFarmingBatchResults");
        MongoCollection<Document> top10HumidDaysCollection = precision_farmingDB.getCollection("top10HumidDays");

        top10HumidDaysCollection.insertMany(top10HumidDays);

        mongoClient.close();
        spark.close();
    }
}
