package com.mammut.precisionfarming.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.regex.Pattern;

@Component
@Slf4j
public class SparkStreamingListener {

    private final Pattern separator = Pattern.compile(",");
    private final String hostname = "localhost";
    private final int port = 9092;

    public void listen() {

        SparkConf sparkConf = new SparkConf().setAppName("prova").setMaster("local");
        log.info("SparkConf set for {}", sparkConf);

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        log.info("JavaStreamingContext set {}", ssc);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(this.hostname, this.port, StorageLevels.MEMORY_AND_DISK_SER);
        log.info("lines: {}", lines);

        JavaDStream<String> fields = lines.flatMap(line -> Arrays.asList(separator.split(line)).iterator());
        log.info("fields: {}", fields);

    }

}
