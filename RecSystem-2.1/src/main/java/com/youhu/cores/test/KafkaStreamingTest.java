package com.youhu.cores.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.SparkUtils;

import scala.Tuple2;

// 测试，SparkStreaming消费Kafka消息。
public class KafkaStreamingTest {
    public static void main(String[] args) {
        // 0.设定打印级别
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hbase").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR);
        JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));

        // Kafka參數
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("zk.connect", RecSystemProperties.ZK_QUORUM);
        kafkaParams.put("bootstrap.servers", RecSystemProperties.BROKER_LIST);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", RecSystemProperties.GROUP);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Kafka主题
        Collection<String> topics = Arrays.asList(RecSystemProperties.TOPIC);

        // Streaming接收kafka消息
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> words = kafkaStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            public Iterator<String> call(ConsumerRecord<String, String> t) {
                return Arrays.asList(t.value().split(" ")).iterator();
            }
        });

        // 对其中的单词进行统计
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(String s) {
                return new Tuple2(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        // 打印结果
        wordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.close();
    }
}
