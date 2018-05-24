package com.youhu.cores.test;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.DateUtils;
import com.youhu.cores.utils.HBaseUtils;
import com.youhu.cores.utils.SparkUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

// 测试，SparkStreaming消费Kafka消息。
public class KafkaStreaming2HBaseTest {
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

        JavaDStream<String> lines = kafkaStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            public Iterator<String> call(ConsumerRecord<String, String> t) {
                return Arrays.asList(t.value()).iterator();
            }
        });

        // 打印结果
        lines.print();

//        lines.foreachRDD(rdd ->{
//            rdd.foreachPartition(ite -> {
//                List<Row> batch = new ArrayList<>();
//                try {
//                    while (ite.hasNext()) {
//                        String str = ite.next();
//                        String[] arr = str.split(",");
//                        String str2 = DateUtils.getDateYMD();
//                        String row = arr[0] + "_" + str2;
//                        Put put = new Put(Bytes.toBytes(row));
//                        put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfUSERACTIONSTABLE[0]),
//                                Bytes.toBytes(RecSystemProperties.columnsOfUSERACTIONSTABLE[0]),
//                                Bytes.toBytes(arr[1]));
//                        batch.add(put);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                Object[] results = new Object[batch.size()];
//                try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.USERACTIONSTABLE);) {
//                    t.batch(batch, results);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            });
//        });

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
