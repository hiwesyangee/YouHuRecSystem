package com.youhu.cores.test;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.SparkUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

// 测试：Streaming和Loot的兼容问题
public class StreamingAndLoot {
    public static void anyway() {
        // 0.设定打印级别
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hbase").setLevel(Level.ERROR);
        JavaStreamingContext jssc = SparkUtils.getInstance().getStreamingContext();
        JavaDStream<String> bookStream = jssc.textFileStream(RecSystemProperties.BOOKSPATH);
        bookStream.print();

        StreamingAndLoot2.anyway();

        try{
            jssc.start();
            jssc.awaitTermination();
        }catch(InterruptedException e){
            e.printStackTrace();
        }

    }
}

