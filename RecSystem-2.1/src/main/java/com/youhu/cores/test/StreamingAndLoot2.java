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
public class StreamingAndLoot2 {
    public static void anyway() {
        Timer timer = new Timer();
        // timer.schedule(new UpdateModelThread(), 10*60*1000, 10*60*1000);//
        // 在1min后执行此任务,每次间隔1min,再次执行一次
        timer.schedule(new hello(), 30 * 1000, 10 * 1000);

    }
}

class hello extends TimerTask implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public synchronized void run() {
        System.out.println("_____h1h1h1hh1h1h1_____");
    }
}