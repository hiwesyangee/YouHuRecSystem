package com.youhu.cores.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @ClassName: SparkUtils 
 * @Description: Spark工具类
 * @author: hiwes
 * @date: 2018年5月22日
 * @Version: 2.1.1
 */
public class SparkUtils {
	private SparkConf conf;
	private JavaSparkContext sc;
	private JavaStreamingContext jssc;

	private SparkUtils() {
		conf = new SparkConf();
		conf.setAppName("RecommendSystem");
//		conf.setMaster("local[4]");
		sc = new JavaSparkContext(conf);
		jssc = new JavaStreamingContext(sc, Durations.seconds(10));
		jssc.checkpoint("hdfs://master:9000/opt/checkpoint/");
	}
	
	private static SparkUtils instance = null;

	// 双重校验锁，保证线程安全
	public static synchronized SparkUtils getInstance() {
		if (instance == null) {
			synchronized (SparkUtils.class) {
				if (instance == null) {
					instance = new SparkUtils();
				}
			}
		}
		return instance;
	}

	/**
	 * 获取单例对象sc实例
	 */
	public JavaSparkContext getSparkContext() {
		return sc;
	}

	/**
	 * 获取单例对象jssc实例
	 */
	public JavaStreamingContext getStreamingContext() {
		return jssc;
	}
}
