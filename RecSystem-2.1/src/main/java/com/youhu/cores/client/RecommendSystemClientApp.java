package com.youhu.cores.client;

import java.io.IOException;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.youhu.cores.recengine.CreateTable;
import com.youhu.cores.recengine.InitializeRecApp;
import com.youhu.cores.recengine.StreamingApp;

/**
 * @ClassName: HBaseUtils
 * @Description:推荐系统客户端运行类,主要作用：读取数据库，将用户数据和书籍数据进行读取，并对用户的书籍和好友推荐进行初始化 在这里，要生成——书籍推荐表，好友推荐表，用户类簇推荐表，书籍类簇推荐表
 * @author: hiwes
 * @date: 2018年5月22日
 * @Version: 2.1.1
 */
public class RecommendSystemClientApp implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		// 0.设定打印级别
		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.hbase").setLevel(Level.ERROR);
		// 1.创建所有初始化需要的表
		CreateTable.createAllTable();
		// 2.推荐初始化，对用户的书籍推荐和好友推荐进行初始化
		long start = System.currentTimeMillis();
		InitializeRecApp.InitializeRec();
		long stop = System.currentTimeMillis();
		System.out.println("\n" + "初始化推荐用时=====" + (stop - start) * 1.0 / 1000 + "s");

		// 3.启用Streaming流计算，接收数据
		StreamingApp.runningStreaming();

	}
}
