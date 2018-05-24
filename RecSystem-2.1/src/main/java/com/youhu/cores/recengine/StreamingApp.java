package com.youhu.cores.recengine;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.DateUtils;
import com.youhu.cores.utils.HBaseUtils;
import com.youhu.cores.utils.SparkUtils;

/**
 * @ClassName: StreamingApp
 * @Description: 推荐系统引擎流运行类，在初始化之后开始接收各类数据，包括： 书籍数据，用户数据，用户评分，用户点击/浏览，用户收藏
 * @author: hiwes
 * @date: 2018年5月22日
 * @version: 2.1.1
 */
public class StreamingApp  implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public static void runningStreaming() {
		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.hbase").setLevel(Level.ERROR);
		JavaStreamingContext jssc = SparkUtils.getInstance().getStreamingContext();
	
		/**
		 * 接收书籍数据
		 */
		JavaDStream<String> bookStream = jssc.textFileStream(RecSystemProperties.BOOKSPATH);
		bookStream.print();
		
		// 直接将书籍数据进行录库，不进行其他操作,首字段作为row进行存储，存到booksInfoTable表中。
		bookStream.foreachRDD(rdd -> {
			if (!rdd.isEmpty()) {
				rdd.foreachPartition(ite -> {
					List<Row> batch = new ArrayList<>();
					try {
						while (ite.hasNext()) {
							String str = ite.next();
							String[] arr = str.split(",");
							if (arr.length >= 4) {
								Put put = new Put(Bytes.toBytes(arr[0]));
								for (int i = 1; i < arr.length; i++) {
									put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfBOOKSINFOTABLE[0]),
											Bytes.toBytes(RecSystemProperties.columnsOfBOOKSINFOTABLE[i - 1]),
											Bytes.toBytes(arr[i]));
									batch.add(put);
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					Object[] results = new Object[batch.size()];
					try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.BOOKSINFOTABLE);) {
						t.batch(batch, results);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		});
		
		/**
		 * 接收用户数据
		 */
		JavaDStream<String> userStream = jssc.textFileStream(RecSystemProperties.USERSPATH);
		userStream.print();
		// 先将用户数据进行录库，首字段作为row进行存储，存到usersInfoTable表中，根据KMeansModel对用户的类进行预测，向其进行第一次初始化推荐（好友/书籍）。
		userStream.foreachRDD(rdd -> {
			if (!rdd.isEmpty()) {
				rdd.foreachPartition(ite -> {
					List<Row> batch = new ArrayList<>();
					List<Row> batchF = new ArrayList<>();
					List<Row> batchR = new ArrayList<>();
					try {
						while (ite.hasNext()) {
							String str = ite.next();
							String[] arr = str.split(",");
							if (arr.length >= 4) {
								String userId = arr[0]; // 用户ID
								String gender = arr[2]; // 用户性别
								String age = arr[3]; // 用户年龄
								Put put = new Put(Bytes.toBytes(userId));
								for (int i = 1; i < arr.length; i++) {
									put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfUSERSINFOTABLE[0]),
											Bytes.toBytes(RecSystemProperties.columnsOfUSERSINFOTABLE[i - 1]),
											Bytes.toBytes(arr[i]));
									batch.add(put);
								}
								// KMeans模型相关
								String[] array = { userId, gender, age };
								double[] values = new double[array.length];
								for (int i = 0; i < array.length; i++) {
									values[i] = Double.parseDouble(array[i]);
								}
								Vector dense = Vectors.dense(values);
								// 下载当天KMeans模型
								KMeansModel dailyKMeansModel = KMeansModel.load(
										SparkUtils.getInstance().getSparkContext().sc(),
										RecSystemProperties.KMEANS_MODEL);

								int clusterNumber = dailyKMeansModel.predict(dense);
								// 根据类簇号进行查询clusters_recTable。
								String cluster = String.valueOf(clusterNumber);
								String friends = HBaseUtils.getInstance().getValue(
										RecSystemProperties.USERCLUSTERS_RECTABLE, cluster,
										RecSystemProperties.cfsOfUSERCLUSTERS_RECTABLE[0],
										RecSystemProperties.columnsOfUSERCLUSTERS_RECTABLE[0]);
								// 对friends_recTable进行初始化 推荐好友列表
								Put putOfFriends = new Put(Bytes.toBytes(userId));
								putOfFriends.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfFRIENDS_RECTABLE[0]),
										Bytes.toBytes(RecSystemProperties.columnsOfFRIENDS_RECTABLE[0]),
										Bytes.toBytes(friends));
								putOfFriends.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfFRIENDS_RECTABLE[0]),
										Bytes.toBytes(RecSystemProperties.columnsOfFRIENDS_RECTABLE[1]),
										Bytes.toBytes(cluster));
								batchF.add(putOfFriends);
								// 对books_recTable进行初始化 推荐书籍列表
								String books = HBaseUtils.getInstance().getValue(RecSystemProperties.USERCLUSTERS_RECTABLE,
										cluster, RecSystemProperties.cfsOfUSERCLUSTERS_RECTABLE[0],
										RecSystemProperties.columnsOfUSERCLUSTERS_RECTABLE[1]);
								Put putOfRating = new Put(Bytes.toBytes(userId));
								putOfRating.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfBOOKS_RECTABLE[0]),
										Bytes.toBytes(RecSystemProperties.columnsOfBOOKS_RECTABLE[0]),
										Bytes.toBytes(books));
								batchR.add(putOfRating);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					Object[] results1 = new Object[batch.size()];
					Object[] results2 = new Object[batchF.size()];
					Object[] results3 = new Object[batchR.size()];
					try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.USERSINFOTABLE);
							Table tf = HBaseUtils.getInstance().getTable(RecSystemProperties.FRIENDS_RECTABLE);
							Table tr = HBaseUtils.getInstance().getTable(RecSystemProperties.BOOKS_RECTABLE);) {
						t.batch(batch, results1);
						tf.batch(batchF, results2);
						tr.batch(batchR, results3);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		});
		
		/**
		 * 接收评分数据
		 */
		JavaDStream<String> ratingStream = jssc.textFileStream(RecSystemProperties.RATINGSPATH);
		ratingStream.print();
		// 直接将评分数据进行录库，不进行其他操作,"首字段_次字段"作为row进行存储，存到ratingsTable表中。
		ratingStream.foreachRDD(rdd -> {
			if (!rdd.isEmpty()) {
				rdd.foreachPartition(ite -> {
					List<Row> batch = new ArrayList<>();
					try {
						while (ite.hasNext()) {
							String str = ite.next();
							String[] arr = str.split(",");
							if (arr.length == 3) {
								String row = arr[0] + "_" + arr[1];
								Put put = new Put(Bytes.toBytes(row));
								for (int i = 2; i < arr.length; i++) {
									put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfRATINGSTABLE[0]),
											Bytes.toBytes(RecSystemProperties.columnsOfRATINGSTABLE[i - 2]),
											Bytes.toBytes(arr[i]));
									batch.add(put);
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					Object[] results = new Object[batch.size()];
					try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.RATINGSTABLE);) {
						t.batch(batch, results);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		});
		/**
		 * 接收收藏数据
		 */
		JavaDStream<String> collectStream = jssc.textFileStream(RecSystemProperties.COLLECTSPATH);
		collectStream.print();
		// 直接将收藏数据进行录库，不进行其他操作,"ID_当前时间戳"作为row进行存储，存到userActionTable表中的collect列。
		collectStream.foreachRDD(rdd -> {
			if (!rdd.isEmpty()) {
				rdd.foreachPartition(ite -> {
					List<Row> batch = new ArrayList<>();
					try {
						while (ite.hasNext()) {
							String str = ite.next();
							String[] arr = str.split(",");
							String str2 = DateUtils.getDateYMD();
							String row = arr[0] + "_" + str2;
							Put put = new Put(Bytes.toBytes(row));
							put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfUSERACTIONSTABLE[0]),
									Bytes.toBytes(RecSystemProperties.columnsOfUSERACTIONSTABLE[1]),
									Bytes.toBytes(arr[1]));
							batch.add(put);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					Object[] results = new Object[batch.size()];
					try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.USERACTIONSTABLE);) {
						t.batch(batch, results);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		});

		/**
		 * 接收浏览数据
		 */
		JavaDStream<String> browseStream = jssc.textFileStream(RecSystemProperties.BROWSESPATH);
		// 直接将浏览数据进行录库，不进行其他操作,"ID_当前时间戳"作为row进行存储，存到userActionTable表中的browse列。
		browseStream.print();
		browseStream.foreachRDD(rdd -> {
			if (!rdd.isEmpty()) {
				rdd.foreachPartition(ite -> {
					List<Row> batch = new ArrayList<>();
					try {
						while (ite.hasNext()) {
							String str = ite.next();
							String[] arr = str.split(",");
							String str2 = DateUtils.getDateYMD();
							String row = arr[0] + "_" + str2;
							Put put = new Put(Bytes.toBytes(row));
							put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfUSERACTIONSTABLE[0]),
									Bytes.toBytes(RecSystemProperties.columnsOfUSERACTIONSTABLE[0]),
									Bytes.toBytes(arr[1]));
							batch.add(put);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					Object[] results = new Object[batch.size()];
					try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.USERACTIONSTABLE);) {
						t.batch(batch, results);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		});

		UpdateRecResult.timeMaker();
		/**
		 * 开启数据流
		 */
		try {
			jssc.start();
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}
