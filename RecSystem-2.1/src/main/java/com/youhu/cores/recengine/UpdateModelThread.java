package com.youhu.cores.recengine;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.HBaseUtils;
import com.youhu.cores.utils.HdfsUtils;
import com.youhu.cores.utils.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 当前版本循环更新线程的任务是：
 * 1.书籍推荐的任务是：每次读取booksInfoTable，获取ID列表并随机进行推荐，更新books_recTable的rank列
 * 2.好友推荐的任务是：每次读取usersInfoTable，针对表数据进行KMeans建模，同时进行用户的类簇预测，还是按照之前的判断：
 * 将类簇号和用户列表找出来，然后把好友推荐表和类簇表都进行更新。
 */

public class UpdateModelThread extends TimerTask implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;

    // 内部类进行变型
    static class ParseVector implements Function<String, Vector> {
        private static final long serialVersionUID = 1L;
        private static final Pattern COMMA = Pattern.compile(",");

        @Override
        public Vector call(String s) {
            String[] array = COMMA.split(s);
            double[] values = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                values[i] = Double.parseDouble(array[i]);
            }
            return Vectors.dense(values);
        }
    }

    @Override
    public synchronized void run() {
        long start = System.currentTimeMillis();
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hbase").setLevel(Level.ERROR);
        JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();

        // 读取书籍表
        Configuration configuration = HBaseUtils.getInstance().getConfiguration();
        try {
            configuration.set(TableInputFormat.INPUT_TABLE, RecSystemProperties.BOOKSINFOTABLE);
            Scan scan = new Scan();
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            configuration.set(TableInputFormat.SCAN, ScanToString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JavaPairRDD<ImmutableBytesWritable, Result> booksInfoTableData = sc.newAPIHadoopRDD(configuration,
                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        configuration.clear();// 释放连接资源

        JavaRDD<String> booksListData = booksInfoTableData.map(tup -> Bytes.toString(tup._2().getRow()));
        /** 获取所有书籍的编号列表 */
        List<String> booksList = booksListData.collect();


        // 读取用户数据信息
        configuration.set(TableInputFormat.INPUT_TABLE, RecSystemProperties.USERSINFOTABLE);
        try {
            Scan scan2 = new Scan();
            ClientProtos.Scan proto2 = ProtobufUtil.toScan(scan2);
            String ScanToString2 = Base64.encodeBytes(proto2.toByteArray());
            configuration.set(TableInputFormat.SCAN, ScanToString2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JavaPairRDD<ImmutableBytesWritable, Result> usersInfoTableData = sc
                .newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .cache();
        configuration.clear();// 释放连接资源

        // 获取所有用户的编号列表
        JavaRDD<String> usersListData = usersInfoTableData.map(tup -> Bytes.toString(tup._2().getRow())).cache();
        System.out.println(usersListData.count());
        // 针对每一位用户进行推荐书籍初始化

        // 遍历所有用户列表，对每个用户都进行好友推荐
        usersListData.foreach(str -> {
            List<Row> batch = new ArrayList<>();
            List<String> randomList = getRandomList(booksList, 10);
            String result = randomList.toString();
            String recBooks = result.substring(1, result.length() - 1);
            Put put = new Put(Bytes.toBytes(str));
            put.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfBOOKS_RECTABLE[0]),
                    Bytes.toBytes(RecSystemProperties.columnsOfBOOKS_RECTABLE[0]), Bytes.toBytes(recBooks));
            batch.add(put);
            Object[] results = new Object[batch.size()];
            try (Table t = HBaseUtils.getInstance().getTable(RecSystemProperties.BOOKS_RECTABLE);) {
                t.batch(batch, results);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        usersListData.unpersist();
        // 获取用户编号，性别，年龄
        JavaRDD<String> userData = usersInfoTableData
                .map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String call(Tuple2<ImmutableBytesWritable, Result> tup) throws Exception {
                        Result result = tup._2();
                        String userId = Bytes.toString(result.getRow());
                        String gender = Bytes
                                .toString(result.getValue(Bytes.toBytes(RecSystemProperties.cfsOfUSERSINFOTABLE[0]),
                                        Bytes.toBytes(RecSystemProperties.columnsOfUSERSINFOTABLE[1])));
                        String age = Bytes
                                .toString(result.getValue(Bytes.toBytes(RecSystemProperties.cfsOfUSERSINFOTABLE[0]),
                                        Bytes.toBytes(RecSystemProperties.columnsOfUSERSINFOTABLE[2])));
                        return new String(userId + "," + gender + "," + age);
                    }
                });

        // 获取用户数量
        int usersNumber = (int) userData.count();

        JavaRDD<Vector> kmeansVector = userData.map(new ParseVector());
        // 将用户分簇，并进行KMeans建模
        int numClusters = 100;
        if (usersNumber <= 100) { // 逻辑判断，设定k的值
            numClusters = 5;
        } else if (usersNumber > 100 && usersNumber <= 150) {
            numClusters = 8;
        } else if (usersNumber > 150 && usersNumber <= 200) {
            numClusters = 10;
        } else if (usersNumber > 200 && usersNumber <= 250) {
            numClusters = 12;
        } else if (usersNumber > 250 && usersNumber <= 300) {
            numClusters = 15;
        } else if (usersNumber > 300 && usersNumber <= 350) {
            numClusters = 20;
        } else if (usersNumber > 350 && usersNumber <= 400) {
            numClusters = 25;
        } else if (usersNumber > 400 && usersNumber <= 500) {
            numClusters = 30;
        } else if (usersNumber > 500 && usersNumber <= 700) {
            numClusters = 40;
        } else if (usersNumber > 700 && usersNumber <= 1000) {
            numClusters = 50;
        }
        int numIterations = 5;
        // KMeans算法建模
        KMeansModel originalKMeansModel = KMeans.train(kmeansVector.rdd(), numClusters, numIterations);

        // 将KMeans模型保存到HDFS
        try {
            HdfsUtils.deleteHDFSFile(RecSystemProperties.KMEANS_MODEL);
            originalKMeansModel.save(sc.sc(), RecSystemProperties.KMEANS_MODEL); // 保存KMeans模型
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<Integer, Integer> userCluster = userData.mapToPair(new PairFunction<String, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Integer> call(String str) throws Exception {
                String[] array = str.split(",");
                double[] values = new double[array.length];
                for (int i = 0; i < array.length; i++) {
                    values[i] = Double.parseDouble(array[i]);
                }
                Vector dense = Vectors.dense(values);
                int clusterNumber = originalKMeansModel.predict(dense);

                int user = Integer.parseInt(array[0]);
                return new Tuple2(clusterNumber, user); // 这里返回的是 (用户所属类簇号，用户ID)
            }
        });

        JavaRDD<Tuple2<Integer, String>> clustersAndUsersList = userCluster.groupByKey()
                .map(new Function<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<Integer, Iterable<Integer>> tuple2) throws Exception {
                        String result = "";
                        for (int i : tuple2._2()) {
                            result += String.valueOf(i) + ",";
                        }
                        return new Tuple2(tuple2._1(), result.substring(0, result.length() - 1));
                    }
                }).cache(); // 这里得到的数据是Tuple2<>（类簇号，该类簇号下的所有用户列表）。

        List<String> usersList2 = userData.map(x -> x.split(",")[0]).distinct().collect(); // users列表
        userData.unpersist();

        clustersAndUsersList.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                // 获取表对象
                Table table1 = HBaseUtils.getInstance().getTable(RecSystemProperties.FRIENDS_RECTABLE);
                Table table2 = HBaseUtils.getInstance().getTable(RecSystemProperties.USERCLUSTERS_RECTABLE);

                String clusNumber = String.valueOf(tuple2._1());
                String friendsList = String.valueOf(tuple2._2());
                String[] split = friendsList.split(",");
                List<String> list = Arrays.asList(split);

                List<Row> batch1 = new ArrayList<>();
                List<Row> batch2 = new ArrayList<>();

                Put put1;
                Put put2;
                for (String user : usersList2) {
                    if (list.contains(user)) {
                        // 查询该用户的推荐书籍
                        String recBooks = HBaseUtils.getInstance().getValue(RecSystemProperties.BOOKS_RECTABLE, user,
                                RecSystemProperties.cfsOfBOOKS_RECTABLE[0],
                                RecSystemProperties.columnsOfBOOKS_RECTABLE[0]);
                        // 先对好友推荐表进行写入
                        put1 = new Put(Bytes.toBytes(user)); // 设置rowkey1
                        put1.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfFRIENDS_RECTABLE[0]),
                                Bytes.toBytes(RecSystemProperties.columnsOfFRIENDS_RECTABLE[0]),
                                Bytes.toBytes(friendsList));
                        batch1.add(put1);

                        // 再对类簇推荐表进行写入
                        put2 = new Put(Bytes.toBytes(clusNumber)); // 设置rowkey2
                        put2.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfUSERCLUSTERS_RECTABLE[0]),
                                Bytes.toBytes(RecSystemProperties.columnsOfUSERCLUSTERS_RECTABLE[0]),
                                Bytes.toBytes(friendsList));
                        put2.addColumn(Bytes.toBytes(RecSystemProperties.cfsOfUSERCLUSTERS_RECTABLE[0]),
                                Bytes.toBytes(RecSystemProperties.columnsOfUSERCLUSTERS_RECTABLE[1]),
                                Bytes.toBytes(recBooks));
                        batch2.add(put2);
                    }
                }
                Object[] results1 = new Object[batch1.size()];
                Object[] results2 = new Object[batch2.size()];
                try {
                    table1.batch(batch1, results1);
                    table2.batch(batch2, results2);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 关闭table
                    if (table1 != null) {
                        try {
                            table1.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else if (table2 != null) {
                        try {
                            table2.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        clustersAndUsersList.unpersist();

        long stop = System.currentTimeMillis();
        System.out.println("\n" + "本次推荐更新用时===" + (stop - start) * 1.0 / 1000 + "s");
    }

    // 返回随机List，且不重复
    public static List<String> getRandomList(List<String> top100, int count) {
        if (top100.size() < count) {
            return top100;
        }
        Random random = new Random();
        List<Integer> tempList = new ArrayList<Integer>();
        List<String> newList = new ArrayList<String>();
        int temp;
        for (int i = 0; i < count; i++) {
            temp = random.nextInt(top100.size());// 将产生的随机数作为被抽list的索引
            if (!tempList.contains(temp)) {
                tempList.add(temp);
                newList.add(top100.get(temp));
            } else {
                i--;
            }
        }
        return newList;
    }
}
