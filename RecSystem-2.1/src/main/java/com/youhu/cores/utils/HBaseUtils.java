package com.youhu.cores.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: HBaseUtils 
 * @Description: HBase工具类
 * @author: hiwes
 * @date: 2018年5月22日
 * @Version: 2.1.1
 */
public class HBaseUtils {
	private Configuration conf;
	private Connection conn;
	private Admin admin;
	private Table table;

	private HBaseUtils() {
		conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "master:2181");
		conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static HBaseUtils instance = null;

	// 双重校验锁，保证线程安全
	public static synchronized HBaseUtils getInstance() {
		if (instance == null) {
			synchronized (HBaseUtils.class) {
				if (instance == null) {
					instance = new HBaseUtils();
				}
			}
		}
		return instance;
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public Connection getConnection() {
		return conn;
	}

	public Admin getAdmin() {
		return admin;
	}

	public Table getTable(String tableName) {
		try {
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}

	/**
	 * HBase创建表
	 */
	public void createTable(String tableName, String[] cfs) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		TableName tName = TableName.valueOf(tableName);
		if (admin.tableExists(tName)) {				// 如果存在要创建的表，那么先删除，再创建
			admin.disableTable(tName);
			admin.deleteTable(tName);
		}
		for (String cf : cfs) {
			desc.addFamily(new HColumnDescriptor(cf));
		}
		createTable(desc);
	}

	public void createTable(HTableDescriptor desc) throws IOException {
		admin.createTable(desc);
	}

	public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
		admin.createTable(desc, splitKeys);
	}

	public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
		admin.createTable(desc, startKey, endKey, numRegions);
	}

	/**
	 * HBase更新表——单条方式
	 */
	public void putTable(String tableName, String rowkey, String family, String qualifier, String value)
			throws IOException {
		Table table = getTable(tableName);
		Put put = new Put(rowkey.getBytes());
		put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
		table.put(put);
	}

	/**
	 * HBase更新表——List方式
	 */
	public void putTable(String tableName, List<Put> puts) throws IOException {
		Table table = getTable(tableName);
		table.put(puts);
	}

	/**
	 * 根据文件名，表名，列簇，列名，将文件中每行以逗号隔开的第一个元素作为row，之后每个元素作为列名进行批处理写入
	 */
	public void batchTable(String fileName, String tableName, String cf, String[] columns) {
		Table table = getTable(tableName);
		List<Row> batch = new ArrayList<Row>();

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString;
			while ((tempString = reader.readLine()) != null) {
				String[] arrLine = tempString.split(",");
				try {
					// 对表进行批量put操作
					Put put = new Put(Bytes.toBytes(arrLine[0])); // 利用ID作为row
					for (int i = 0; i < columns.length; i++) {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i]), Bytes.toBytes(arrLine[i + 1]));
					}
					batch.add(put);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException el) {
					el.printStackTrace();
				}
			}
			Object[] results = new Object[batch.size()];
			try {
				table.batch(batch, results);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 写死方法：对评分文件进行HBase批处理写入，使用"元素1_元素2"作为row，元素3作为value进行写入。
	 */
	public void ratingsBatchTable(String fileName, String tableName, String cf, String[] columns) {
		Table table = getTable(tableName);
		List<Row> batch = new ArrayList<Row>();

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString;
			while ((tempString = reader.readLine()) != null) {
				String[] arrLine = tempString.split(",");
				try {
					// 对表进行批量put操作
					String row = arrLine[0] + "_" + arrLine[1];
					Put put = new Put(Bytes.toBytes(row)); // 利用"用户ID_书籍ID"作为row
					for (int i = 0; i < columns.length; i++) {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i]), Bytes.toBytes(arrLine[i + 2]));
					}
					batch.add(put);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException el) {
					el.printStackTrace();
				}
			}
			Object[] results = new Object[batch.size()];
			try {
				table.batch(batch, results);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 写死方法2：对榜单文件进行HBase批处理写入，使用"排行编号"作为row，元素不同的值插入不同的列
	 */
	public void rankListBatchTable(String fileName, String tableName, String cf, String[] columns) {
		Table table = getTable(tableName);
		List<Row> batch = new ArrayList<>();

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString;
			while ((tempString = reader.readLine()) != null) {
				String[] arrLine = tempString.split(",");
				try {
					// 对表进行批量put操作
					String row = arrLine[0];
					Put put = new Put(Bytes.toBytes(row)); // 利用"排行编号=1,2,3,4等"作为row
					for (int i = 0; i < columns.length; i++) {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i]), Bytes.toBytes(arrLine[i+1].split(":")[1]));
					}
					batch.add(put);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException el) {
					el.printStackTrace();
				}
			}
			Object[] results = new Object[batch.size()];
			try {
				table.batch(batch, results);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * HBase获取数据，根据row，列簇1,列名列表1,列簇2,列名列表2，获得Map。
	 */
	public Map<String, Object> get(String tableName, String row, String family1, List<String> qualifiers1,
			String family2, List<String> qualifiers2) throws IOException {
		Map<String, Object> map = new HashMap<>();
		Get get = new Get(row.getBytes());
		qualifiers1.forEach(qualifier -> {
			get.addColumn(family1.getBytes(), qualifier.getBytes());
		});
		qualifiers2.forEach(qualifier -> {
			get.addColumn(family2.getBytes(), qualifier.getBytes());
		});
		Result rs = getTable(tableName).get(get);
		qualifiers1.forEach(qualifier -> {
			Cell cell = rs.getColumnLatestCell(family1.getBytes(), qualifier.getBytes());
			map.put(qualifier, CellUtil.cloneValue(cell));
		});
		qualifiers2.forEach(qualifier -> {
			Cell cell = rs.getColumnLatestCell(family2.getBytes(), qualifier.getBytes());
			map.put(qualifier, CellUtil.cloneValue(cell));
		});
		return map;
	}

	/**
	 * HBase获取数据，根据row，列簇,列名列表，获得Map。
	 */
	public Map<String, Object> get(String tableName, String row, String family, List<String> qualifiers)
			throws IOException {
		Map<String, Object> map = new HashMap<>();
		Get get = new Get(row.getBytes());
		qualifiers.forEach(qualifier -> {
			get.addColumn(family.getBytes(), qualifier.getBytes());
		});
		Result rs = getTable(tableName).get(get);
		qualifiers.forEach(qualifier -> {
			Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());
			map.put(qualifier, CellUtil.cloneValue(cell));
		});
		return map;
	}

	/**
	 * HBase获取数据，根据表名，row,列簇，列名，获得String。
	 */
	public String getValue(String tableName, String rowKey, String cf, String column) throws IOException {
		Table table = getTable(tableName);
		Get g = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(g);
		byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(column));
		String result1 = Bytes.toString(value);
		table.close();
		return result1;
	}

	/**
	 * HBase删除表
	 */
	public void deleteTable(String tableName) throws IOException {
		admin.disableTables(tableName);
		admin.deleteTables(tableName);
	}

	/**
	 * 释放资源
	 */
	public void releaseResource() {
		if (null != table) {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (null != admin) {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (null != conn) {
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
