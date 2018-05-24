package com.youhu.cores.utils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @ClassName: HdfsUtils 
 * @Description: HDFS工具类
 * @author: hiwes
 * @date: 2018年5月22日
 * @Version： 2.1.1
 */
public class HdfsUtils {
	/**
	 * 创建新的HDFS文件
	 */
	public static boolean createNewHDFSFile(String newFile, String content) throws IOException {
		if (StringUtils.isBlank(newFile) || null == content) {
			return false;
		}
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(newFile), config);
		FSDataOutputStream os = hdfs.create(new Path(newFile));
		os.write(content.getBytes("UTF-8"));
		os.close();
		hdfs.close();
		return true;
	}

	/**
	 * 创建新的HDFS文件夹
	 */
	public static boolean createNewHDFSDir(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		if (!fs.exists(new Path(dir))) {
			fs.mkdirs(new Path(dir));
		}
		fs.close();
		return true;
	}

	/**
	 * 删除HDFS目录文件夹
	 */
	public static boolean deleteHDFSDir(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		fs.delete(new Path(dir), true);
		fs.close();
		return true;
	}

	/**
	 * 删除HDFS目录文件
	 */
	public static boolean deleteHDFSFile(String hdfsFile) throws IOException {
		if (StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), config);
		Path path = new Path(hdfsFile);
		boolean isDeleted = hdfs.delete(path, true);
		hdfs.close();
		return isDeleted;
	}

	/**
	 * 列出HDFS目录下所有文件
	 */
	public static List<String> listAll(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return new ArrayList<String>();
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		FileStatus[] stats = fs.listStatus(new Path(dir));
		List<String> names = new ArrayList<String>();
		for (int i = 0; i < stats.length; ++i) {
			if (stats[i].isFile()) {
				names.add(stats[i].getPath().toString());
			} else if (stats[i].isDirectory()) {
				names.add(stats[i].getPath().toString());
			} else if (stats[i].isSymlink()) {
				names.add(stats[i].getPath().toString());
			}
		}
		fs.close();
		return names;
	}

	/**
	 * 上传本地文件到HDFS
	 */
	public static boolean uploadLocalFile2HDFS(String localFile, String hdfsFile) throws IOException {
		if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://hy:8020"), config);
		Path src = new Path(localFile);
		Path dst = new Path(hdfsFile);
		hdfs.copyFromLocalFile(src, dst);
		hdfs.close();
		return true;
	}

	/**
	 * 将HDFS文件进行追加。
	 */
	public static void addFile2HDFS(String hFile, String file) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);

		Path inputDir = new Path(file);
		Path hdfsFile = new Path(hFile);
		try {
			FileStatus[] inputFiles = local.listStatus(inputDir);
			FSDataOutputStream out = hdfs.append(hdfsFile);
			for (int i = 0; i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				byte buffer[] = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
