package com.youhu.cores.recengine;

import java.io.IOException;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.HBaseUtils;

public class CreateTable {
	public static void createAllTable() throws IOException {
		// 创建书籍推荐表/好友推荐表/类簇推荐表/评分表/用户行为表
		try {
			HBaseUtils.getInstance().createTable(RecSystemProperties.BOOKS_RECTABLE,
					RecSystemProperties.cfsOfBOOKS_RECTABLE);
			HBaseUtils.getInstance().createTable(RecSystemProperties.FRIENDS_RECTABLE,
					RecSystemProperties.cfsOfFRIENDS_RECTABLE);
			HBaseUtils.getInstance().createTable(RecSystemProperties.USERCLUSTERS_RECTABLE,
					RecSystemProperties.cfsOfUSERCLUSTERS_RECTABLE);
			HBaseUtils.getInstance().createTable(RecSystemProperties.RATINGSTABLE,
					RecSystemProperties.cfsOfRATINGSTABLE);
			HBaseUtils.getInstance().createTable(RecSystemProperties.USERACTIONSTABLE,
					RecSystemProperties.cfsOfUSERACTIONSTABLE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

//	public static void main(String[] args) throws IOException {
//		createAllTable();
//	}
}
