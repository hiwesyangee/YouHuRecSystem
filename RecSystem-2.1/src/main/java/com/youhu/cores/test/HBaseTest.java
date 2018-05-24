package com.youhu.cores.test;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.HBaseUtils;

import java.io.IOException;

// 测试：读取HBase的书籍推荐表中一位用户的数据
public class HBaseTest {
    public static void main(String[] args) throws IOException {
        long start1 = System.currentTimeMillis();
        String result1 = HBaseUtils.getInstance().getValue(RecSystemProperties.BOOKS_RECTABLE,"1",RecSystemProperties.cfsOfBOOKS_RECTABLE[0],RecSystemProperties.columnsOfBOOKS_RECTABLE[0]);
        long stop1 = System.currentTimeMillis();
        System.out.println((stop1-start1) * 1.0 +"ms");
        System.out.println(result1);
        long start2 = System.currentTimeMillis();
        String result2 = HBaseUtils.getInstance().getValue(RecSystemProperties.BOOKS_RECTABLE,"1",RecSystemProperties.cfsOfBOOKS_RECTABLE[0],RecSystemProperties.columnsOfBOOKS_RECTABLE[0]);
        long stop2 = System.currentTimeMillis();

        System.out.println((stop2-start2) * 1.0 +"ms");
        System.out.println(result2);
    }
}
