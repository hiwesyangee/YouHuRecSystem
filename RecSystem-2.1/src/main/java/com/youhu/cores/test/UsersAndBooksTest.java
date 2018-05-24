package com.youhu.cores.test;

import com.youhu.cores.properties.RecSystemProperties;
import com.youhu.cores.utils.HBaseUtils;

import java.io.IOException;

// 测试：直接读取文件批处理生成用户数据和书籍数据
public class UsersAndBooksTest {
    public static void main(String[] args) {
        // 批处理书籍数据
        String booksTable = RecSystemProperties.BOOKSINFOTABLE;
        String[] cfsOfBooksTable = RecSystemProperties.cfsOfBOOKSINFOTABLE;
        String[] columnsOfBooksTable = RecSystemProperties.columnsOfBOOKSINFOTABLE;
        try {
            HBaseUtils.getInstance().createTable(booksTable, cfsOfBooksTable);
            HBaseUtils.getInstance().batchTable("/Users/hiwes/data/book.dat", booksTable, cfsOfBooksTable[0], columnsOfBooksTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 批处理用户数据
        String usersTable = RecSystemProperties.USERSINFOTABLE;
        String[] cfsOfUsersTable = RecSystemProperties.cfsOfUSERSINFOTABLE;
        String[] columnsOfUsersTable = RecSystemProperties.columnsOfUSERSINFOTABLE;
        try {
            HBaseUtils.getInstance().createTable(usersTable, cfsOfUsersTable);
            HBaseUtils.getInstance().batchTable("/Users/hiwes/data/user.dat", usersTable, cfsOfUsersTable[0], columnsOfUsersTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
