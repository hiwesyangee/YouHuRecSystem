package com.youhu.cores.properties;

/**
 * @ClassName: RecSystemProperties
 * @Description: 推荐系统常数类
 * @author: hiwes
 * @date: 2018年5月22日
 * @Version: 2.1
 */
public class RecSystemProperties {
    // 用户表相关
    public static final String USERSINFOTABLE = "usersInfoTable";
    public static final String[] cfsOfUSERSINFOTABLE = {"info"};
    public static final String[] columnsOfUSERSINFOTABLE = {"nickName", "gender", "age"}; // 昵称，性别，年龄

    // 书籍表相关
    public static final String BOOKSINFOTABLE = "booksInfoTable";
    public static final String[] cfsOfBOOKSINFOTABLE = {"info"};
    public static final String[] columnsOfBOOKSINFOTABLE = {"bookName", "author", "class"}; // 书名，作者，类别

//	// 榜单表相关
//	public static final String RANKLISTTABLE = "rankListTable";
//	public static final String[] cfsOfRANKLISTTABLE = { "info" };
//	public static final String[] columnsOfRANKLISTTABLE = { "rec", "new", "girls", "sinyee" }; // 推荐榜，新书榜，女频榜，星艺榜__row:rankID_时间，如：1_20180521

    // 评分表相关
    public static final String RATINGSTABLE = "ratingsTable";
    public static final String[] cfsOfRATINGSTABLE = {"info"};
    public static final String[] columnsOfRATINGSTABLE = {"rating"}; // 评分

    // 用户行为数据相关——关于用户的浏览或者收藏等
    public static final String USERACTIONSTABLE = "usersActionTable";
    public static final String[] cfsOfUSERACTIONSTABLE = {"action"};
    public static final String[] columnsOfUSERACTIONSTABLE = {"browse", "collect"}; // 浏览，收藏__row:用户ID_时间，如：00001_20180522

    // 书籍推荐表相关
    public static final String BOOKS_RECTABLE = "books_recTable";
    public static final String[] cfsOfBOOKS_RECTABLE = {"rec"};
    public static final String[] columnsOfBOOKS_RECTABLE = {"rank", "rating", "friends", "books"};// 根据榜单、评分、用户相似度、书籍相似度进行书籍推荐

    // 好友推荐表相关
    public static final String FRIENDS_RECTABLE = "friends_recTable";
    public static final String[] cfsOfFRIENDS_RECTABLE = {"rec"};
    public static final String[] columnsOfFRIENDS_RECTABLE = {"info", "friends"};// 根据基本信息、用户相似度进行好友推荐

//    //推荐表
//    public static final String RECTABLE = "recTable";
//    public static final String[] cfsRECTABLE = {"rec"};
//    public static final String[] columnsOfRECTABLE = {"recBooks", "recFriends"};

    // 用户类簇推荐表相关
    public static final String USERCLUSTERS_RECTABLE = "userClusters_recTable";
    public static final String[] cfsOfUSERCLUSTERS_RECTABLE = {"rec"};
    public static final String[] columnsOfUSERCLUSTERS_RECTABLE = {"recFriends", "recBooks"}; // 推荐好友，推荐书籍

    // 书籍类簇推荐表相关
//	public static final String BOOKCLUSTERS_RECTABLE = "userClusters_recTable";
//	public static final String[] cfsOfBOOKCLUSTERS_RECTABLE = { "rec" };
//	public static final String[] columnsOfBOOKCLUSTERS_RECTABLE = { "recClass", "recSimilar" }; // 推荐同类别，推荐类似

    // ALS模型存储位置
//	public static final String ALS_MODEL = "hdfs://hy:8020/opt/model/ALSModel";

    // KMeans模型存储位置
    public static final String KMEANS_MODEL = "hdfs://master:9000/opt/model/KMeansModel";

    // 书籍监控目录
    public static final String BOOKSPATH = "file:///opt/data/transData/booksData";

    // 用户监控目录
    public static final String USERSPATH = "file:///opt/data/transData/usersData/";

    // 评分监控目录
    public static final String RATINGSPATH = "file:///opt/data/transData/ratingsData/";

    // 榜单监控目录
//	public static final String RANKLISTSPATH = "file:///opt/data/transData/rankListsData/";

    // 用户收藏监控目录
    public static final String COLLECTSPATH = "file:///opt/data/transData/collectsData/";

    // 用户浏览监控目录
    public static final String BROWSESPATH = "file:///opt/data/transData/browsesData/";

    // Kafka相关
    public static final String BROKER_LIST = "hiwes:9092";
    public static final String TOPIC = "recsystem";
    public static final String GROUP = "kafka-streaming-group";
    public static final String ZK_QUORUM = "hiwes:2181";
}
