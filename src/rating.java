import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.io.*;
import java.util.*;

import static java.lang.Integer.parseInt;


/**
 * @package: src
 * @Description: 电影评分数据分析任务：电影平均评分，用户观看电影部数，电影观看人数
 * @author: Brandon
 * @date: 2023/4/30 10:37
 **/
public class rating {

    // Name of the column for which data is to be obtained
    static String[] hear = {"userID", "movieID", "rating", "timestamp"};

    public static void main(String[] args) throws IOException {

        // 创建数据库服务连接对象 MongoClient 无密码
        MongoClient client = MongoClients.create("mongodb://192.168.1.135:27017");
        // 设置数据库连接信息 mongoClient.getDatabase(数据库名)
        // 注： 如果数据库名不存在，会在第一次插入文档时创建
        MongoDatabase db = client.getDatabase("test");
        // 通过数库连接对象获取集合 mongoDatabase.getCollection(集合名称);
        // 判断集合是否存在来导入数据
        boolean isCollectionExists = db.listCollectionNames().into(new ArrayList()).contains("ratings");;
        MongoCollection<Document> collection = db.getCollection("ratings");

        // 批量插入数据
        if (isCollectionExists == false) {
            System.out.println("开始导入数据···");
            insertMany(collection, "resources/rating.txt");
        } else {
            System.out.println("已导入数据，直接开始数据分析。");
        }

        // 打印电影平均评分函数
        Block<Document> MovieAvgRatingBlock = document -> {
            // 将分组聚合统计平均分结果写入HashMap集合result中
            HashMap result = new Gson().fromJson(document.toJson(), HashMap.class);
            int movieID = (int) Math.round((Double) result.get("_id"));
            double avgRating = (Double) result.get("avgRating");
            String movieAvgRating = "电影编号：" + movieID + " 平均评分：" + avgRating + "\n";
            try {
                bufferedWriterMethod("resources/MovieAvgRating.txt", movieAvgRating);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(movieAvgRating);
        };

        // 打印用户观看电影部数函数
        Block<Document> UserCountBlock = document -> {
            // 将分组聚合统计用户观看电影部数结果写入HashMap集合result中
            HashMap result = new Gson().fromJson(document.toJson(), HashMap.class);
            int userID = (int) Math.round((Double) result.get("_id"));
            int count = (int) Math.round((Double) result.get("userCount"));
            String userCount = "用户编号：" + userID + " 观看电影部数：" + count + "\n";
            try {
                bufferedWriterMethod("resources/UserByMovieCount.txt", userCount);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(userCount);
        };

        // 打印电影观看用户人数函数
        Block<Document> MovieCountBlock = document -> {
            // 将分组聚合统计电影观看用户人数结果写入HashMap集合result中
            HashMap result = new Gson().fromJson(document.toJson(), HashMap.class);
            int movieID = (int) Math.round((Double) result.get("_id"));
            int count = (int) Math.round((Double) result.get("movieCount"));
            String movieCount = "电影编号：" + movieID + " 观看用户人数：" + count + "\n";
            try {
                bufferedWriterMethod("resources/MovieByUserCount.txt", movieCount);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(movieCount);
        };

        // 查询每部电影用户平均评分
        MovieAvgRating(collection, MovieAvgRatingBlock, "$movieID");

        // 用户观看电影部数
        UserByMovieCount(collection, UserCountBlock, "$userID");

        // 电影观看用户胡人数
        MovieByUserCount(collection, MovieCountBlock, "$movieID");

        // 关闭连接
        client.close();
    }


    // 批量插入数据
    public static void insertMany(MongoCollection<Document> collection, String inputPath) throws IOException {

        // 准备插入数据
        ArrayList<Document> documents = new ArrayList<>();
        // Document 接口表示整个 HTML 或 XML 文档。从概念上讲，它是文档树的根，并提供对文档数据的基本访问。
        Document document;

        // 读取本地文件
        FileReader fr = new FileReader(inputPath);
        BufferedReader br = new BufferedReader(fr);
        String line = "";
        int x = 0, y = 0;

        // 这一行有数据就继续读取，知道行为空停止插入数据
        while ((line = br.readLine()) != null) {
            // 如果某一行数据分割后长度与指定的mongodb字段长度（hear中有4个字段）不匹配，则不插入这一条document
            if (line.split("::").length != hear.length) {
                // y 值记录异常记录条数
                y++;
                continue;
            }
            document = new Document();
            // 如果某一行数据分割后长度与指定的mongodb字段长度（hear中有4个字段）匹配，则插入这一条document，全部转换为int便于求平均评分
            for (int i = 0; i < line.split("::").length; i++) {
                document.append(hear[i], parseInt(line.split("::")[i]));
            }
            documents.add(document);
            x++;
        }

        // 数据集中是否含有异常数据
        System.out.println("删除了" + y + "条异常数据");
        System.out.println("总共插入" + x + "条数据");
        System.out.println("导入数据完成，可以开始数据分析。");
        collection.insertMany(documents);
        br.close();
    }

    // 分组聚合电影平均评分
    // db.ratings.aggregate([{$group:{_id:"$movieID",avgRating:{$avg:"$rating"}}},{$sort:{"avgRating":-1}}])
    public static void MovieAvgRating(MongoCollection<Document> collection, Block<Document> MovieAvgRatingBlock, String id) {
        collection.aggregate(
                Arrays.asList(
                        // 求和（若想统计则将expression设置为1）
                        Aggregates.group(id, Accumulators.avg("avgRating", "$rating")),
                        Aggregates.sort(Sorts.descending("avgRating"))
                )
        ).forEach(MovieAvgRatingBlock);
    }

    // 分组聚合用户观看电影部数
    // db.ratings.aggregate([{$group:{_id:"$userID",userCount:{$sum:1}}},{$sort:{"userCount":-1}}])
    public static void UserByMovieCount(MongoCollection<Document> collection, Block<Document> UserCountBlock, String id) {
        collection.aggregate(
                Arrays.asList(
                        // 求和（若想统计则将expression设置为1）
                        Aggregates.group(id, Accumulators.sum("userCount", 1)),
                        Aggregates.sort(Sorts.descending("userCount"))
                )
        ).forEach(UserCountBlock);
    }

    // 分组聚合电影观看用户人数
    // db.ratings.aggregate([{$group:{_id:"$movieID",movieCount:{$sum:1}}},{$sort:{"movieCount":-1}}])
    public static void MovieByUserCount(MongoCollection<Document> collection, Block<Document> MovieCountBlock, String id) {
        collection.aggregate(
                Arrays.asList(
                        // 求和（若想统计则将expression设置为1）
                        Aggregates.group(id, Accumulators.sum("movieCount", 1)),
                        Aggregates.sort(Sorts.descending("movieCount"))
                )
        ).forEach(MovieCountBlock);
    }

    // 将数据分析结果保存至HashMap键值对集合result后写入txt文件
    public static void bufferedWriterMethod(String filepath, String content) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath, true))) {
            bufferedWriter.write(content);
        }
    }
}
