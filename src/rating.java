import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.lang.Integer.parseInt;


/**
 * @package: src
 * @Description:
 * @author: Brandon
 * @date: 2023/4/10 15:43
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
        MongoCollection<Document> collection = db.getCollection("ratings");

        // 打印函数
        Block<Document> printBlock = document -> {
            HashMap map = new Gson().fromJson(document.toJson(), HashMap.class);
            int movieID = (int) Math.round((Double) map.get("_id"));
            double avgRating = (Double) map.get("avgRating");
            System.out.println("电影编号：" + movieID + " 平均评分：" + avgRating);
        };

        // 批量插入数据
//        insertMany(collection, "resources/rating.txt");

        // 查询每部电影用户平均评分
        GroupAggregation(collection, printBlock, "$movieID");

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
        while ((line = br.readLine()) != null) {
            // 如果某一行数据分割后长度与指定的mongodb字段长度（hear中有4个字段）不匹配，则不插入这一条document
            if (line.split("::").length != hear.length) {
                y++;
                continue;
            }
            document = new Document();
            // 如果某一行数据分割后长度与指定的mongodb字段长度（hear中有4个字段）匹配，则插入这一条document
            for (int i = 0; i < line.split("::").length; i++) {
                document.append(hear[i], parseInt(line.split("::")[i]));
            }
            documents.add(document);
            x++;
        }
        // 数据集中是否含有异常数据
        System.out.println("删除了" + y + "条异常数据");
        System.out.println("总共插入" + x + "条数据");
        collection.insertMany(documents);
        br.close();
    }

    /*
    分组聚合
    */
    public static void GroupAggregation(MongoCollection<Document> collection, Block<Document> printBlock, String id) {
        // 分组聚合
        // db.ratings.aggregate([{$group:{_id:"$movieID",ratingAvg:{$avg:"$rating"}}}])
        collection.aggregate(
                Arrays.asList(
                        // 求和（若想统计则将expression设置为1）
                        Aggregates.group(id, Accumulators.avg("avgRating", "$rating")),
                        Aggregates.sort(Sorts.descending("avgRating"))
                )
        ).forEach(printBlock);
    }

}
