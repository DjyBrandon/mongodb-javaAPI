import com.mongodb.Block;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * @package: src
 * @Description:
 * @author: Brandon
 * @date: 2023/4/10 15:43
 **/
public class law {
    // Name of the column for which data is to be obtained
    static String[] hear = {
            "id", "realIP", "realAreacode", "userAgent", "userOS",
            "userID", "clientID", "timestamps", "timestamp_format",
            "pagePath", "ymd", "fullURL", "fullURLID", "hostname",
            "pageTitle", "pageTitleCategoryId", "pageTitleCategoryName",
            "pageTitleKw", "fullReferrrer", "FullReferrerURL", "organicKeyword", "source"
    };

    static Map<String, Integer> sum_map = new HashMap();

    public static void main(String[] args) throws IOException {
        // Create a connection
        MongoClient client = MongoClients.create("mongodb://192.168.1.134:27017");
        // Open database test
        MongoDatabase db = client.getDatabase("test");
        // Obtain set law
        MongoCollection<Document> collection = db.getCollection("law");
        // 打印函数
        Block<Document> printBlock = new Block<Document>() {
            public void apply(final Document document) {
                String day = document.toJson().substring(11, 19);
                String month = document.toJson().substring(11, 17);
                int count = Integer.valueOf(document.toJson().substring(33, document.toJson().length() - 1));
                System.out.println("日期：" + day + " 访问流量数：" + count);
                if (!sum_map.containsKey(month)) {
                    sum_map.put(month, count);
                } else {
                    sum_map.put(month, sum_map.get(month) + count);
                }
            }
        };

        // 打印函数2
        Block<Document> printBlock2 = new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };

        // 批量插入数据
        insertMany(collection, "resources/lawtime_one1.csv");

        // 查询每月及每日访问流量
        GroupAggregation(collection, printBlock, "$ymd");

        // 打印结果
        for (Map.Entry<String, Integer> entry : sum_map.entrySet()) {
            System.out.println("月份：" + entry.getKey() + " 访问流量数" + entry.getValue());
        }

        // 查询每个用户的访问记录数
        GroupAggregation(collection, printBlock2, "$userID");

        // 修改用户日志访问日期
        update(collection);

        // 修改完成后再次统计每月访问流量
        GroupAggregation(collection, printBlock2, "$ymd");


        // 关闭连接
        client.close();
    }



    /*
     批量插入数据
    */
    public static void insertMany(MongoCollection<Document> collection, String inputPath) throws IOException {

        // TODO准备插入数据
        ArrayList<Document> documents = new ArrayList<Document>();
        Document document;

        // TODO读取本地文件
        FileReader fr = new FileReader(inputPath);
        BufferedReader br = new BufferedReader(fr);
        String line = "";
        int x = 0;
        int y = 0;
        while ((line = br.readLine()) != null) {
            if (line.split(",").length != hear.length) {
                y++;
                continue;
            }
            document = new Document();
            for (int i = 0; i < line.split(",").length; i++) {
                document.append(hear[i], line.split(",")[i]);
            }
            documents.add(document);
            x++;
        }
        System.out.println("删除了" + y + "条异常数据");
        System.out.println("总共插入" + x + "条数据");
        collection.insertMany(documents);
        br.close();
    }

    /*
    分组聚合
    */
    public static void GroupAggregation(MongoCollection<Document> collection, Block<Document> printBlock, String id) {
        // TODO 分组聚合
        collection.aggregate(
                Arrays.asList(
                        // TODO求和（若想统计则将expression设置为1）
                        Aggregates.group(id, Accumulators.sum("count", 1))
                )
        ).forEach(printBlock);
    }

    /*
     修改数据
     */
    public static void update(MongoCollection<Document> collection) {
        // 先将id字段由string类型转变为Int类型
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            String val = document.getString("id");
            String sub_val = val.substring(val.indexOf("\"") + 1, val.lastIndexOf("\""));
            collection.updateOne(regex("id", sub_val), combine(set("id", Integer.valueOf(sub_val))));
        }
        collection.updateMany(lt("id", 10), new Document("$set", new Document("ymd", "201502")));
        collection.updateMany(gte("id", 10), new Document("$set", new Document("ymd", "201503")));
        collection.updateMany(gt("id", 20), new Document("$set", new Document("ymd", "201504")));
    }

}
