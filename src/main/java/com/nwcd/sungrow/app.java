package com.nwcd.sungrow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.TableName;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hadoop.hbase.HConstants;

import org.json.JSONObject;

public class app {

    public static void main(String[] args) {
        try {
            String filename = args[0];

            // 获取项目根目录的路径
            Path currentPath = Paths.get(System.getProperty("user.dir"));

            // 构建相对路径，假设JSON文件在项目根目录下的"data"文件夹中，文件名为"example.json"
            Path filePath = currentPath.resolve(filename);
            System.out.println(filePath);

            String jsonContent = readJsonFile(filePath.toString());
            // 解析JSON内容
            JSONObject jsonObject = new JSONObject(jsonContent);

            String table = jsonObject.getString("table");
            System.out.println("set table: " + table);
            String cmd = jsonObject.getString("cmd");
            System.out.println("set cmd: " + cmd);
            String column = jsonObject.getString("column");
            System.out.println("set column: " + column);

            String from = jsonObject.getString("from");
            String to = jsonObject.getString("to");
            System.out.println("to: " + from);
            System.out.println("to: " + to);

            // 配置
            Configuration conf = HBaseConfiguration.create();
            conf.setInt("hbase.client.operation.timeout", 60000000); // 操作超时时间，单位毫秒
            conf.setInt("hbase.client.scanner.timeout.period", 60000000); // 扫描超时时间，单位毫秒
            conf.setInt("hbase.client.retries.number", 3); // 重试次数

            // 创建 AggregationClient
            AggregationClient aggregationClient = new AggregationClient(conf);

            // HBase 表名
            TableName tableName = TableName.valueOf(table);

            // 创建 Scan 对象
            Scan scan = new Scan();
            if (!from.equals("") && !to.equals("")) {
                long startTime = getTimeStamp(from);
                long endTime = getTimeStamp(to);
                System.out.println("Total startTime: " + startTime);
                System.out.println("Total endTime: " + endTime);
                scan.setTimeRange(startTime, endTime);
            }
            // scan.setTimeRange(1700016800000L, 1700016872379L);
            // 列解释器，用于指定聚合操作的列类型
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();

            long begin = System.currentTimeMillis();
            System.out.println("begin at " + begin);

            long rowCount = aggregationClient.rowCount(tableName, columnInterpreter, scan);

            // 打印结果
            System.out.println("Total row count: " + rowCount);
            long currentTimestamp = System.currentTimeMillis();
            long seconds = (currentTimestamp - begin) / 1000;
            System.out.println("time cost(seconds):" + seconds);
            aggregationClient.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private static String readJsonFile(String filePath) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
        }
        return content.toString();
    }

    private static long getTimeStamp(String dateString) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse(dateString);
            return date.getTime() - 8 * 60 * 60 * 1000;
        } catch (Exception e) {
            throw new RuntimeException("Error parsing date: " + dateString, e);
        }
    }

}