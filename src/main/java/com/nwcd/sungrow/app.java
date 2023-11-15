package com.nwcd.sungrow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.TableName;


public class app {

    public static void main(String[] args) {
        try {
            String strTb = args[0];
            // 配置
            Configuration conf = HBaseConfiguration.create();

            // 创建 AggregationClient
            AggregationClient aggregationClient = new AggregationClient(conf);

            // HBase 表名
            TableName tableName = TableName.valueOf(strTb);

            // 创建 Scan 对象
            Scan scan = new Scan();

            // 列解释器，用于指定聚合操作的列类型
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();

            // 执行 rowCount 聚合操作
            long rowCount = aggregationClient.rowCount(tableName, columnInterpreter, scan);

            // 打印结果
            System.out.println("Total row count: " + rowCount);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


}