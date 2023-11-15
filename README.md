# 计算 hbase 表的一些 统计信息
# QuickStart
##  注册
修改emr集群， 添加如下配置：
```shell
<property>
   <name>hbase.coprocessor.user.region.classes</name>
   <value>org.apache.hadoop.hbase.coprocessor.AggregateImplementation</value>
</property>
```
打开hbase shell
```shell
sudo hbase shell
```

```给表注册coproccessor
disable 'your-table-name'
alter 'your-table-name', METHOD => 'table_att', 'coprocessor' => '|org.apache.hadoop.hbase.coprocessor.AggregateImplementation|1001|'
enable 'your-table-name'
```


## 运行代码
编译打包后，把jar包 （带有with-dependencies.jar）放到emr主节点上

运行
```shell
java -jar stathbase-0.0.1-jar-with-dependencies.jar your-table-name
```

## 注意
1. 代码编译环境最好和emr master环境一致，或者代码提交到emr master上执行