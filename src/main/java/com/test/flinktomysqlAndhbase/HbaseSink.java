package com.test.flinktomysqlAndhbase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HbaseSink extends RichSinkFunction<Entity> {
    private static final Logger logger = LoggerFactory.getLogger(HbaseSink.class);
    private Connection conn = null;
    private Table table = null;
    private static String zkServer = "node01,node02,node03";
    private static String port = "2181";
    private static  String cf = "info";
    private static TableName tableName = TableName.valueOf("hbaseflink");


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkServer);
        config.set("hbase.zookeeper.property.clientPort", port);
        config.set("zookeeper.znode.parent","/hbase/master");
        conn= ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
    }

    @Override
    public void invoke(Entity value, Context context) throws Exception {
        List<Put> list=new ArrayList<>();
        String rowKey=value.os+"_"+value.phoneName;
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("city"),Bytes.toBytes(value.city));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("loginTime"),Bytes.toBytes(value.loginTime));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("os"),Bytes.toBytes(value.os));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("phoneName"),Bytes.toBytes(value.phoneName));
        list.add(put);
        table.put(list);
    }

    @Override
    public void close() throws Exception {
        super.close();
        table.close();
        conn.close();
    }

}
