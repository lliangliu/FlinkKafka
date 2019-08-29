package com.test.flinktohbase;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.Properties;

public class FlinkHBase {

    private static String zkServer = "node01,node02,node03";
    private static String port = "2181";
    private static TableName tableName = TableName.valueOf("testflink");
    private static final String cf = "ke";
    private static final String topic = "kafka_flink_mysql";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> transction = env.addSource(new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), configByKafka()));
        transction.rebalance().map(new MapFunction<String, Object>() {
            private static final long serialVersionUID = 1L;

            public String map(String value) throws Exception {
                write2HBase(value);
                return value;
            }
        }).print();
        try {
            env.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static Properties configByKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("group.id", "kv_flink");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static void write2HBase(String value) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkServer);
        config.set("hbase.zookeeper.property.clientPort", port);
        config.set("zookeeper.znode.parent","/hbase/master");
        config.setInt("hbase.rpc.timeout", 30000);
        config.setInt("hbase.client.operation.timeout", 30000);
        config.setInt("hbase.client.scanner.timeout.period", 30000);

        Connection connect = ConnectionFactory.createConnection(config);
        Admin admin = connect.getAdmin();
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(cf)));
        }
        Table table = connect.getTable(tableName);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        Put put = new Put(Bytes.toBytes(date.getTime()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("test"), Bytes.toBytes(value));
        table.put(put);
        table.close();
        connect.close();
    }
}