package com.test.flinktomysqlAndhbase;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import java.util.Properties;

public class FlinkSubmitter {
    public static void main(String[] args) throws Exception{
        //获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //kafka配置文件
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");
        props.put("zookeeper.connect","node01:2181");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化
        //这里我们使用的是011版本，011 与 09 或者10 的区别在于，011支持Exactly-once语义
        SingleOutputStreamOperator<Entity> StreamRecord = env.addSource(new FlinkKafkaConsumer010<>(
                "kafka_flink_mysql",
                new SimpleStringSchema(),//String 序列
                props)).map(string -> JSON.parseObject(string, Entity.class)).setParallelism(1);
        //写入本地
        //StreamRecord.writeAsText("D:/flink");
        //写入Mysql
        //StreamRecord.addSink(new MysqlSink());
        //写入hbase
        StreamRecord.addSink(new HbaseSink());
        env.execute("KafkatoMysql");

    }
}