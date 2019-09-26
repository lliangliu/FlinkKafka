package com.test.flinktomysqlAndhbase;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.test.Utils.PropertyUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkSubmitter {
    private static String zkconnect= PropertyUtil.getProperty("kafka.zookeeper.connect");
    private static String kafkaservers=PropertyUtil.getProperty("kafka.bootstrap.servers");
    private static String keydeserializer=PropertyUtil.getProperty("kafka.key.serializer");
    private static String valuedeserializer=PropertyUtil.getProperty("kafka.value.serializer");
    private static String topicName=PropertyUtil.getProperty("kafka.topic.name");
    public static void main(String[] args) throws Exception{
        //获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //kafka配置文件
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaservers);
        props.put("zookeeper.connect",zkconnect);
        props.setProperty("group.id", "kafka13");
        props.setProperty("auto.offset.reset", "earliest");
        props.put("key.deserializer", keydeserializer);  //key 反序列化
        props.put("value.deserializer",valuedeserializer ); //value 反序列化
        //这里我们使用的是011版本，011 与 09 或者10 的区别在于，011支持Exactly-once语义
        SingleOutputStreamOperator<Entity> StreamRecord = env.addSource(new FlinkKafkaConsumer010<>(
                topicName,
                new SimpleStringSchema(),//String 序列
                props)).map(string -> JSON.parseObject(string, Entity.class)).setParallelism(1);
        StreamRecord.print();
        SingleOutputStreamOperator<List<Entity>> apply = StreamRecord.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Entity, List<Entity>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Entity> value, Collector<List<Entity>> out) throws Exception {
                ArrayList<Entity> entities = Lists.newArrayList(value);
                if (entities.size() > 0) {
                    System.out.println("1 分钟内收集到 student 的数据条数是：" + entities.size());
                    out.collect(entities);
                }
            }
        });
        //批量插入mysql
        apply.addSink(new MysqlBacthSink());

        //写入本地
        //StreamRecord.writeAsText("D:/flink");
        //写入Mysql
        //StreamRecord.addSink(new MysqlSink());
        //写入hbase
        //StreamRecord.addSink(new HbaseSink());
        //写入redis中
        env.execute("KafkatoMysql");

    }
}