package com.test.flinktoredis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Properties;

public class flinkKafkaWordCount {
    @Test
    public void run1() throws Exception{

        ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
        Long startTime=System.currentTimeMillis();
        DataSet<String> ds= env.readTextFile("D:/test/hello.log");
        Long endTime=System.currentTimeMillis();
        System.out.println("执行时长："+(startTime-endTime));
        ds.print();

    }
    @Test
    public void run2()throws Exception{
     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Long startTime=System.currentTimeMillis();
        DataStream<String> ds= env.readTextFile("D:/test/hello.log");
        Long endTime=System.currentTimeMillis();
        System.out.println("执行时长："+(startTime-endTime));
        ds.print();
        env.execute();
    }
    @Test
    public void run(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
        properties.setProperty("group.id","kafka08");
        properties.setProperty("auto.offset.reset","earliest");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "kafka_flink_redis", new SimpleStringSchema(), properties) );
        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new linesFilter())
                 .keyBy(0)
                 //.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) ////指定计算数据的窗口大小和滑动窗口大小
                 .sum(1);
        counts.print();
    //实例化Flink和Redis 关联类FlinkJedisPoolConfig,设置Redis端口
        FlinkJedisPoolConfig conf=new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.65.20").setPort(6379).build();
        //实例化RedisSink,并通过flink的addSink的方式将flink计算的结果插入到redis中
        counts.addSink(new RedisSink<Tuple2<String, Integer>>(conf,new RedisExampleMapper()));
        System.out.println("持久化到redis中成功");
//        sum.print();
//        sum.writeAsText("D:/test/kafka", FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute("flinkKafkaWordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  static final class linesFilter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] str = value.split(",");
            for (String string:str){
                if(string.length()>0){
                    out.collect(new Tuple2<>(string,1));
                }

            }

        }
    }

    public static final class RedisExampleMapper implements RedisMapper<Tuple2<String,Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"kafka-flink-redis");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}
