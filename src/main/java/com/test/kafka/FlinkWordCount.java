package com.test.kafka;

import com.test.flinktoredis.flinkKafkaWordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkWordCount {
//4423038
    public static void main(String[] args)throws Exception{
        long startTime=System.currentTimeMillis();
        System.out.println("程序开始执行时间:"+startTime);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(1000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
        properties.setProperty("group.id","kafka05");
        properties.setProperty("auto.offset.reset","earliest");
        //properties.setProperty("state.backend.fs.checkpointdir","hdfs://node01:8020/flink-checkpoints");
        long endTime=System.currentTimeMillis();
        //System.out.println("****88888888888888888**********************************");
        System.out.println("执行时长:"+(endTime-startTime));
        //System.out.println("****************************************************");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "flinktest", new SimpleStringSchema(), properties) );

//        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                //System.out.println("单条数据："+value);
//                String[] str = value.split(",");
//                for(String s:str){
//                    out.collect(new Tuple2<>(s,1));
//
//                }
//
//            }
//        })
//                .keyBy(0)
//                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) ////指定计算数据的窗口大小和滑动窗口大小
//                .sum(1);
//        counts.print();
        stream.print();

        env.execute("FlinkWordCount");


    }
}
