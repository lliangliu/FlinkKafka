package com.test.windowWaterMark;

import org.apache.flink.api.common.functions.FlatMapFunction;;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 窗口内分组聚合：计算10秒中内各个单词的总数
 */
public class GruopWC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //连接socket
        DataStreamSource<String> textStream = env.socketTextStream("node01", 4000, "\n");
        SingleOutputStreamOperator<WordWithCount> windowCount = textStream.flatMap(new FlatMapFunction<String, WordWithCount>() {

            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] str = value.split(",");
                for (String count : str) {
                    out.collect(new WordWithCount(count, 1L));
                }
            }

        })
        .keyBy("word")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //.countWindow(3)
                .sum("count");
        windowCount.print();
        try {
            env.execute("streaming word count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word,long count){
            this.word=word;
            this.count=count;
        }

        @Override
        public String toString() {
            return " "+word+", :"+count;
        }
    }
}
