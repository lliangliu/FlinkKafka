package com.test.windowWaterMark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 案例一:求五秒钟内每个班级最高的分数
 * 数据准备：时间戳，班级名，分数
 */
public class WindowsTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> text = env.readTextFile("D:/score.txt");
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SingleOutputStreamOperator<Row> map = text.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] str = value.split(",");
                String timeStamp = str[0];
                String name = str[1];
                int score = Integer.parseInt(str[2]);
                Row row = new Row(3);
                row.setField(0, timeStamp);
                row.setField(1, name);
                row.setField(2, score);
                return row;
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
            long currentMaxTimestamp=0L;
            long maxOutOfOrderness=10000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {

                return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {
                long timeSatmp=0L;
                try {
                    timeSatmp=sdf.parse(element.getField(1).toString()).getDate();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                currentMaxTimestamp=Math.max(currentMaxTimestamp,timeSatmp);
                return timeSatmp;
            }
        });
        map.keyBy(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) throws Exception {
                return value.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Row>() {
                    @Override
                    public Row reduce(Row value1, Row value2) throws Exception {
                        String s1 = value1.getField(2).toString();
                        String s2 = value2.getField(2).toString();
                        if(Integer.parseInt(s1)<Integer.parseInt(s2)){
                            return value2;
                        }else {
                            return value1;
                        }
                    }
                }).print();
        try {
            env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
