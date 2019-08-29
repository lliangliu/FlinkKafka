package com.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;

/*
案例一：求五秒钟内每个班级最高的分数
使用reduce算子计算最大值
 */
public class windowsTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> res = env.readTextFile("D:/score.txt");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final SingleOutputStreamOperator<Row> strteam = res.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                String timeStamp = split[0];
                String name = split[1];
                int score = Integer.parseInt(split[2]);
                Row row = new Row(3);
                row.setField(0, timeStamp);
                row.setField(1, name);
                row.setField(2, score);
                return row;

            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
            long currentMaxTimestamp = 0L;
            long maxOutOfOrderness = 10000L;
            Watermark watermark = null;

            //最大允许的乱序时间是10s
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                return watermark;
            }

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {
                long timeStamp = 0;
                try {
                    timeStamp = simpleDateFormat.parse(element.getField(0).toString()).getDate();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                return timeStamp;
            }

        });
        strteam.keyBy(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) throws Exception {
                return value.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Row>() {
                    @Override
                    public Row reduce(Row value1, Row value2) throws Exception {
                        String s1=value1.getField(2).toString();
                        String s2=value1.getField(2).toString();
                        if(Integer.valueOf(s1)<Integer.valueOf(s2)){
                            return value2;
                        }else {
                            return value1;
                        }

                    }
                }).print();
        try {
            env.execute("windowsTest");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
