package com.test.windowWaterMark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
//参考连接 https://blog.csdn.net/xu470438000/article/details/83271123
/**
 * 1 接收socket数据
 * 2 将每行数据按照逗号分隔，每行数据调用map转换成tuple<String,Long>类型。其中tuple中的第一个元素代表具体的数据，第二行代表数据的eventtime
 * 3 抽取timestamp，生成watermar，允许的最大乱序时间是10s，并打印（key,eventtime,currentMaxTimestamp,watermark）等信息
 * 4 分组聚合，window窗口大小为3秒，输出（key，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
 */
public class StreamingWindowWatermark {
    public static void main(String[] args)throws Exception {
        //定义主机号
        String host = "node1";
        //定义socket的端口号
        int port = 4000;
        //定义分割符
        String splitChart = "\n";
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度位1，默认并行度是当前机器cpu的数量
        env.setParallelism(1);

        //连接socket获取输入数据
        DataStreamSource<String> text = env.socketTextStream(host, port, splitChart);
        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] str = value.split(",");

                return new Tuple2<>(str[0], Long.parseLong(str[1]));
            }
        });
        //抽取timestamp和生成watermark
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10s
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义生成watermark的逻辑
             * 默认100ms被调用一次
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
                StringBuffer sb=new StringBuffer();
                sb.append("key:").append(element.f0).append(",eventTime:[").append(element.f1)
                        .append("|").append(sdf.format(element.f1)).append("],currentMaxTimestamp:[")
                        .append(currentMaxTimestamp).append("|").append(sdf.format(currentMaxTimestamp))
                        .append("],watermark:[").append(getCurrentWatermark().getTimestamp()).append("|")
                        .append(sdf.format(getCurrentWatermark().getTimestamp())).append("]");
                 String res = sb.toString();
                System.out.println(res);
//                System.out.println("key:" + element.f0 + ",eventTime:[" + element.f1 + "|" + sdf.format(element.f1) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
//                        sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                return timestamp;
            }
        });
        OutputTag<Tuple2<String,Long>> outputTag=new OutputTag<Tuple2<String,Long>>("late-data"){};
        //分组 聚合
       // DataStream<String> window = waterMarkStream.keyBy(0) todo 修改侧输出
        SingleOutputStreamOperator<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口,和调用TimeWindow效果一样
                //.allowedLateness(Time.seconds(2)) //todo 允许数据迟到2s
                //.sideOutputLateData(outputTag) //todo 侧输出
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> list = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            list.add(next.f1);
                        }
                        Collections.sort(list);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        StringBuffer sb = new StringBuffer();
                        String separator = ",";
                        sb.append(key).append(separator)
                                .append(list.size()).append(separator)
                                .append(sdf.format(list.get(0))).append(separator)
                                .append(sdf.format(list.get(list.size() - 1))).append(separator)
                                .append(sdf.format(timeWindow.getStart())).append(separator)
                                .append(sdf.format(timeWindow.getEnd()));
                        String res = sb.toString();
                        //key，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间
                        out.collect(res);

                    }
                });
        //把迟到的数据暂时打印到控制台,实际上可以保存到其他存储介质中
         DataStream<Tuple2<String, Long>> sideOutput = window.getSideOutput(outputTag);
         sideOutput.print();
        //测试 把结果打印到控制台即可

        window.print();

        //flink懒加载 需要调用execute方法 执行上面的代码
        env.execute("event-watermark");

    }
}
