package com.test.kafka.createData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class CreateDataToKafka {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.65.20:9092");
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384); // 16K
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432); // 32M
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        int i = 0;
        while (true) {
            i++;
            //手机信号
            String phoneArray[] = {"iPhone", "HUAWEI", "xiaomi", "moto", "vivo"};
            //操作系统
            String osArray[] = {"Android 7.0", "Mac OS", "Apple Kernel", "Windows","kylin OS","chrome"};
            //城市
            String cityArray[] = {"北京","上海","杭州","南京","西藏","西安","合肥","葫芦岛"};
            //随机产生一个手机型号
            int k = (int) (Math.random() *5);
            String phoneName = phoneArray[k];
            //随机产生一个os
            int m = (int) (Math.random() *6);
            String os = osArray[m];
            //随机产生一个城市地点
            int n = (int) (Math.random() *8);
            String city = cityArray[n];
            //时间戳，存当前时间
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String loginTime = sf.format(new Date());
            StringBuffer sb=new StringBuffer();
            String res = sb.append(i).append(",").append(phoneName).append(",").append(os).append(",").append(city).append(",").append(loginTime).toString();
            //flink 模块中scala.example.streaming.kafka.StreamingWindowWatermarkScala.scala 中的ti输入源
            ProducerRecord<String, String> record = new ProducerRecord<>("flinktest", res);
            //kafka-console-consumer.sh --bootstrap-server node01:9092,node02:9092,node03:9092 --topic flink_test
            // 只管发送消息，不管是否发送成功
            producer.send(record);
            //Thread.sleep(300);
        }

    }
}
