package com.test.kafka.createData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CreateDataToKafka {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.200.10:9092");
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
            //flink 模块中scala.example.streaming.kafka.StreamingWindowWatermarkScala.scala 中的ti输入源
            ProducerRecord<String, String> record = new ProducerRecord<>("t1", i + "," + System.currentTimeMillis());
            // 只管发送消息，不管是否发送成功
            producer.send(record);
            Thread.sleep(300);
        }

    }
}
