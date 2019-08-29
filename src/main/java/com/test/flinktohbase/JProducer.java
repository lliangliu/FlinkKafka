package com.test.flinktohbase;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class JProducer extends Thread {

    public static final String topic = "flink_topic";
    public static String brokerList = "node01:9092,node02:9092,node03:9092";
    public static void createDate(){
        Properties props = new Properties();
        //声明Kakfa相关信息
        props.put("bootstrap.servers",brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        props.put("request.required.acks", "1");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        for (int i = 300; i < 400; i++) {
            String json = "{\"id\":" + i + ",\"ip\":\"192.168.0." + i + "\",\"date\":" + new Date().toString() + "}";
            String k = "key" + i;
            System.out.println("发送数据"+json);

            producer.send(new ProducerRecord<String, String>("flink_topic", k, json));
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws  InterruptedException{
        createDate();
    }

}