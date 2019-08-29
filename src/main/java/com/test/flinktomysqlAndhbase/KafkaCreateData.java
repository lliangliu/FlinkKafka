package com.test.flinktomysqlAndhbase;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Producer代码，模拟从java生产数据至Kafka Topic
 */
public class KafkaCreateData {
    public static String topic = "kafka_flink_mysql";
    public static String brokerList = "node01:9092";
    public static void createDate(){
        Entity entity = new Entity();
        Properties props = new Properties();
        //声明Kakfa相关信息
        props.put("bootstrap.servers",brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        props.put("request.required.acks", "1");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

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
        //加载数据到实体中
        entity.setCity(city);
        entity.setLoginTime(loginTime);
        entity.setOs(os);
        entity.setPhoneName(phoneName);
        ProducerRecord record = new ProducerRecord<String, String>(topic,JSON.toJSONString(entity));
        producer.send(record);
        System.out.println("发送数据："+ JSON.toJSONString(entity));
    }

    public static void main(String[] args) throws  InterruptedException{
        while (true){
            createDate();
            Thread.sleep(500);
        }
    }
}