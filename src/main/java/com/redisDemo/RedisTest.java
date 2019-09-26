package com.redisDemo;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Map;

public class RedisTest {
    public static void main(String[] args) {
        Jedis jedis=new Jedis("192.168.65.20");
        System.out.println("Server is running: " + jedis.ping());
        Map<String, String> map = jedis.hgetAll("kafka-flink-redis");
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, String> next = it.next();
             String key = next.getKey();
             String value=next.getValue();
            System.out.println("key:"+key+","+"value:"+value);
        }

    }
}
