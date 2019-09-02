package com.test.flinkBroadCast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 * 第一步:封装DataSet,调用withBroadcastSet
 * 第二步:getRuntimeContext().getBroadcastVariable,获得广播变量
 * 第三步:RichMapFunction中执行获得广播变量的逻辑
 */
public class BatchDemoBroadcast {
    
    public static void main(String[] args)throws Exception {
        //获得运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备需要广播的数据
        ArrayList<Tuple2<String,Integer>> broadData=new ArrayList<>();
        broadData.add(new Tuple2<>("杰森-塔图姆",21));
        broadData.add(new Tuple2<>("杰伦-布朗",23));
        broadData.add(new Tuple2<>("马库斯-斯玛特",25));
        broadData.add(new Tuple2<>("肯巴-沃克",29));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);
        DataSet<HashMap<String, Integer>> toBroadcast=tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String,Integer>>() {
            //处理需要广播变量的数据,把数据集转换成map类型,map中的key就是用户名,value就是用户年龄
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String,Integer> res=new HashMap<>();
                res.put(value._1,value._2);
                return res;
            }
        });
        //源数据
        DataSource<String> data = env.fromElements("杰森-塔图姆", "杰伦-布朗", "马库斯-斯玛特", "肯巴-沃克");

        //注意:在这里需要使用到RichMapFunction获得广播变量
         DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<>();
            HashMap<String, Integer> allMap = new HashMap<>();
            //获取广播变量

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             * 所以，就可以在open方法中获取广播变量数据
             *
             */

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }


            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");//执行广播数据的操作
        result.print();

    }
}
