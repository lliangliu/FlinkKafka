package com.flink.window;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.junit.Test;

import java.text.SimpleDateFormat;

public class FlinkHdfs {
    @Test
    public void run()throws Exception{
         ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         DataSet<String> ds = env.readTextFile("hdfs://node1/tmp/liu_test1.txt");
         ds.print();
    }
    @Test
    public void run1(){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(1567645904920L);
        System.out.println(date);
    }

}
