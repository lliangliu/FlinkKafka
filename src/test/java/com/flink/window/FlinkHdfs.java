package com.flink.window;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.junit.Test;

public class FlinkHdfs {
    @Test
    public void run()throws Exception{
         ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         DataSet<String> ds = env.readTextFile("hdfs://node1/tmp/liu_test1.txt");
         ds.print();
    }
}
