package com.test.flinktomysqlAndhbase;

import com.test.Utils.PropertyUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 *自定义mysqlsink
 */
public class MysqlSink extends RichSinkFunction<Entity> {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);
    private static final String driver = PropertyUtil.getProperty("mysql.connect.driver");
    private static final String url = PropertyUtil.getProperty("mysql.connect.url");
    private static final String username  = PropertyUtil.getProperty("mysql.connect.username");
    private static final String password = PropertyUtil.getProperty("mysql.connect.password");
    private PreparedStatement ps=null;
    private Connection connection=null;

    /**
     * open()方法建立连接
     * 这样不用每次 invoke 的时候都要建立连接和释放连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //System.out.println("----加载JDBC驱动----");
        //加载JDBC驱动
        Class.forName(driver);
        logger.info("url {} username {} password {}",url,username,password);
        //System.out.println("url:"+url+",username:"+username+",password:"+password);
        //创建连接
        connection = DriverManager.getConnection(url,username,password);
        //connection.setAutoCommit(false);
        String sql = "insert into web_access (city,loginTime,os,phoneName) values (?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }

    /**
     * 每插入一条数据的调用一次invoke
     * @param value
     * @param context
     * @throws Exception
     */
    public void invoke(Entity value, Context context) throws Exception {
            ps.setString(1,value.city);
            ps.setString(2,value.loginTime);
            ps.setString(3,value.os);
            ps.setString(4,value.phoneName);
            System.out.println("insert into web_access (city,loginTime,os,phoneName values ("+value.city+","+value.loginTime+","+value.os+","+value.phoneName);
            //ps.addBatch();
            //ps.executeBatch();
            ps.executeUpdate();
            //connection.commit();
        }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }
        if (ps != null){
            ps.close();
        }
    }
}