package com.test.flinktomysqlAndhbase;


import com.test.Utils.PropertyUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

public class MysqlBacthSink extends RichSinkFunction<List<Entity>> {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);
    private static final String driver = PropertyUtil.getProperty("mysql.connect.driver");
    private static final String url = PropertyUtil.getProperty("mysql.connect.url");
    private static final String username  = PropertyUtil.getProperty("mysql.connect.username");
    private static final String password = PropertyUtil.getProperty("mysql.connect.password");
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into web_access (city,loginTime,os,phoneName) values (?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<Entity> value, Context context) throws Exception {
        //遍历数据集合
        for (Entity entity : value) {
            ps.setString(1,entity.city);
            ps.setString(2,entity.loginTime);
            ps.setString(3,entity.os);
            ps.setString(4,entity.phoneName);
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");

    }



    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName(driver);
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
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
