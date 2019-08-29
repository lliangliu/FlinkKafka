package com.test.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * properties 工具类
 */
public class PropertyUtil {
    private final static Logger logger = LoggerFactory.getLogger(PropertyUtil.class);
    private final static String PROP_FILE_NAME = "config.properties";
    private static Properties props;

    static {
        loadProps();
    }

    synchronized static private void loadProps() {
        logger.info("开始加载properties文件内容...");
        props = new Properties();
        InputStream in = null;
        try {
            File file = new File(new File(PropertyUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent(), PROP_FILE_NAME);
            if (file.isFile()) {
                logger.info("加载指定properties文件 {}", file.getPath());
                in = new FileInputStream(file);
            } else {
                logger.info("加载默认properties文件");
                in = PropertyUtil.class.getClassLoader().getResourceAsStream(PROP_FILE_NAME);

            }
            //in = PropertyUtil.class.getResourceAsStream("/jdbc.properties");
            props.load(in);
        } catch (FileNotFoundException e) {
            logger.error("jdbc.properties文件未找到");
        } catch (IOException e) {
            logger.error("出现IOException");
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
                logger.error("jdbc.properties文件流关闭出现异常");
            }
        }
        logger.info("加载properties文件内容完成...");
        logger.info("properties文件内容：" + props);
    }

    public static String getProperty(String key) {
        if (null == props) {
            loadProps();
        }
        return props.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        if (null == props) {
            loadProps();
        }
        return props.getProperty(key, defaultValue);
    }
}
