package com.flink.connector.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-7-10 下午5:37
 */
public class ConfigReader {

    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class);
    private final static String CONFIG_FILE_NAME = "config-dev.properties";
    private static Properties props;

    static{
        loadProps();
    }

    synchronized static private void loadProps(){
        props = new Properties();
        InputStream in = null;
        try {
            //通过类加载器进行获取properties文件流
            in = ConfigReader.class.getClassLoader().getResourceAsStream(CONFIG_FILE_NAME);
            props.load(in);
        } catch (Exception e) {
            LOGGER.error("加载配置文件"+CONFIG_FILE_NAME+"失败",e);
        } finally {
            try {
                if(null != in) {
                    in.close();
                }
            } catch (IOException e) {
                LOGGER.error(CONFIG_FILE_NAME+"文件流关闭出现异常",e);
            }
        }
    }

    public static String getProperty(String key){
        if(null == props) {
            loadProps();
        }
        return props.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        if(null == props) {
            loadProps();
        }
        return props.getProperty(key, defaultValue);
    }
}
