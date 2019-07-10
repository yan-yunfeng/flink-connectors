package com.flink.connector.core;

/**
 * es的配置的key
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-22 上午11:05
 */
public interface EsConfigKey {


    String HOSTS = "hosts";

    String USERNAME = "username";

    String PASSWORD = "password";

    String INDICES = "indices";

    String TYPES = "types";

    String QUERY_SIZE = "size";

    String SCROLL_SETTING_NAME = "scrollSettingName";

    String SCROLL_TIME = "scrollTime";
}
