package com.flink.connector.batch.es.inputformat;

import com.flink.connector.common.EsConfigKey;

import org.apache.flink.configuration.Configuration;
import org.elasticsearch.search.SearchHit;

/**
 * 获取es的所有的数据,结构为JSON
 *
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-22 下午2:08
 */
public class ElasticsearchInputFormat extends ElasticsearchInputFormatBase<String> {

    private static final long serialVersionUID = 1L;

    private ElasticsearchInputFormat(Configuration parameters) {
        super(parameters);
    }

    /**
     * 将searchHit 转化为json，如需转化为其他的可以重写该方法
     *
     * @param hit SearchHit
     * @return String  json
     */
    @Override
    public String transform(SearchHit hit) {
        return hit.getSourceAsString();
    }

    public static class Builder {

        private Configuration parameters;

        public Builder() {
            this.parameters = new Configuration();
        }

        /**
         * @param hosts es的连接地址，多个host用“,”分隔
         * @return this
         */
        public Builder setHosts(String hosts) {
            this.parameters.setString(EsConfigKey.HOSTS, hosts);
            return this;
        }

        /**
         * @param username 用户名
         * @return this
         */
        public Builder setUsername(String username) {
            this.parameters.setString(EsConfigKey.USERNAME, username);
            return this;
        }

        /**
         * @param password 密码
         * @return this
         */
        public Builder setPassword(String password) {
            this.parameters.setString(EsConfigKey.PASSWORD, password);
            return this;
        }

        /**
         * @param indices 索引名，多个索引名用","隔开
         * @return this
         */
        public Builder setIndices(String indices) {
            this.parameters.setString(EsConfigKey.INDICES, indices);
            return this;
        }

        /**
         * @param types 类型名，多个类型名用","隔开
         * @return this
         */
        public Builder setTypes(String types) {
            this.parameters.setString(EsConfigKey.TYPES, types);
            return this;
        }

        /**
         * @param size 每次请求返回的数据大小，类型为int，默认为1000
         * @return this
         */
        public Builder setQuerySize(int size) {
            this.parameters.setInteger(EsConfigKey.QUERY_SIZE, size);
            return this;
        }

        /**
         * @param scrollTime 创建scroll时需要设置失效市场，类型为String，格式为时间值+单位
         *                   这里的单位有nanos,micros,ms,s,m,h,d，分别对应纳秒，微秒，毫秒，秒，分，时，天
         *                   examples：
         *                      2天：2d
         *                      2分钟：2m
         *                      2000毫秒：2000ms
         * @return this
         */
        public Builder setScrollTime(String scrollTime) {
            this.parameters.setString(EsConfigKey.SCROLL_TIME, scrollTime);
            return this;
        }

        /**
         * @param scrollSettingName 创建scroll时需要的一个参数，默认为"scroll setting name"
         * @return this
         */
        public Builder setScrollSettingName(String scrollSettingName) {
            this.parameters.setString(EsConfigKey.SCROLL_SETTING_NAME, scrollSettingName);
            return this;
        }

        /**
         * 创建ElasticsearchInputFormat实例
         *
         * @return ElasticsearchInputFormat
         */
        public ElasticsearchInputFormat build() {
            return new ElasticsearchInputFormat(this.parameters);
        }
    }

}
