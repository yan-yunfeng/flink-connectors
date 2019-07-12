package com.flink.connector.batch.es.outputformat;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-7-10 下午6:01
 */
public abstract class ElasticsearchOutputFormatBase<OUT> extends RichOutputFormat<OUT>{

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(OUT record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
