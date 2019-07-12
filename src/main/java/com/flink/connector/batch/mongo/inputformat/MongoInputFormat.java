package com.flink.connector.batch.mongo.inputformat;

import com.flink.connector.common.MongoConfigKey;

import org.apache.flink.configuration.Configuration;
import org.bson.Document;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-26 下午4:03
 */
public class MongoInputFormat extends MongoInputFormatBase<String> {

    public MongoInputFormat(Configuration parameters) {
        super(parameters);
    }

    @Override
    public String transform(Document hit) {
        return hit.toJson();
    }

    public static class Builder{
        private Configuration parameters;

        public Builder() {
            this.parameters = new Configuration();
        }

        public Builder setUri(String uri){
            this.parameters.setString(MongoConfigKey.URI, uri);
            return this;
        }

        public Builder setDatabase(String database){
            this.parameters.setString(MongoConfigKey.DATABASE, database);
            return this;
        }

        public Builder setCollection(String collection){
            this.parameters.setString(MongoConfigKey.COLLECTION, collection);
            return this;
        }

        public Builder setBatchSize(int batchSize){
            this.parameters.setInteger(MongoConfigKey.BATCH_SIZE, batchSize);
            return this;
        }

        public MongoInputFormat build(){
            return new MongoInputFormat(this.parameters);
        }
    }
}
