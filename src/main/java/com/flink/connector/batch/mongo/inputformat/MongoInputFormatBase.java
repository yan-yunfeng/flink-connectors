package com.flink.connector.batch.mongo.inputformat;

import com.flink.connector.core.MongoConfigKey;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.bson.Document;

import java.io.IOException;
import java.util.Objects;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-26 下午4:03
 */
public abstract class MongoInputFormatBase<IN> extends GenericInputFormat<IN> implements NonParallelInput {

    private Configuration parameters;

    private transient MongoCursor<Document> mongoCursor;

    MongoInputFormatBase(Configuration parameters) {
        this.parameters = parameters;
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);

        String uri = Objects.requireNonNull(this.parameters.getString(MongoConfigKey.URI, null), "uri can not be null");
        String database = Objects.requireNonNull(this.parameters.getString(MongoConfigKey.DATABASE, null), "database can not be null");
        String collection = Objects.requireNonNull(this.parameters.getString(MongoConfigKey.COLLECTION, null), "collection can not be null");
        int batchSize = this.parameters.getInteger(MongoConfigKey.BATCH_SIZE, 1000);

        this.mongoCursor = new MongoClient(new MongoClientURI(uri)).getDatabase(database)
                                                                   .getCollection(collection)
                                                                   .find()
                                                                   .batchSize(batchSize)
                                                                   .iterator();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (this.mongoCursor!=null) {
            this.mongoCursor.close();
            this.mongoCursor =null;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !this.mongoCursor.hasNext();
    }

    @Override
    public IN nextRecord(IN reuse){
        return transform(this.mongoCursor.next());
    }

    public abstract IN transform(Document hit);
}
