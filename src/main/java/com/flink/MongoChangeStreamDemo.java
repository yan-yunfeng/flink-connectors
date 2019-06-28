package com.flink;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.qydata.json.JsonObject;

import org.bson.BsonDocument;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-27 下午4:28
 */
public class MongoChangeStreamDemo {
    public static void main(String[] args) {

        ChangeStreamIterable<Document> changeStreamIterable =
            new MongoClient(new MongoClientURI("mongodb://localhost:27701"))
                .getDatabase("database")
                .getCollection("collection")
                .watch()
//                .resumeAfter(BsonDocument.parse("{ \"_data\" : { \"$binary\" : \"gl0UizIAAAABRjxfaWQAPDQ2YzFjOWU5OWIwNWRjNTVjZDBlMDEyYjM2Y2U5Njg5AABaEAQdM4OAt+tAO776qZoa1xxXBA==\", \"$type\" : \"00\" } }"))
                .maxAwaitTime(10, TimeUnit.DAYS);

        MongoCursor<ChangeStreamDocument<Document>> streamCursor = changeStreamIterable.iterator();

        while (streamCursor.hasNext()){
            ChangeStreamDocument<Document> changeStreamDocument = streamCursor.next();
            Document document = changeStreamDocument.getFullDocument();
            System.out.println(changeStreamDocument.getResumeToken().toJson());
//            System.out.println(changeStreamDocument.getDocumentKey().toJson());
//            System.out.println(changeStreamDocument.getOperationType().getValue());
//            System.out.println(new JsonObject(document).encodePrettily());
        }

    }
}
