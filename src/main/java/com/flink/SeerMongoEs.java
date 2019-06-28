package com.flink;

import com.flink.es.ElasticsearchInputFormat;
import com.flink.es.EsConfigKey;
import com.flink.mongo.MongoInputFormat;
import com.qydata.json.JsonObject;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * es中有，mongo中没有的数据清理
 *
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-23 下午5:04
 */
public class SeerMongoEs {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        ElasticsearchInputFormat inputFormat =
            new ElasticsearchInputFormat.Builder()
                                                  .setHosts("localhost:9200")
                                                  .setUsername("username")
                                                  .setPassword("password")
                                                  .setIndices("index")
                                                  .setTypes("type")
                                                  .setQuerySize(1000)
                                                  .setScrollTime("2m")
                                                  .setScrollSettingName("scroll_name")
                                                  .build();

        MongoInputFormat mongoInputFormat =
            new MongoInputFormat.Builder()
                                          .setUri("mongodb://localhost:27701")
                                          .setDatabase("database")
                                          .setCollection("collection")
                                          .setBatchSize(1000)
                                          .build();

        DataSet<String> mongoSet =
            env.createInput(mongoInputFormat)
               .map(json -> {
                   JsonObject jsonObject = new JsonObject(json);
                   return jsonObject.getString("_id");
               });

        DataSet<String> esSet =
            env.createInput(inputFormat)
               .map(json -> {
                   JsonObject jsonObject = new JsonObject(json);
                   return jsonObject.getString("rid");
               });

        mongoSet.fullOuterJoin(esSet)
                .where(key -> key)
                .equalTo(key -> key)
                .with(new FlatJoinFunction<String, String, String>() {
                          @Override
                          public void join(String first, String second, Collector<String> out) throws Exception {
                              if (first == null || second == null) {
                                  out.collect(first == null ? second + ",es" : first + ",mongo");
                              }
                          }
                      }
                )
                .writeAsText("hdfs://localhost:8020/es_mongo")
                //这里的并行度必须设置为1，不然会报错
                .setParallelism(1);
//                .writeAsText("/home/yyf/Desktop/join");

//        mongoSet.union(esSet)
//                .groupBy(0)
//                .reduce((value1, value2) ->
//                            Tuple2.of(value1.f0, value1.f1 + "=" + value2.f1)
//                )
//                .filter(value -> !value.f1.contains("="))
//                .map(value -> value.f0 + "," + value.f1)
////                .writeAsText("hdfs://172.16.20.2:8020/EsCompareWhithMongo");
//                .writeAsText("/home/yyf/Desktop/asd");

        env.execute();
    }
}
