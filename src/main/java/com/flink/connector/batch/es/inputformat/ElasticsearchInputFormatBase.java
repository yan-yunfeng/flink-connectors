package com.flink.connector.batch.es.inputformat;

import com.flink.connector.common.EsConfigKey;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * es通过RestHighLevelClient全量读取Elasticsearch的数据
 *
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-22 上午11:50
 */
public abstract class ElasticsearchInputFormatBase<IN> extends GenericInputFormat<IN> {

    private static final String HTTP_HOST_SCHEME = "http";

    /**
     * 配置参数
     */
    private Configuration parameters;

    //-----所有的未在open方法中初始化的属性，都应可序列化，不能序列化的应该用transient修饰-----//

    /**
     * RestHighLevelClient的封装比较好，这里直接用这个
     */
    private transient RestHighLevelClient client;

    /**
     * ES的游标,每次请求都要带上
     */
    private transient Scroll scroll;

    /**
     * 每次请求的回复。
     * 当本次的回复的所有hits都被遍历完后，调用nextSearchResponse()获取下一个
     */
    private transient SearchResponse searchResponse;

    /**
     * 通过searchResponse获取的当次请求的hits的迭代器
     */
    private transient Iterator<SearchHit> searchHitIterator;


    ElasticsearchInputFormatBase(Configuration parameters) {
        this.parameters = parameters;
    }

    /**
     * 根据split初始化这个分区的所有参数，这里的split和分区的概念差不多，split的总数即为并行度数量
     *
     * @param split 当前打开的split，可以从中获取split总数和当前的split数
     * @throws IOException client查询时有可能会抛出该异常
     */
    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);

        this.client = new RestHighLevelClient(clientBuilder());

        this.scroll = scroll();

        //第一次搜索请求的回复及hits迭代器
        this.searchResponse = this.client.search(searchRequest(this.scroll,split));

        this.searchHitIterator = this.searchResponse.getHits().iterator();
    }

    /**
     * 根据配置得到RestHighLevelClient
     *
     * @return RestHighLevelClient
     */
    private RestClientBuilder clientBuilder() {
        String hosts = Objects.requireNonNull(parameters.getString(EsConfigKey.HOSTS, null), "hosts can not be null");
        HttpHost[] httpHosts = Stream.of(hosts.split(","))
                                     .map(host -> {
                                         String[] hostPort = host.split(":");
                                         return new HttpHost(hostPort[0], Integer.parseInt(hostPort[1]), HTTP_HOST_SCHEME);
                                     })
                                     .toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(httpHosts);
        authenticate(builder);
        return builder;
    }

    /**
     * 有的client有用户名和密码，需要在builder中进行设置
     *
     * @param builder RestClientBuilder
     */
    private void authenticate(RestClientBuilder builder) {
        //如果配置了用户名和密码，则需要如下的配置
        if (parameters.containsKey(EsConfigKey.USERNAME) && parameters.containsKey(EsConfigKey.PASSWORD)) {
            String username = Objects.requireNonNull(parameters.getString(EsConfigKey.USERNAME, null), "username can not be null");
            String password = Objects.requireNonNull(parameters.getString(EsConfigKey.PASSWORD, null), "password can not be null");
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
    }

    /**
     * 通过配置得到游标Scroll
     *
     * @return Scroll
     */
    private Scroll scroll() {
        //获取游标的超时时间
        String timeValue = Objects.requireNonNull(this.parameters.getString(EsConfigKey.SCROLL_TIME, null), "timeValue can not be null");
        //创建游标，此处的settingName应该是没有实际意义，仅做异常的标识定位
        String scrollSettingName = this.parameters.getString(EsConfigKey.SCROLL_SETTING_NAME, "scroll setting name");
        return new Scroll(TimeValue.parseTimeValue(timeValue, scrollSettingName));
    }

    /**
     * 根据配置以及游标，得到初始的SearchRequest
     *
     * @param scroll 查询时的游标
     * @param split 本次开启的split
     * @return SearchRequest
     */
    private SearchRequest searchRequest(Scroll scroll,GenericInputSplit split) {
        String[] indices = Objects.requireNonNull(this.parameters.getString(EsConfigKey.INDICES, null), "indices can not be null").split(",");
        String[] types = Objects.requireNonNull(this.parameters.getString(EsConfigKey.TYPES, null), "types can not be null").split(",");

        //查询全部数据
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();

        //得到每次请求返回的数据大小，默认1000条
        int size = this.parameters.getInteger(EsConfigKey.QUERY_SIZE, 1000);

        return new SearchRequest().indices(indices)
                                  .types(types)
                                  .scroll(scroll)
                                  .source(
                                      new SearchSourceBuilder().query(queryBuilder)
                                                               .size(size)
                                                               //这里可以直接根据split的配置进行分片的读取，最好按照es的分片数进行分区
                                                               //详见https://www.elastic.co/guide/en/elasticsearch/reference/7.1/search-request-scroll.html#sliced-scroll
                                                               .slice(
                                                                   new SliceBuilder(split.getSplitNumber(), //得到当前分区数
                                                                                    split.getTotalNumberOfSplits() //得到总分区数
                                                                   )
                                                               )
                                  );
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (this.searchHitIterator.hasNext()) {
            return false;
        } else {
            nextSearchResponse();
            return !this.searchHitIterator.hasNext();
        }
    }

    /**
     * 当上一次请求得到的数据全部被消费完后，通过client从scroll中进行下一次请求
     * @throws IOException searchScroll可能抛出IO异常
     */
    private void nextSearchResponse() throws IOException {
        String scrollId = this.searchResponse.getScrollId();
        //新建通过游标搜索的请求
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(this.scroll);

        this.searchResponse = this.client.searchScroll(scrollRequest);
        this.searchHitIterator = this.searchResponse.getHits().iterator();
    }

    @Override
    public IN nextRecord(IN reuse){
        return transform(this.searchHitIterator.next());
    }

    /**
     * 将searchHit转化为需要的数据结构
     * @param hit SearchHit
     * @return IN 用户自行定义的数据结构
     */
    public abstract IN transform(SearchHit hit);

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        }
    }
}
