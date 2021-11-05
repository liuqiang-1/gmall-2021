package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class APIRead {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("Http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);
        JestClient jestClient = jestClientFactory.getObject();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(termQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "球");
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        TermsAggregationBuilder groupByClass = AggregationBuilders.terms("groupByClass").field("class_id");
        MaxAggregationBuilder maxAge = AggregationBuilders.max("maxAge").field("age");
        searchSourceBuilder.aggregation(groupByClass.subAggregation(maxAge));
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        SearchResult result= jestClient.execute(search);


        //a.获取命中数据条数
        System.out.println("获取命中数据条数: "+result.getTotal());

        //b.获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println(hit.index);
            System.out.println(hit.type);
            System.out.println(hit.id);
            System.out.println("-----------source");
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o+" :"+ source.get(o));
            }
        }

        //TODO------------获取聚合组的数据-------------
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass1 = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass1.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key --------"+bucket.getKey());
            System.out.println("count --------"+bucket.getCount());
            System.out.println("maxAge --------"+bucket.getMaxAggregation("maxAge"));
        }
        jestClient.shutdownClient();
    }

}
