package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonRead {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig build = new HttpClientConfig.Builder("Http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);

        JestClient jestClient = jestClientFactory.getObject();
        Search search = new Search.Builder("").addIndex("movie0625").addType("_doc").build();
        SearchResult result = jestClient.execute(search);
        System.out.println("命中总条数"+result.getTotal());
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index :"+hit.index);
            System.out.println("_type: "+hit.type);
            System.out.println("_id: "+hit.id);
            Map source = hit.source;
            Set set = source.keySet();
            for (Object o : set) {
                System.out.println(o+" :"+source.get(o));
            }


        }
        jestClient.shutdownClient();
    }
}
