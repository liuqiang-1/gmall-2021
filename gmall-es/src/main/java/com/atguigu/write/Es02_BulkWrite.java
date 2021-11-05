package com.atguigu.write;

import bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.ArrayList;

public class Es02_BulkWrite {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("Http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);

        JestClient jestClient = jestClientFactory.getObject();


        Movie movie1 = new Movie("101", "喜羊羊与灰太狼");
        Movie movie2 = new Movie("102", "岁月逝去");
        Movie movie3 = new Movie("103", "药神");
        Index build1 = new Index.Builder(movie1).id("1001").build();
        Index build2 = new Index.Builder(movie2).id("1002").build();
        Index build3 = new Index.Builder(movie3).id("1003").build();
        ArrayList<Index> indices = new ArrayList<>();
        indices.add(build1);
        indices.add(build2);
        indices.add(build3);

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie0625")
                .defaultType("_doc")
                .addAction(indices).build();
        jestClient.execute(bulk);


        jestClient.close();
    }
}
