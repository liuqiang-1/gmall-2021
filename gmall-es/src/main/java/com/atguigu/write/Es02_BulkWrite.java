package com.atguigu.write;

import bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.elasticsearch.rest.action.document.RestBulkAction;

import java.io.IOException;
import java.util.ArrayList;

public class Es02_BulkWrite {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig conf = new HttpClientConfig.Builder("Http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(conf);
        JestClient jestClient = jestClientFactory.getObject();
        Bulk.Builder builder = new Bulk.Builder().defaultIndex("movie0625").defaultType("_doc");
        Movie movie = new Movie("1004", "你和你妈");
        Movie movie1 = new Movie("1005", "你和你姐姐");
        Movie movie2 = new Movie("1006", "你和弟弟");


        Index index1 = new Index.Builder(movie).build();
        Index index2 = new Index.Builder(movie1).build();
        Index index3 = new Index.Builder(movie2).build();
        Bulk bulk = builder.addAction(index1)
                .addAction(index2)
                .addAction(index3).build();
        jestClient.execute(bulk);



        jestClient.close();
    }
}
