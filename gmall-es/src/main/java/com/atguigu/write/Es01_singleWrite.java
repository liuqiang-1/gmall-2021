package com.atguigu.write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es01_singleWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端构造器
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("Http://hadoop102:9200").build();
        factory.setHttpClientConfig(build);
        //2.创建客户端对象
        JestClient jestClient = factory.getObject();
        //3.构建Es插入数据

        Index index = new Index.Builder("{\n" +
                "  \"class_id\":\"0725\",\n" +
                "  \"stu_id\":\"110\",\n" +
                "  \"name\":\"猛哥\",\n" +
                "  \"sex\":\"男\",\n" +
                "  \"age\":1,\n" +
                "  \"favo\":\"秀恩爱,造孩子,喝椰奶,游戏王,刷B站,乒乓球\"\n" +
                "}").index("bigdata0625").id("1001").type("_doc").build();
         jestClient.execute(index);


        jestClient.shutdownClient();




    }
}
