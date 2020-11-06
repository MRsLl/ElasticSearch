package com.atguigu.es.test;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class Demo02MappingTest {

    @Resource
    RestHighLevelClient restHighLevelClient;

    /**
     * 目标:配置映射：第一种方式，用XContentBuilder 构建请求体
     * 1.创建请求对象，配置映射
     * 设置索引库name
     * 设置配置映射请求体
     * 2.客户端接受并执行请求，获取请求对象
     * 3.打印响应结果
     * 4.关闭客户端，释放服务资源
     */
    @Test
    public void putMapping01() throws IOException {
//        * 1.创建请求对象，配置映射
        PutMappingRequest request = new PutMappingRequest("shopping01");
//        * 设置索引库name
//        * 设置配置映射请求体
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject()
                .startObject("properties")
                .startObject("title")
                .field("type", "text").field("analyzer", "ik_max_word")
                .endObject()
                .startObject("subtitle")
                .field("type", "text").field("analyzer", "ik_max_word")
                .endObject()
                .startObject("images")
                .field("type", "keyword")
                .endObject()
                .startObject("price")
                .field("type", "float")
                .endObject()
                .startObject("price01")
                .field("type", "float")
                .endObject()
                .endObject()
                .endObject();
        request.source(jsonBuilder);
//        * 2.客户端接受并执行请求，获取请求对象
        AcknowledgedResponse response = restHighLevelClient.indices().putMapping(request, RequestOptions.DEFAULT);
//        * 3.打印响应结果
        System.out.println(response.isAcknowledged());
//        * 4.关闭客户端，释放服务资源
        restHighLevelClient.close();
    }

    /**
     * 目标：配置映射。第二种方式，使用JSON字符串
     * 1.创建请求对象：配置映射
     * 设置索引库name
     * 设置配置映射请求体
     * 2.客户端发送请求，获取响应对象
     * 3.打印响应结果
     */
    @Test
    public void puttingMapping02() throws IOException {
        //1.创建请求对象：配置映射
        PutMappingRequest request = new PutMappingRequest("shopping01");
        //设置配置映射请求体 source("请求体json字符串"，"请求体的数据类型");
        request.source("{\n" +
                "  \"properties\": {\n" +
                "    \"title\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\"\n" +
                "    },\n" +
                "    \"subtitle\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\"\n" +
                "    },\n" +
                "    \"price\":{\n" +
                "      \"type\": \"float\"\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON);
        //2.客户端发送请求，获取响应对象
        AcknowledgedResponse response = restHighLevelClient.indices().putMapping(request, RequestOptions.DEFAULT);
        //3.打印响应结果
        System.out.println("response.isAcknowledged() = " + response.isAcknowledged());
        //4.关闭客户端，释放服务资源
        restHighLevelClient.close();
    }

    @Test
    //查看索引映射
    public void getMapping() throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices("shopping01");
        GetMappingsResponse response = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> map1 = response.mappings();
        MappingMetaData shopping01 = map1.get("shopping01");
        Map<String, Object> map2 = shopping01.getSourceAsMap();
        System.out.println(map2);
    }
}
