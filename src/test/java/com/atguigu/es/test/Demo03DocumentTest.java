package com.atguigu.es.test;

import com.atguigu.es.config.ElasticSearchConfig;
import com.atguigu.es.entity.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class Demo03DocumentTest {

    @Resource
    RestHighLevelClient client;

    @Test
    public void saveDoc() throws Exception {
        //1.创建请求对象，指定索引库名称，类型名称，主键id
        IndexRequest request = new IndexRequest().index("shopping01").type("_doc").id("2");
        //方式一：写一个Product对象将对象转化为json 字符串
        /*Product product = Product.builder().id(1L).title("小米手机").price(1999.0).build();
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(product);
        request.source(productJson, XContentType.JSON);*/

        //方式二：直接在source 中写入k-v参数
        request.source(XContentType.JSON,"id",3L,"title","小米手机","price",1999);

        //2.客户端发送请求，获取响应对象
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_type:" + response.getType());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }

    @Test
    public void update() throws Exception {
        //1.创建请求对象，指定索引库名称，类型名称，主键id
        IndexRequest request = new IndexRequest().index("shopping01").type("_doc").id("2");
        //方式一：写一个Product对象将对象转化为json 字符串
        /*Product product = Product.builder().id(1L).title("小米手机").price(1999.0).build();
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(product);
        request.source(productJson, XContentType.JSON);*/

        //方式二：直接在source 中写入k-v参数
        request.source(XContentType.JSON,"id","1","title","小米手机","price","2999");

        //2.客户端发送请求，获取响应对象
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_type:" + response.getType());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }

    @Test
    public void getDoc() throws IOException {
        GetRequest request = new GetRequest().index("shopping01").type("_doc").id("2");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getType() = " + response.getType());
        System.out.println("response.getSourceAsString() = " + response.getSourceAsString());
    }

    //删除文档
    @Test
    public void delete() throws IOException {
        DeleteRequest request = new DeleteRequest("shopping01").type("_doc").id("2");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println("response.status() = " + response.status());
        System.out.println("response.toString() = " + response.toString());
    }

    @Test
    //批量新增操作
    public void bulkSave() throws IOException {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index("shopping01").type("_doc").id("1").source(XContentType.JSON, "title", "小米手机"));
        request.add(new IndexRequest().index("shopping01").type("_doc").id("2").source(XContentType.JSON, "title", "苹果手机"));
        request.add(new IndexRequest().index("shopping01").type("_doc").id("3").source(XContentType.JSON, "title", "华为手机"));

        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("responses.getTook() = " + responses.getTook());
        System.out.println("responses.getItems() = " + responses.getItems());
    }

    //批量删除操作
    @Test
    public void bulkDelete() throws IOException {
        //创建请求对象
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest().index("shopping01").type("_doc").id("1"));
        request.add(new DeleteRequest().index("shopping01").type("_doc").id("2"));
        request.add(new DeleteRequest().index("shopping01").type("_doc").id("3"));
        //客户端发送请求，获取响应对象
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + responses.getItems());
    }


}
