package com.atguigu.es.test;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

@SpringBootTest
@RunWith(SpringRunner.class)
public class Demo01IndexTest {
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * 目标：创建索引库
     * 1.创建请求对象，设置索引库name
     * 2.客户端发送请求，获取响应对象
     * 3.打印响应对象中的返回结果
     * 4.关闭客户端，释放连接资源
     */
    @Test
    public void create() throws IOException {
//        1.创建请求对象，设置索引库name
        CreateIndexRequest request = new CreateIndexRequest("shopping01");
//        2.客户端发送请求，获取响应对象
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
//        3.打印响应对象中的返回结果
        System.out.println("response.index() = " + response.index());
        System.out.println("response.isAcknowledged() = " + response.isAcknowledged());
//        4.关闭客户端，释放连接资源
        restHighLevelClient.close();
    }

    @Test
    public void getIndex() throws IOException {
        //1.创建请求对象:查询索引库
        GetIndexRequest request = new GetIndexRequest("shopping");
        //2.客户端执行请求发送，返回响应对象
        GetIndexResponse response = restHighLevelClient.indices().get(request,RequestOptions.DEFAULT);
        //3.打印结果信息
        System.out.println("response.getAliases() = " + response.getAliases());
        System.out.println("response.getIndices() = " + response.getIndices());
        Map<String, Settings> settings = response.getSettings();
        Set<Map.Entry<String, Settings>> entries = settings.entrySet();
        for (Map.Entry<String, Settings> entry : entries) {
            System.out.println(entry.getValue());
        }
        //4.关闭客户端释放连接资源
        restHighLevelClient.close();
    }

    @Test
    //删除索引库
    public void deleteIndex() throws IOException {
        //1.创建请求对象，删除索引
        DeleteIndexRequest request = new DeleteIndexRequest("shopping01");
        //2.客户端接收并执行请求，返回请求对象
        AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        System.out.println("response.getResult() = " + response.isAcknowledged());
        //4.关闭客户端，释放连接资源
        restHighLevelClient.close();
    }
}
