package com.atguigu.es.test;

import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
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
public class Demo04RequestBodyTest {

    @Resource
    private RestHighLevelClient client;

    @Test
    public void initData() throws IOException {
        //批量新增操作
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "小米手机", "images", "http://www.gulixueyuan.com/xm.jpg", "price", 1999.0));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "小米电视", "images", "http://www.gulixueyuan.com/xmds.jpg", "price", 2999.0));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "华为手机", "images", "http://www.gulixueyuan.com/hw.jpg", "price", 4999.0, "subtitle", "小米"));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "apple手机", "images", "http://www.gulixueyuan.com/appletl.jpg", "price", 5999.00));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "apple", "images", "http://www.gulixueyuan.com/apple.jpg", "price", 3999.00));
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took::" + response.getTook());
        System.out.println("Items::" + response.getItems());

    }

    /**
     * 打印结果信息
     */
    private void printResult(SearchResponse response){
        SearchHits hits = response.getHits();
        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<<========");
    }

    /**
     * 基本查询
     */
    @Test
    public void basicQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有
//        sourceBuilder.query(QueryBuilders.matchAllQuery());

        //match 查询，带分词器的查询
//        sourceBuilder.query(QueryBuilders.matchQuery("title","小米手机").operator(Operator.OR));

        //term 查询：不带分词器，查询条件做关键词
//        sourceBuilder.query(QueryBuilders.termQuery("title","小米"));

        //multi match：多个字段的match 查询
//        sourceBuilder.query(QueryBuilders.multiMatchQuery("小米手机","title","subtitle"));

        //terms 查询，多个关键词匹配
        sourceBuilder.query(QueryBuilders.termsQuery("title","小米","华为"));

        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }

    /**
     * 查询的字段过滤，排序，分页
     */
    @Test
    public void fetchSourceAndSortAndByPage() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页信息
        sourceBuilder.from(0);
        sourceBuilder.size(2);
        //排序信息
        sourceBuilder.sort("price", SortOrder.DESC);
        //查询字段过滤
        String[] excludes = {};
        String[] indcludes = {"title","subtitle","price"};
        sourceBuilder.fetchSource(indcludes,excludes);

        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }

    /**
     * 高级查询
     * bool，范围，模糊
     */
    @Test
    public void boolAndRangeAndFuzzyQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //高级查询三种方式
/*        //bool 查询：查询title 中必须有小米，一定不含电视，应该有手机的所有商品
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.must(QueryBuilders.matchQuery("title","小米"));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("title","电视"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("title","手机"));

        sourceBuilder.query(boolQueryBuilder);*/

        //范围查询：查询价格大于3000，小于5000 的所有商品
/*        RangeQueryBuilder rangeQueryBuilder =  QueryBuilders.rangeQuery("price");
//        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("price");
        //gt 大于
        rangeQueryBuilder.gt("3000");
        //lt 小于
        rangeQueryBuilder.lt("5000");
        sourceBuilder.query(rangeQueryBuilder);*/

        //模糊查询：查询包含apple 关键词的所有商品，完成模糊查询 cpple
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("title","cpple");
        fuzzyQueryBuilder.fuzziness(Fuzziness.ONE);
        sourceBuilder.query(fuzzyQueryBuilder);

        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }

    @Test
    public void hightLightQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest().types("_doc").indices("shopping01");
        //2.创建查询请求体构建器
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //构建查询方式：高亮查询
        sourceBuilder.query(QueryBuilders.termQuery("title","apple"));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
        highlightBuilder.postTags("</font>");//设置标签后缀
        //设置高亮构建对象
        sourceBuilder.highlighter(highlightBuilder);

        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            //打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }

    }
}
