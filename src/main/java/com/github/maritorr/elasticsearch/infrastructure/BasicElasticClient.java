package com.github.maritorr.elasticsearch.infrastructure;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BasicElasticClient {

    private static final Logger LOG = LoggerFactory.getLogger(BasicElasticClient.class);

    private RestHighLevelClient client;

    public BasicElasticClient() {
        setUp();
    }

    private void setUp() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "https")
                )
        );

    }

    public void index(String id, String jsonContent) {
        IndexRequest request = new IndexRequest("posts", "doc", id);
        request.source(jsonContent, XContentType.JSON);

        client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOG.info("Got response from indexing... [ {} ]", indexResponse.getResult());
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Something went wrong when indexing... ", e);
            }
        });
    }

    public void shutdown() throws IOException {
        client.close();
    }
}
