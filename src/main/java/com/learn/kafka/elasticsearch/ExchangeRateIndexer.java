package com.learn.kafka.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class ExchangeRateIndexer {

    private final ElasticsearchClient elasticsearchClient;
    private final ObjectMapper objectMapper;
    private final String indexName;

    public ExchangeRateIndexer(ElasticsearchClient elasticsearchClient,
                               ObjectMapper objectMapper,
                               @Value("${exchange.elasticsearch.index}") String indexName) {
        this.elasticsearchClient = elasticsearchClient;
        this.objectMapper = objectMapper;
        this.indexName = indexName;
    }

    public String index(String kafkaMessage) {
        Map<String, Object> document = toDocument(kafkaMessage);
        String id = document.getOrDefault("id", UUID.randomUUID().toString()).toString();

        try {
            IndexResponse response = elasticsearchClient.index(request -> request
                    .index(indexName)
                    .id(id)
                    .document(document)
            );
            return response.id();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to index exchange rate in Elasticsearch", exception);
        }
    }

    private Map<String, Object> toDocument(String kafkaMessage) {
        try {
            Map<String, Object> document = objectMapper.readValue(
                    kafkaMessage,
                    new TypeReference<LinkedHashMap<String, Object>>() {
                    }
            );
            document.putIfAbsent("indexedAt", Instant.now().toString());
            return document;
        } catch (Exception exception) {
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("id", UUID.randomUUID().toString());
            document.put("rawMessage", kafkaMessage);
            document.put("indexedAt", Instant.now().toString());
            document.put("parsingError", exception.getMessage());
            return document;
        }
    }
}
