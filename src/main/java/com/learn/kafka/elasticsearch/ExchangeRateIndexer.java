package com.learn.kafka.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class ExchangeRateIndexer {

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final String indexName;

    public ExchangeRateIndexer(RestClient.Builder restClientBuilder,
                               ObjectMapper objectMapper,
                               @Value("${spring.elasticsearch.uris}") String elasticsearchUri,
                               @Value("${exchange.elasticsearch.index}") String indexName) {
        this.restClient = restClientBuilder.baseUrl(elasticsearchUri).build();
        this.objectMapper = objectMapper;
        this.indexName = indexName;
    }

    public String index(String kafkaMessage) {
        Map<String, Object> document = toDocument(kafkaMessage);
        String id = document.getOrDefault("id", UUID.randomUUID().toString()).toString();

        restClient.put()
                .uri("/{index}/_doc/{id}", indexName, id)
                .body(document)
                .retrieve()
                .toBodilessEntity();

        return id;
    }

    private Map<String, Object> toDocument(String kafkaMessage) {
        try {
            String now = Instant.now().toString();
            Map<String, Object> document = objectMapper.readValue(
                    kafkaMessage,
                    new TypeReference<LinkedHashMap<String, Object>>() {
                    }
            );
            document.putIfAbsent("@timestamp", now);
            document.putIfAbsent("indexedAt", now);
            Object apiTimestamp = document.get("time_last_updated_utc");
            if (apiTimestamp != null) {
                document.putIfAbsent("timestamp", apiTimestamp);
            }
            return document;
        } catch (Exception exception) {
            String now = Instant.now().toString();
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("id", UUID.randomUUID().toString());
            document.put("rawMessage", kafkaMessage);
            document.put("@timestamp", now);
            document.put("indexedAt", now);
            document.put("parsingError", exception.getMessage());
            return document;
        }
    }
}
