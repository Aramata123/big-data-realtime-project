package com.learn.kafka.exchange;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.producer.MessageProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class ExchangeRatePublisher {

    private final ExchangeRateApiClient apiClient;
    private final MessageProducer messageProducer;
    private final ObjectMapper objectMapper;
    private final String topic;

    public ExchangeRatePublisher(ExchangeRateApiClient apiClient,
                                 MessageProducer messageProducer,
                                 ObjectMapper objectMapper,
                                 @Value("${exchange.kafka.topic}") String topic) {
        this.apiClient = apiClient;
        this.messageProducer = messageProducer;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public List<ExchangeRate> publishLatestRates() {
        ExchangeRateApiResponse response = Objects.requireNonNull(
                apiClient.getLatestRates(),
                "Exchange rate API returned an empty response"
        );

        Instant timestamp = Instant.now();
        List<ExchangeRate> publishedRates = new ArrayList<>();

        response.rates().forEach((devise, taux) -> {
            ExchangeRate exchangeRate = new ExchangeRate(devise, taux, timestamp);
            messageProducer.sendMessage(topic, toJson(exchangeRate));
            publishedRates.add(exchangeRate);
        });

        return publishedRates;
    }

    private String toJson(ExchangeRate exchangeRate) {
        try {
            return objectMapper.writeValueAsString(exchangeRate);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize exchange rate", exception);
        }
    }
}
