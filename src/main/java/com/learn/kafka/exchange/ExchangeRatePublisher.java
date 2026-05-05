package com.learn.kafka.exchange;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.producer.MessageProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class ExchangeRatePublisher {

    private static final BigDecimal REFERENCE_AMOUNT_USD = BigDecimal.valueOf(100);

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

        messageProducer.sendMessage(topic, toJson(response));

        response.rates().forEach((devise, taux) -> {
            ExchangeRate exchangeRate = new ExchangeRate(
                    response.base(),
                    devise,
                    currencyName(devise),
                    currencyZone(devise),
                    taux,
                    inverseRate(taux),
                    taux.multiply(REFERENCE_AMOUNT_USD).setScale(2, RoundingMode.HALF_UP),
                    timestamp
            );
            publishedRates.add(exchangeRate);
        });

        return publishedRates;
    }

    private String toJson(Object exchangeRate) {
        try {
            return objectMapper.writeValueAsString(exchangeRate);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize exchange rate", exception);
        }
    }

    private BigDecimal inverseRate(BigDecimal taux) {
        if (BigDecimal.ZERO.compareTo(taux) == 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.ONE.divide(taux, 6, RoundingMode.HALF_UP);
    }

    private String currencyName(String currencyCode) {
        return switch (currencyCode) {
            case "USD" -> "Dollar americain";
            case "EUR" -> "Euro";
            case "XOF" -> "Franc CFA BCEAO";
            case "XAF" -> "Franc CFA BEAC";
            case "GBP" -> "Livre sterling";
            case "CAD" -> "Dollar canadien";
            case "CHF" -> "Franc suisse";
            case "JPY" -> "Yen japonais";
            default -> currencyCode;
        };
    }

    private String currencyZone(String currencyCode) {
        return switch (currencyCode) {
            case "USD" -> "Etats-Unis";
            case "EUR" -> "Zone euro";
            case "XOF" -> "Afrique de l'Ouest";
            case "XAF" -> "Afrique centrale";
            case "GBP" -> "Royaume-Uni";
            case "CAD" -> "Canada";
            case "CHF" -> "Suisse";
            case "JPY" -> "Japon";
            default -> "Autre";
        };
    }
}
