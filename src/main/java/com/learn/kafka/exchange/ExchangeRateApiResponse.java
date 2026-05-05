package com.learn.kafka.exchange;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ExchangeRateApiResponse(
        String base,
        String date,
        Map<String, BigDecimal> rates
) {
}
