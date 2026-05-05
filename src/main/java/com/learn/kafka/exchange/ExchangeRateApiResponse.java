package com.learn.kafka.exchange;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ExchangeRateApiResponse(
        String base,
        String date,
        @JsonProperty("time_last_updated_utc")
        String timeLastUpdatedUtc,
        Map<String, BigDecimal> rates
) {
}
