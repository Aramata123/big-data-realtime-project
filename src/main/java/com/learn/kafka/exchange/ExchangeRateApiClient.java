package com.learn.kafka.exchange;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class ExchangeRateApiClient {

    private final RestClient restClient;
    private final String apiUrl;

    public ExchangeRateApiClient(RestClient.Builder restClientBuilder,
                                 @Value("${exchange.api.url}") String apiUrl) {
        this.restClient = restClientBuilder.build();
        this.apiUrl = apiUrl;
    }

    public ExchangeRateApiResponse getLatestRates() {
        return restClient.get()
                .uri(apiUrl)
                .retrieve()
                .body(ExchangeRateApiResponse.class);
    }
}
