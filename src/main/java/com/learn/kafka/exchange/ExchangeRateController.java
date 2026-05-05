package com.learn.kafka.exchange;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ExchangeRateController {

    private final ExchangeRatePublisher exchangeRatePublisher;

    public ExchangeRateController(ExchangeRatePublisher exchangeRatePublisher) {
        this.exchangeRatePublisher = exchangeRatePublisher;
    }

    @PostMapping("/exchange-rates/refresh")
    public ResponseEntity<List<ExchangeRate>> refreshRates() {
        return ResponseEntity.ok(exchangeRatePublisher.publishLatestRates());
    }
}
