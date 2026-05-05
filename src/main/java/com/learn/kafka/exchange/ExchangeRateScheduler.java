package com.learn.kafka.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ExchangeRateScheduler {

    private static final Logger log = LoggerFactory.getLogger(ExchangeRateScheduler.class);

    private final ExchangeRatePublisher exchangeRatePublisher;

    public ExchangeRateScheduler(ExchangeRatePublisher exchangeRatePublisher) {
        this.exchangeRatePublisher = exchangeRatePublisher;
    }

    @Scheduled(
            fixedDelayString = "${exchange.polling.fixed-delay-ms}",
            initialDelayString = "${exchange.polling.initial-delay-ms}"
    )
    public void publishRates() {
        int count = exchangeRatePublisher.publishLatestRates().size();
        log.info("Published {} exchange rates to Kafka", count);
    }
}
