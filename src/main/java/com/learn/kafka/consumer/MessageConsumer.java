package com.learn.kafka.consumer;

import com.learn.kafka.elasticsearch.ExchangeRateIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final ExchangeRateIndexer exchangeRateIndexer;

    public MessageConsumer(ExchangeRateIndexer exchangeRateIndexer) {
        this.exchangeRateIndexer = exchangeRateIndexer;
    }

    @KafkaListener(topics = "${exchange.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String message) {
        log.info("Exchange rate message received from Kafka: {}", message);
        exchangeRateIndexer.index(message);
    }

}
