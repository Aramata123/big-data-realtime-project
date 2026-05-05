package com.learn.kafka.exchange;

import java.math.BigDecimal;
import java.time.Instant;

public record ExchangeRate(
        String devise,
        BigDecimal taux,
        Instant timestamp
) {
}
