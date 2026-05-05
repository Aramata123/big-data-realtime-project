package com.learn.kafka.exchange;

import java.math.BigDecimal;
import java.time.Instant;

public record ExchangeRate(
        String deviseSource,
        String devise,
        String nomDevise,
        String zoneMonetaire,
        BigDecimal taux,
        BigDecimal tauxInverse,
        BigDecimal montantPour100Usd,
        Instant timestamp
) {
}
