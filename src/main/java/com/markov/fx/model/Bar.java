package com.markov.fx.model;

import java.time.Instant;

public record Bar(
        String symbol,
        Instant timestamp,
        double open,
        double high,
        double low,
        double close,
        double volume
) {
}
