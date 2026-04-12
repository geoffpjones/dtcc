package com.markov.fx.model;

public record MarkovConfig(
        int order,
        double neutralReturnThreshold,
        double laplaceAlpha
) {
    public MarkovConfig {
        if (order < 1) {
            throw new IllegalArgumentException("order must be >= 1");
        }
        if (neutralReturnThreshold < 0.0) {
            throw new IllegalArgumentException("neutralReturnThreshold must be >= 0");
        }
        if (laplaceAlpha <= 0.0) {
            throw new IllegalArgumentException("laplaceAlpha must be > 0");
        }
    }
}
