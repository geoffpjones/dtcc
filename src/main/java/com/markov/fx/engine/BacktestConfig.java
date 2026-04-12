package com.markov.fx.engine;

public record BacktestConfig(
        int warmupBars
) {
    public BacktestConfig {
        if (warmupBars < 0) {
            throw new IllegalArgumentException("warmupBars must be >= 0");
        }
    }
}
