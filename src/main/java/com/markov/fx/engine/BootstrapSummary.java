package com.markov.fx.engine;

import java.util.List;

public record BootstrapSummary(
        ModelSpec spec,
        int iterations,
        double meanAccuracy,
        double p10Accuracy,
        double p50Accuracy,
        double p90Accuracy,
        List<Double> samples
) {
}
