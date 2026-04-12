package com.markov.fx.model;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

public record Prediction(
        Instant timestamp,
        Map<Regime, Double> probabilities,
        Regime predictedRegime
) {
    public static Prediction from(Instant ts, double up, double none, double down) {
        Map<Regime, Double> p = new EnumMap<>(Regime.class);
        p.put(Regime.UP, up);
        p.put(Regime.NONE, none);
        p.put(Regime.DOWN, down);

        Regime best = Regime.UP;
        double bestP = up;
        if (none > bestP) {
            best = Regime.NONE;
            bestP = none;
        }
        if (down > bestP) {
            best = Regime.DOWN;
        }
        return new Prediction(ts, p, best);
    }
}
