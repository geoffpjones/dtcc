package com.markov.fx.engine;

import com.markov.fx.model.Regime;

import java.util.EnumMap;
import java.util.Map;

public record BacktestResult(
        int evaluatedPredictions,
        int correctPredictions,
        Map<Regime, Map<Regime, Integer>> confusionMatrix
) {
    public double accuracy() {
        if (evaluatedPredictions == 0) {
            return 0.0;
        }
        return (double) correctPredictions / evaluatedPredictions;
    }

    public static BacktestResult empty() {
        Map<Regime, Map<Regime, Integer>> matrix = new EnumMap<>(Regime.class);
        for (Regime predicted : Regime.values()) {
            Map<Regime, Integer> row = new EnumMap<>(Regime.class);
            for (Regime actual : Regime.values()) {
                row.put(actual, 0);
            }
            matrix.put(predicted, row);
        }
        return new BacktestResult(0, 0, matrix);
    }
}
