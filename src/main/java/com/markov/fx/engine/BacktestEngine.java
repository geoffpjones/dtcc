package com.markov.fx.engine;

import com.markov.fx.model.Bar;
import com.markov.fx.model.OnlineMarkovRegimeModel;
import com.markov.fx.model.Prediction;
import com.markov.fx.model.Regime;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BacktestEngine {
    public BacktestResult run(List<Bar> bars, OnlineMarkovRegimeModel model, BacktestConfig config) {
        if (bars.isEmpty()) {
            return BacktestResult.empty();
        }

        Map<Regime, Map<Regime, Integer>> matrix = initMatrix();
        int evaluated = 0;
        int correct = 0;
        int observedBars = 0;

        Optional<Prediction> pending = Optional.empty();

        for (Bar bar : bars) {
            Optional<Regime> realized = model.observeBar(bar);
            observedBars++;

            if (realized.isPresent() && pending.isPresent() && observedBars > config.warmupBars()) {
                Regime predicted = pending.get().predictedRegime();
                Regime actual = realized.get();
                matrix.get(predicted).put(actual, matrix.get(predicted).get(actual) + 1);
                evaluated++;
                if (predicted == actual) {
                    correct++;
                }
            }

            pending = model.predictNext(bar.timestamp());
        }

        return new BacktestResult(evaluated, correct, matrix);
    }

    private Map<Regime, Map<Regime, Integer>> initMatrix() {
        Map<Regime, Map<Regime, Integer>> matrix = new EnumMap<>(Regime.class);
        for (Regime predicted : Regime.values()) {
            Map<Regime, Integer> row = new EnumMap<>(Regime.class);
            for (Regime actual : Regime.values()) {
                row.put(actual, 0);
            }
            matrix.put(predicted, row);
        }
        return matrix;
    }
}
