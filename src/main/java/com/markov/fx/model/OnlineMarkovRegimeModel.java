package com.markov.fx.model;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class OnlineMarkovRegimeModel {
    private final MarkovConfig config;
    private final Map<StateKey, EnumMap<Regime, Integer>> transitions = new ConcurrentHashMap<>();
    private final Deque<Regime> recentRegimes = new ArrayDeque<>();

    private Bar previousBar;

    public OnlineMarkovRegimeModel(MarkovConfig config) {
        this.config = config;
    }

    public Optional<Regime> observeBar(Bar bar) {
        if (previousBar == null) {
            previousBar = bar;
            return Optional.empty();
        }

        Regime realized = classify(previousBar.close(), bar.close());

        if (recentRegimes.size() == config.order()) {
            StateKey state = new StateKey(recentRegimes.stream().toList());
            transitions.computeIfAbsent(state, k -> new EnumMap<>(Regime.class));
            EnumMap<Regime, Integer> counts = transitions.get(state);
            counts.put(realized, counts.getOrDefault(realized, 0) + 1);
        }

        recentRegimes.addLast(realized);
        while (recentRegimes.size() > config.order()) {
            recentRegimes.removeFirst();
        }
        previousBar = bar;

        return Optional.of(realized);
    }

    public Optional<Prediction> predictNext(Instant asOf) {
        if (recentRegimes.size() < config.order()) {
            return Optional.empty();
        }

        StateKey state = new StateKey(recentRegimes.stream().toList());
        EnumMap<Regime, Integer> counts = transitions.get(state);

        double alpha = config.laplaceAlpha();
        double up = alpha;
        double none = alpha;
        double down = alpha;

        if (counts != null) {
            up += counts.getOrDefault(Regime.UP, 0);
            none += counts.getOrDefault(Regime.NONE, 0);
            down += counts.getOrDefault(Regime.DOWN, 0);
        }

        double sum = up + none + down;
        return Optional.of(Prediction.from(asOf, up / sum, none / sum, down / sum));
    }

    public int learnedStates() {
        return transitions.size();
    }

    public void resetObservationContext() {
        previousBar = null;
        recentRegimes.clear();
    }

    public Regime classify(double prevClose, double close) {
        double r = (close - prevClose) / prevClose;
        if (r > config.neutralReturnThreshold()) {
            return Regime.UP;
        }
        if (r < -config.neutralReturnThreshold()) {
            return Regime.DOWN;
        }
        return Regime.NONE;
    }
}
