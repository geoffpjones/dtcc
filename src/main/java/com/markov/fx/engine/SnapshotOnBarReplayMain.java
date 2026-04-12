package com.markov.fx.engine;

import com.markov.fx.model.Bar;
import com.markov.fx.model.OnlineSnapshotDualLogisticModel;
import com.markov.fx.model.Prediction;
import com.markov.fx.model.Regime;
import com.markov.fx.model.SnapshotDualLogisticConfig;
import com.markov.fx.store.SqliteBarRepository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Replay bars through incremental onBar updates for snapshot models.
 *
 * Usage:
 *   SnapshotOnBarReplayMain [targetDbPath] [externalDbPath] [from] [to]
 */
public class SnapshotOnBarReplayMain {
    public static void main(String[] args) throws Exception {
        String targetDbPath = args.length >= 1 ? args[0] : "data/fx-bars-5y.db";
        String externalDbPath = args.length >= 2 ? args[1] : "data/market-bars-5y.db";
        Instant from = args.length >= 3 ? Instant.parse(args[2]) : Instant.parse("2021-03-22T00:00:00Z");
        Instant to = args.length >= 4 ? Instant.parse(args[3]) : Instant.parse("2026-03-22T00:00:00Z");

        SqliteBarRepository targetRepo = new SqliteBarRepository(targetDbPath);
        SqliteBarRepository externalRepo = new SqliteBarRepository(externalDbPath);

        runPreset(targetRepo, externalRepo, from, to, SnapshotDualLogisticConfig.previousSnapshot());
        runPreset(targetRepo, externalRepo, from, to, SnapshotDualLogisticConfig.target3040Snapshot());
    }

    private static void runPreset(
            SqliteBarRepository targetRepo,
            SqliteBarRepository externalRepo,
            Instant from,
            Instant to,
            SnapshotDualLogisticConfig cfg
    ) throws Exception {
        List<Bar> target = targetRepo.load(cfg.targetSymbol(), from, to);
        List<Bar> merged = new ArrayList<>(target);
        if (cfg.useExternal()) {
            merged.addAll(externalRepo.load(cfg.externalSymbol(), from, to));
        }

        // External first on same timestamp so target feature extraction sees latest external close.
        merged.sort(Comparator.comparing(Bar::timestamp).thenComparing(b -> b.symbol().equals(cfg.targetSymbol()) ? 1 : 0));

        OnlineSnapshotDualLogisticModel model = new OnlineSnapshotDualLogisticModel(cfg);

        Optional<Prediction> pending = Optional.empty();
        Double prevTargetClose = null;
        int evaluated = 0;
        int correct = 0;
        int signaled = 0;
        int signaledCorrect = 0;

        for (Bar bar : merged) {
            boolean isTarget = bar.symbol().equals(cfg.targetSymbol());
            if (isTarget && prevTargetClose != null && pending.isPresent()) {
                Regime realized = classify(prevTargetClose, bar.close(), model.currentThreshold().orElse(0.00030));
                Prediction pred = pending.get();
                evaluated++;
                if (pred.predictedRegime() != Regime.NONE) {
                    signaled++;
                    if (pred.predictedRegime() == realized) {
                        signaledCorrect++;
                    }
                }
                if (pred.predictedRegime() == realized) {
                    correct++;
                }
            }

            Optional<Prediction> next = model.onBar(bar);
            if (isTarget) {
                prevTargetClose = bar.close();
                pending = next;
            }
        }

        double acc = evaluated == 0 ? 0.0 : (double) correct / evaluated;
        double signalPrecision = signaled == 0 ? 0.0 : (double) signaledCorrect / signaled;
        double signalRate = evaluated == 0 ? 0.0 : (double) signaled / evaluated;
        double noneRate = 1.0 - signalRate;

        System.out.printf(
                "Preset=%s evaluated=%d accuracy=%.4f signalPrecision=%.4f signalRate=%.4f noneRate=%.4f trained=%d seenTarget=%d%n",
                cfg.name(),
                evaluated,
                acc,
                signalPrecision,
                signalRate,
                noneRate,
                model.trainedExamples(),
                model.seenTargetBars()
        );
    }

    private static Regime classify(double prevClose, double close, double threshold) {
        double r = close / prevClose - 1.0;
        if (r > threshold) {
            return Regime.UP;
        }
        if (r < -threshold) {
            return Regime.DOWN;
        }
        return Regime.NONE;
    }
}
