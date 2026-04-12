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
 * Backtest main for incremental snapshot dual-logistic models with python-style metrics.
 *
 * Usage:
 *   SnapshotDualLogisticBacktestMain [targetDbPath] [externalDbPath] [from] [to] [warmupBars]
 */
public class SnapshotDualLogisticBacktestMain {
    public static void main(String[] args) throws Exception {
        String targetDbPath = args.length >= 1 ? args[0] : "data/fx-bars-5y.db";
        String externalDbPath = args.length >= 2 ? args[1] : "data/market-bars-5y.db";
        Instant from = args.length >= 3 ? Instant.parse(args[2]) : Instant.parse("2021-03-22T00:00:00Z");
        Instant to = args.length >= 4 ? Instant.parse(args[3]) : Instant.parse("2026-03-22T00:00:00Z");
        int warmupBars = args.length >= 5 ? Integer.parseInt(args[4]) : 30;

        SqliteBarRepository targetRepo = new SqliteBarRepository(targetDbPath);
        SqliteBarRepository externalRepo = new SqliteBarRepository(externalDbPath);

        SnapshotMetrics m1 = runPreset(targetRepo, externalRepo, from, to, warmupBars, SnapshotDualLogisticConfig.previousSnapshot());
        SnapshotMetrics m2 = runPreset(targetRepo, externalRepo, from, to, warmupBars, SnapshotDualLogisticConfig.target3040Snapshot());

        printHeader();
        printRow(m1);
        printRow(m2);
    }

    private static SnapshotMetrics runPreset(
            SqliteBarRepository targetRepo,
            SqliteBarRepository externalRepo,
            Instant from,
            Instant to,
            int warmupBars,
            SnapshotDualLogisticConfig cfg
    ) throws Exception {
        List<Bar> target = targetRepo.load(cfg.targetSymbol(), from, to);
        List<Bar> merged = new ArrayList<>(target);
        if (cfg.useExternal()) {
            merged.addAll(externalRepo.load(cfg.externalSymbol(), from, to));
        }
        merged.sort(Comparator.comparing(Bar::timestamp).thenComparing(b -> b.symbol().equals(cfg.targetSymbol()) ? 1 : 0));

        OnlineSnapshotDualLogisticModel model = new OnlineSnapshotDualLogisticModel(cfg);

        Optional<Prediction> pending = Optional.empty();
        Double prevTargetClose = null;
        int observedTargetBars = 0;
        double pendingThreshold = 0.00030;

        int evaluated = 0;
        int eventCount = 0;
        int signalCount = 0;
        int eventCorrect = 0;
        int signalCorrect = 0;
        int eventCovered = 0;
        int actualUp = 0;
        int actualDown = 0;
        int upProbHitNum = 0;
        int downProbHitNum = 0;
        int predUp = 0;
        int predDown = 0;
        int predUpCorrect = 0;
        int predDownCorrect = 0;
        int actualNone = 0;
        int actualUpCorrect = 0;
        int actualDownCorrect = 0;
        int actualNoneCorrect = 0;

        for (Bar bar : merged) {
            boolean isTarget = bar.symbol().equals(cfg.targetSymbol());

            if (isTarget && prevTargetClose != null && pending.isPresent()) {
                observedTargetBars++;
                if (observedTargetBars > warmupBars) {
                    Regime actual = classify(prevTargetClose, bar.close(), pendingThreshold);
                    Prediction pred = pending.get();
                    Regime predicted = pred.predictedRegime();
                    double pUp = pred.probabilities().get(Regime.UP);
                    double pDown = pred.probabilities().get(Regime.DOWN);

                    evaluated++;
                    if (actual != Regime.NONE) {
                        eventCount++;
                        if (predicted == actual) {
                            eventCorrect++;
                        }
                        if (predicted != Regime.NONE) {
                            eventCovered++;
                        }
                    }
                    if (predicted != Regime.NONE) {
                        signalCount++;
                        if (predicted == actual) {
                            signalCorrect++;
                        }
                    }

                    if (actual == Regime.UP) {
                        actualUp++;
                        if (pUp > 0.5) {
                            upProbHitNum++;
                        }
                        if (predicted == Regime.UP) {
                            actualUpCorrect++;
                        }
                    } else if (actual == Regime.DOWN) {
                        actualDown++;
                        if (pDown > 0.5) {
                            downProbHitNum++;
                        }
                        if (predicted == Regime.DOWN) {
                            actualDownCorrect++;
                        }
                    } else {
                        actualNone++;
                        if (predicted == Regime.NONE) {
                            actualNoneCorrect++;
                        }
                    }

                    if (predicted == Regime.UP) {
                        predUp++;
                        if (actual == Regime.UP) {
                            predUpCorrect++;
                        }
                    } else if (predicted == Regime.DOWN) {
                        predDown++;
                        if (actual == Regime.DOWN) {
                            predDownCorrect++;
                        }
                    }
                }
            }

            Optional<Prediction> next = model.onBar(bar);
            if (isTarget) {
                prevTargetClose = bar.close();
                pending = next;
                pendingThreshold = model.currentThreshold().orElse(0.00030);
            }
        }

        double eventAcc = ratio(eventCorrect, eventCount);
        double signalPrecision = ratio(signalCorrect, signalCount);
        double eventCoverage = ratio(eventCovered, eventCount);
        double upProbHit = ratio(upProbHitNum, actualUp);
        double downProbHit = ratio(downProbHitNum, actualDown);
        double upPrecisionSignal = ratio(predUpCorrect, predUp);
        double downPrecisionSignal = ratio(predDownCorrect, predDown);
        double signalRate = ratio(signalCount, evaluated);
        double noneRate = 1.0 - signalRate;
        double balAcc3 = balancedAccuracy(actualUp, actualUpCorrect, actualNone, actualNoneCorrect, actualDown, actualDownCorrect);

        return new SnapshotMetrics(
                cfg.name(),
                cfg.eventQuantile(),
                cfg.minProb(),
                evaluated,
                eventCount,
                signalCount,
                eventAcc,
                signalPrecision,
                eventCoverage,
                upProbHit,
                downProbHit,
                upPrecisionSignal,
                downPrecisionSignal,
                signalRate,
                noneRate,
                balAcc3
        );
    }

    private static void printHeader() {
        System.out.println("name,event_q,min_prob,rows_test,event_count,signal_count,event_acc,signal_precision,event_coverage,up_prob_hit,down_prob_hit,up_precision_signal,down_precision_signal,signal_rate,none_rate,bal_acc_3class");
    }

    private static void printRow(SnapshotMetrics m) {
        System.out.printf(
                "%s,%.4f,%.4f,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f%n",
                m.name(),
                m.eventQ(),
                m.minProb(),
                m.rowsTest(),
                m.eventCount(),
                m.signalCount(),
                m.eventAcc(),
                m.signalPrecision(),
                m.eventCoverage(),
                m.upProbHit(),
                m.downProbHit(),
                m.upPrecisionSignal(),
                m.downPrecisionSignal(),
                m.signalRate(),
                m.noneRate(),
                m.balAcc3Class()
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

    private static double balancedAccuracy(
            int actualUp,
            int actualUpCorrect,
            int actualNone,
            int actualNoneCorrect,
            int actualDown,
            int actualDownCorrect
    ) {
        double ru = ratio(actualUpCorrect, actualUp);
        double rn = ratio(actualNoneCorrect, actualNone);
        double rd = ratio(actualDownCorrect, actualDown);
        int classes = 0;
        double sum = 0.0;
        if (!Double.isNaN(ru)) {
            classes++;
            sum += ru;
        }
        if (!Double.isNaN(rn)) {
            classes++;
            sum += rn;
        }
        if (!Double.isNaN(rd)) {
            classes++;
            sum += rd;
        }
        return classes == 0 ? Double.NaN : sum / classes;
    }

    private static double ratio(int num, int den) {
        if (den == 0) {
            return Double.NaN;
        }
        return (double) num / den;
    }

    private record SnapshotMetrics(
            String name,
            double eventQ,
            double minProb,
            int rowsTest,
            int eventCount,
            int signalCount,
            double eventAcc,
            double signalPrecision,
            double eventCoverage,
            double upProbHit,
            double downProbHit,
            double upPrecisionSignal,
            double downPrecisionSignal,
            double signalRate,
            double noneRate,
            double balAcc3Class
    ) {
    }
}

