package com.markov.fx.model;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TreeMap;

/**
 * Incremental onBar dual-logistic model using snapshot hyperparameters and decision rules.
 * This model learns online from each new realized EURUSD move.
 */
public class OnlineSnapshotDualLogisticModel {
    private final SnapshotDualLogisticConfig config;
    private final int absRetWindowSize;
    private final OnlineBinaryLogistic upModel;
    private final OnlineBinaryLogistic downModel;
    private final Deque<Double> absRetWindow = new ArrayDeque<>();
    private final TreeMap<Instant, Double> externalCloses = new TreeMap<>();

    private Double previousTargetClose;
    private Double previousExternalCloseAtTarget;
    private double[] pendingFeatures;
    private int trainedExamples;
    private int seenTargetBars;

    public OnlineSnapshotDualLogisticModel(SnapshotDualLogisticConfig config) {
        this(config, 5_000);
    }

    public OnlineSnapshotDualLogisticModel(SnapshotDualLogisticConfig config, int absRetWindowSize) {
        this.config = config;
        this.absRetWindowSize = absRetWindowSize;
        int featureDim = config.useExternal() ? 4 : 2;
        this.upModel = new OnlineBinaryLogistic(featureDim, config.dualC());
        this.downModel = new OnlineBinaryLogistic(featureDim, config.dualC());
    }

    /**
     * Incremental update entry point. Call this for every bar across target + external symbols.
     */
    public Optional<Prediction> onBar(Bar bar) {
        if (config.useExternal() && bar.symbol().equals(config.externalSymbol())) {
            externalCloses.put(bar.timestamp(), bar.close());
            trimExternal(bar.timestamp());
            return Optional.empty();
        }
        if (!bar.symbol().equals(config.targetSymbol())) {
            return Optional.empty();
        }

        seenTargetBars++;
        if (previousTargetClose == null) {
            previousTargetClose = bar.close();
            return Optional.empty();
        }

        double realizedRet = bar.close() / previousTargetClose - 1.0;
        double threshold = currentThreshold().orElse(0.00030);

        // Train using features from previous target bar and the just-realized move.
        if (pendingFeatures != null) {
            int label = classFromReturn(realizedRet, threshold);
            upModel.update(pendingFeatures, label == 1 ? 1 : 0);
            downModel.update(pendingFeatures, label == -1 ? 1 : 0);
            trainedExamples++;
        }

        addAbsReturn(Math.abs(realizedRet));

        Optional<double[]> maybeFeatures = buildFeaturesForCurrentTarget(bar.timestamp(), realizedRet);
        previousTargetClose = bar.close();
        if (maybeFeatures.isEmpty()) {
            pendingFeatures = null;
            return Optional.empty();
        }

        double[] x = maybeFeatures.get();
        pendingFeatures = x;

        double pUp = calibrate(upModel.predictProb(x));
        double pDown = calibrate(downModel.predictProb(x));
        Regime decision = decide(pUp, pDown);
        return Optional.of(buildPrediction(bar.timestamp(), pUp, pDown, decision));
    }

    public SnapshotDualLogisticConfig config() {
        return config;
    }

    public int trainedExamples() {
        return trainedExamples;
    }

    public int seenTargetBars() {
        return seenTargetBars;
    }

    public OptionalDouble currentThreshold() {
        if (absRetWindow.size() < 50) {
            return OptionalDouble.empty();
        }
        List<Double> sorted = new ArrayList<>(absRetWindow);
        sorted.sort(Comparator.naturalOrder());
        int idx = (int) Math.floor((sorted.size() - 1) * config.eventQuantile());
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return OptionalDouble.of(sorted.get(idx));
    }

    private Optional<double[]> buildFeaturesForCurrentTarget(Instant targetTs, double eurRet1) {
        if (!config.useExternal()) {
            return Optional.of(new double[]{eurRet1, Math.abs(eurRet1)});
        }

        Map.Entry<Instant, Double> extAtOrBefore = externalCloses.floorEntry(targetTs);
        if (extAtOrBefore == null) {
            return Optional.empty();
        }
        double extCloseNow = extAtOrBefore.getValue();
        if (previousExternalCloseAtTarget == null || previousExternalCloseAtTarget <= 0.0) {
            previousExternalCloseAtTarget = extCloseNow;
            return Optional.empty();
        }

        double extRet1 = extCloseNow / previousExternalCloseAtTarget - 1.0;
        previousExternalCloseAtTarget = extCloseNow;
        return Optional.of(new double[]{eurRet1, Math.abs(eurRet1), extRet1, eurRet1 - extRet1});
    }

    private Prediction buildPrediction(Instant ts, double pUpRaw, double pDownRaw, Regime decision) {
        double pNoneRaw = Math.max(0.0, 1.0 - Math.max(pUpRaw, pDownRaw));
        double sum = pUpRaw + pDownRaw + pNoneRaw;
        double pUp = pUpRaw / sum;
        double pDown = pDownRaw / sum;
        double pNone = pNoneRaw / sum;

        Map<Regime, Double> probs = new EnumMap<>(Regime.class);
        probs.put(Regime.UP, pUp);
        probs.put(Regime.NONE, pNone);
        probs.put(Regime.DOWN, pDown);
        return new Prediction(ts, probs, decision);
    }

    private double calibrate(double p) {
        // Snapshot presets currently use uncalibrated probabilities. Keep hook for parity with python sweeps.
        if ("none".equalsIgnoreCase(config.calibration())) {
            return clamp01(p);
        }
        return clamp01(p);
    }

    private Regime decide(double pUp, double pDown) {
        double gap = Math.abs(pUp - pDown);
        if (pUp >= config.minProb() && pUp > pDown && gap >= config.decisionMargin()) {
            return Regime.UP;
        }
        if (pDown >= config.minProb() && pDown > pUp && gap >= config.decisionMargin()) {
            return Regime.DOWN;
        }
        return Regime.NONE;
    }

    private int classFromReturn(double ret, double threshold) {
        if (ret > threshold) {
            return 1;
        }
        if (ret < -threshold) {
            return -1;
        }
        return 0;
    }

    private void addAbsReturn(double absRet) {
        absRetWindow.addLast(absRet);
        while (absRetWindow.size() > absRetWindowSize) {
            absRetWindow.removeFirst();
        }
    }

    private void trimExternal(Instant latestTs) {
        Instant cutoff = latestTs.minusSeconds(7L * 24 * 60 * 60);
        while (!externalCloses.isEmpty() && externalCloses.firstKey().isBefore(cutoff)) {
            externalCloses.pollFirstEntry();
        }
    }

    private static double clamp01(double p) {
        if (p < 0.0) {
            return 0.0;
        }
        if (p > 1.0) {
            return 1.0;
        }
        return p;
    }

    private static final class OnlineBinaryLogistic {
        private final double[] w;
        private double b;
        private final double lambda;
        private final double lr0;
        private long steps;

        private OnlineBinaryLogistic(int dim, double c) {
            this.w = new double[dim];
            this.lambda = 1.0 / Math.max(c, 1e-6);
            this.lr0 = 0.05;
        }

        private double predictProb(double[] x) {
            double z = b;
            for (int i = 0; i < w.length; i++) {
                z += w[i] * x[i];
            }
            return 1.0 / (1.0 + Math.exp(-z));
        }

        private void update(double[] x, int y01) {
            steps++;
            double lr = lr0 / Math.sqrt(1.0 + steps * 0.001);
            double p = predictProb(x);
            double grad = p - y01;
            for (int i = 0; i < w.length; i++) {
                double g = grad * x[i] + lambda * w[i];
                w[i] -= lr * g;
            }
            b -= lr * grad;
        }
    }
}

