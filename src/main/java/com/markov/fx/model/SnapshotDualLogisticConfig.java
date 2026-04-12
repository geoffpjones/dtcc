package com.markov.fx.model;

public record SnapshotDualLogisticConfig(
        String name,
        String targetSymbol,
        String externalSymbol,
        boolean useExternal,
        double eventQuantile,
        double minProb,
        double decisionMargin,
        double dualC,
        String calibration
) {
    public static SnapshotDualLogisticConfig previousSnapshot() {
        return new SnapshotDualLogisticConfig(
                "snapshot_prev_q045_p020_noext",
                "EURUSD",
                null,
                false,
                0.45,
                0.20,
                0.0,
                1.0,
                "none"
        );
    }

    public static SnapshotDualLogisticConfig target3040Snapshot() {
        return new SnapshotDualLogisticConfig(
                "snapshot_target3040_q030_p055_usdjpy",
                "EURUSD",
                "USDJPY",
                true,
                0.30,
                0.55,
                0.0,
                0.05,
                "none"
        );
    }
}

