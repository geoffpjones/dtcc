package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PairSignalSelectionTest {
    @TempDir
    Path tempDir;

    @Test
    void defaultOnly_fallsBackToLegacySignal() {
        PairSignalSelection selection = PairSignalSelection.defaultOnly();
        assertEquals(LimitSignalCalculator.DEFAULT_SIGNAL.name(), selection.selectedFor("EURUSD").name());
        assertEquals(LimitSignalCalculator.DEFAULT_SIGNAL.name(), selection.selectedFor("NZDUSD").name());
    }

    @Test
    void load_readsExplicitPerPairChoices() throws Exception {
        Path csv = tempDir.resolve("signal_selection.csv");
        Files.writeString(csv, String.join("\n",
                "pair,selected_signal",
                "EURUSD,gamma_nearest_top1_md50_dec1",
                "AUDUSD,default_gamma_weighted"
        ));

        PairSignalSelection selection = PairSignalSelection.load(csv);
        assertEquals(LimitSignalCalculator.ALT_SIGNAL.name(), selection.selectedFor("EURUSD").name());
        assertEquals(LimitSignalCalculator.DEFAULT_SIGNAL.name(), selection.selectedFor("AUDUSD").name());
        assertEquals(LimitSignalCalculator.DEFAULT_SIGNAL.name(), selection.selectedFor("GBPUSD").name());
    }

    @Test
    void load_rejectsUnknownSignalName() throws Exception {
        Path csv = tempDir.resolve("signal_selection.csv");
        Files.writeString(csv, String.join("\n",
                "pair,selected_signal",
                "EURUSD,unknown_signal"
        ));

        assertThrows(IllegalArgumentException.class, () -> PairSignalSelection.load(csv));
    }
}
