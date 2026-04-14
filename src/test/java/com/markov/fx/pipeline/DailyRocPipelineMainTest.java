package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DailyRocPipelineMainTest {
    @Test
    void chooseSelectedLevels_fallsBackToDefaultWhenAltSellIsBelowReference() {
        LimitSignalCalculator.LimitRow defaultRow = new LimitSignalCalculator.LimitRow(
                LocalDate.parse("2026-04-13"),
                1.17216,
                1.1670,
                1.1756593028,
                1.1750,
                false,
                false,
                null,
                null,
                0.0,
                0.0,
                0.0,
                0.0,
                "signal=default_gamma_weighted"
        );
        LimitSignalCalculator.LimitRow altRow = new LimitSignalCalculator.LimitRow(
                LocalDate.parse("2026-04-13"),
                1.17216,
                1.1670,
                1.1675,
                1.1750,
                false,
                false,
                null,
                null,
                0.0,
                0.0,
                0.0,
                0.0,
                "signal=gamma_nearest_top1_md50_dec1"
        );

        DailyRocPipelineMain.SelectedLevels selected = DailyRocPipelineMain.chooseSelectedLevels(
                defaultRow,
                altRow,
                LimitSignalCalculator.ALT_SIGNAL
        );

        assertEquals(1.1670, selected.buyLevel());
        assertEquals(1.1756593028, selected.sellLevel());
        assertEquals("fallback_default_sell", selected.notesSuffix());
    }
}
