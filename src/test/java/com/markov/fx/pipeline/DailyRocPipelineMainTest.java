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

    @Test
    void tradeLevels_usesConfiguredPipDistances() {
        DailyRocPipelineMain.TradeLevels levels = DailyRocPipelineMain.tradeLevels(
                new DailyRocPipelineMain.SelectedLevels(1.1000, 1.2000, null),
                new ExitParamSelection.ExitParams("trail_after_tp", 20.0, 30.0, 5.0),
                0.0001
        );

        assertEquals(1.1020, levels.buyTp(), 1e-9);
        assertEquals(1.0970, levels.buySl(), 1e-9);
        assertEquals(1.1980, levels.sellTp(), 1e-9);
        assertEquals(1.2030, levels.sellSl(), 1e-9);
    }
}
