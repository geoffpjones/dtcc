package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineConfigTest {
    @TempDir
    Path tempDir;

    @Test
    void parse_readsExplicitFlags() {
        PipelineConfig cfg = PipelineConfig.parse(new String[]{
                "--project-root", "/tmp/project",
                "--pairs", "EURUSD,USDJPY",
                "--report-date", "2026-04-11",
                "--topn", "7",
                "--vol-assumption", "0.2",
                "--report-only", "true",
                "--signal-selection", "config/signal_selection.csv",
                "--max-signal-staleness-days", "6"
        });

        assertEquals("/tmp/project/data/market-bars-5y.db", cfg.dbPath());
        assertEquals(java.util.List.of("EURUSD", "USDJPY"), cfg.pairs());
        assertEquals("/tmp/project/config/signal_selection.csv", cfg.signalSelectionPath().toString());
        assertEquals(true, cfg.reportOnly());
        assertEquals(6, cfg.maxSignalStalenessDays());
        assertEquals(7, cfg.topn());
        assertEquals(0.2, cfg.volAssumption());
        assertEquals("2026-04-11", cfg.reportDate().toString());
    }

    @Test
    void parse_rejectsInvalidTopn() {
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.parse(new String[]{
                "--topn", "0"
        }));
    }

    @Test
    void parse_rejectsInvalidPairUniverse() {
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.parse(new String[]{
                "--pairs", "EURUSD,USD-JPY"
        }));
    }

    @Test
    void parse_rejectsNegativeStaleness() {
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.parse(new String[]{
                "--max-signal-staleness-days", "-1"
        }));
    }

    @Test
    void parse_usesRepoSignalSelectionByDefaultWhenPresent() throws Exception {
        Path projectRoot = tempDir.resolve("project");
        Files.createDirectories(projectRoot.resolve("config"));
        Files.writeString(projectRoot.resolve("config/signal_selection.csv"), "pair,selected_signal\nEURUSD,default_gamma_weighted\n");

        PipelineConfig cfg = PipelineConfig.parse(new String[]{
                "--project-root", projectRoot.toString()
        });

        assertEquals(java.util.List.of("EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "USDJPY"), cfg.pairs());
        assertEquals(projectRoot.resolve("config/signal_selection.csv").toString(), cfg.signalSelectionPath().toString());
    }

}
