package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineConfigTest {
    @Test
    void parse_readsExplicitFlags() {
        PipelineConfig cfg = PipelineConfig.parse(new String[]{
                "--project-root", "/tmp/project",
                "--report-date", "2026-04-11",
                "--topn", "7",
                "--vol-assumption", "0.2"
        });

        assertEquals("/tmp/project/data/market-bars-5y.db", cfg.dbPath());
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

}
