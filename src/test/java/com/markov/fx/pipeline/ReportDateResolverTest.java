package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportDateResolverTest {
    @TempDir
    Path tempDir;

    @Test
    void resolveEffectiveSignalDate_usesLatestCommonAvailableDate() throws Exception {
        Path hourly = tempDir.resolve("eurusd_1h.csv");
        Files.writeString(hourly, String.join("\n",
                "timestamp_utc,open,high,low,close,volume",
                "2026-04-10T23:00:00Z,1.13,1.14,1.12,1.135,1",
                "2026-04-12T23:00:00Z,1.16,1.17,1.15,1.167,1"
        ));
        Path gamma = tempDir.resolve("eurusd_gamma.csv");
        Files.writeString(gamma, String.join("\n",
                "date,spot_close,strike,active_call_notional,active_put_notional,call_gamma_abs_per_usd,put_gamma_abs_per_usd,total_gamma_abs_per_usd,dist_spot_to_strike,dist_spot_to_strike_pct",
                "2026-04-10,1.135000,1.120000,1,0,10,0,10,0.015,0.01"
        ));

        var resolved = ReportDateResolver.resolveEffectiveSignalDate(
                LocalDate.parse("2026-04-13"),
                hourly,
                gamma
        );

        assertTrue(resolved.isPresent());
        assertEquals(LocalDate.parse("2026-04-10"), resolved.get());
    }
}
