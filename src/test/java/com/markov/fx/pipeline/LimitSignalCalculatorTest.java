package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LimitSignalCalculatorTest {
    @TempDir
    Path tempDir;

    @Test
    void buildRows_usesPrevCloseAndWeightedTopNWithoutLookahead() throws Exception {
        Path hourly = tempDir.resolve("eurusd_1h.csv");
        Files.writeString(hourly, String.join("\n",
                "timestamp_utc,open,high,low,close,volume",
                "2026-02-01T22:00:00Z,1.1000,1.1010,1.0990,1.1000,1",
                "2026-02-01T23:00:00Z,1.1000,1.1010,1.0990,1.1000,1",
                "2026-02-02T10:00:00Z,1.1005,1.1020,1.0960,1.1010,1",
                "2026-02-02T23:00:00Z,1.1010,1.1040,1.1000,1.1030,1"
        ));
        Path strike = tempDir.resolve("eurusd_gamma_proxy_by_strike_call_put.csv");
        Files.writeString(strike, String.join("\n",
                "date,spot_close,strike,active_call_notional,active_put_notional,call_gamma_abs_per_usd,put_gamma_abs_per_usd,total_gamma_abs_per_usd,dist_spot_to_strike,dist_spot_to_strike_pct",
                "2026-02-02,1.103000,1.095000,1000000.00,0.00,20.0,0.0,20.0,0.008000,0.007253",
                "2026-02-02,1.103000,1.099000,1000000.00,0.00,10.0,0.0,10.0,0.004000,0.003626",
                "2026-02-02,1.103000,1.108000,0.00,1000000.00,0.0,15.0,15.0,0.005000,0.004534",
                "2026-02-02,1.103000,1.110000,0.00,1000000.00,0.0,5.0,5.0,0.007000,0.006346"
        ));

        LimitSignalCalculator calculator = new LimitSignalCalculator();
        List<LimitSignalCalculator.LimitRow> rows = calculator.buildRowsFromFiles(
                strike, hourly, LocalDate.parse("2026-02-02"), LocalDate.parse("2026-02-02"), 2
        );

        assertEquals(1, rows.size());
        LimitSignalCalculator.LimitRow row = rows.get(0);
        assertEquals(1.1, row.refPricePrevClose(), 1e-9);
        assertEquals((1.095 * 20.0 + 1.099 * 10.0) / 30.0, row.buyLimit(), 1e-9);
        assertEquals((1.108 * 15.0 + 1.110 * 5.0) / 20.0, row.sellLimit(), 1e-9);
        assertEquals(1.1030, row.eodClose(), 1e-9);
        assertEquals(true, row.buyFilled());
        assertEquals(false, row.sellFilled());
        assertEquals((1.1030 - row.buyLimit()) * 10_000.0, row.buyPnlPips(), 1e-6);
        assertEquals(row.buyPnlPips(), row.netPnlPips(), 1e-6);
    }

    @Test
    void writeLimitFile_writesMissingRowWhenNoPriorCloseExists() throws Exception {
        Path hourly = tempDir.resolve("gbpusd_1h.csv");
        Files.writeString(hourly, String.join("\n",
                "timestamp_utc,open,high,low,close,volume",
                "2026-03-01T23:00:00Z,1.2500,1.2510,1.2490,1.2500,1"
        ));
        Path strike = tempDir.resolve("gbpusd_gamma_proxy_by_strike_call_put.csv");
        Files.writeString(strike, String.join("\n",
                "date,spot_close,strike,active_call_notional,active_put_notional,call_gamma_abs_per_usd,put_gamma_abs_per_usd,total_gamma_abs_per_usd,dist_spot_to_strike,dist_spot_to_strike_pct",
                "2026-03-01,1.250000,1.245000,1000000.00,0.00,10.0,0.0,10.0,0.005000,0.004000"
        ));
        Path out = tempDir.resolve("out.csv");

        new LimitSignalCalculator().writeLimitFile(
                strike, hourly, LocalDate.parse("2026-03-01"), LocalDate.parse("2026-03-01"), 1, out
        );

        List<String> lines = Files.readAllLines(out);
        assertEquals(2, lines.size());
        String row = lines.get(1);
        assertEquals("2026-03-01,2026-03,,,,,0,0,,,0,0,0,0,missing_hourly_or_strike_gamma_or_ref|signal=ad_hoc_default", row);
    }

    @Test
    void buildRowsFromFiles_supportsAlternateNearestSignal() throws Exception {
        Path hourly = tempDir.resolve("usdcad_1h.csv");
        Files.writeString(hourly, String.join("\n",
                "timestamp_utc,open,high,low,close,volume",
                "2026-03-01T23:00:00Z,1.4000,1.4010,1.3990,1.4000,1",
                "2026-03-02T10:00:00Z,1.4000,1.4015,1.3960,1.3990,1",
                "2026-03-02T23:00:00Z,1.3990,1.4020,1.3980,1.4010,1"
        ));
        Path strike = tempDir.resolve("usdcad_gamma_proxy_by_strike_call_put.csv");
        Files.writeString(strike, String.join("\n",
                "date,spot_close,strike,active_call_notional,active_put_notional,call_gamma_abs_per_usd,put_gamma_abs_per_usd,total_gamma_abs_per_usd,dist_spot_to_strike,dist_spot_to_strike_pct",
                "2026-03-02,1.401000,1.395000,0.00,0.00,20.0,0.0,20.0,0.006000,0.004283",
                "2026-03-02,1.401000,1.398000,0.00,0.00,18.0,0.0,18.0,0.003000,0.002141",
                "2026-03-02,1.401000,1.404000,0.00,0.00,0.0,9.0,9.0,0.003000,0.002141"
        ));

        LimitSignalCalculator calculator = new LimitSignalCalculator();
        List<LimitSignalCalculator.LimitRow> rows = calculator.buildRowsFromFiles(
                strike,
                hourly,
                LocalDate.parse("2026-03-02"),
                LocalDate.parse("2026-03-02"),
                LimitSignalCalculator.ALT_SIGNAL
        );

        assertEquals(1, rows.size());
        LimitSignalCalculator.LimitRow row = rows.get(0);
        assertEquals(1.3980, row.buyLimit(), 1e-9);
        assertEquals(1.4040, row.sellLimit(), 1e-9);
    }
}
