package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GammaRecalculatorTest {
    @TempDir
    Path tempDir;

    @Test
    void recalculate_appliesNewTradesFromNextDay() throws Exception {
        Path tickDir = tempDir.resolve("tick");
        Path outDir = tempDir.resolve("out");
        Files.createDirectories(tickDir);
        Files.createDirectories(outDir);

        Path hourly = tickDir.resolve("eurusd_1h.csv");
        Files.writeString(hourly, String.join("\n",
                "timestamp_utc,open,high,low,close,volume",
                "2026-02-01T23:00:00Z,1.1000,1.1010,1.0990,1.1000,1",
                "2026-02-02T23:00:00Z,1.1100,1.1110,1.1090,1.1100,1",
                "2026-02-03T23:00:00Z,1.1200,1.1210,1.1190,1.1200,1"
        ));

        Path options = tempDir.resolve("options.csv");
        Files.writeString(options, String.join("\n",
                "Action type,UPI FISN,Strike Price,Expiration Date,source_date,Event timestamp,Notional currency-Leg 1,Notional amount-Leg 1,Notional currency-Leg 2,Notional amount-Leg 2,Embedded Option type,Option Type,Option Style,Product name",
                "NEWT,FX OPTION VANILLA EUR USD Call,1.0900,2026-02-03,2026-02-01,2026-02-01T12:00:00Z,EUR,1000000,USD,0,Vanilla,Call,European,FX Option"
        ));

        new GammaRecalculator().recalculate(List.of("EURUSD"), options, tickDir, outDir, 0.10, false);

        Path dailyOut = outDir.resolve("eurusd_gamma_proxy_daily_call_put.csv");
        List<String> lines = Files.readAllLines(dailyOut);
        assertEquals(4, lines.size());
        List<String> day1 = Arrays.asList(lines.get(1).split(",", -1));
        List<String> day2 = Arrays.asList(lines.get(2).split(",", -1));

        assertEquals("2026-02-01", day1.get(0));
        assertEquals("1000000.00", day1.get(2));
        assertEquals("0.00", day1.get(3));
        assertEquals("0.00", day1.get(6));
        assertEquals("0.00", day1.get(7));
        assertEquals("0.00", day1.get(8));

        assertEquals("2026-02-02", day2.get(0));
        assertEquals("0.00", day2.get(2));
        assertEquals("0.00", day2.get(3));
        assertEquals("1000000.00", day2.get(6));
        assertEquals("0.00", day2.get(7));
        assertEquals("1000000.00", day2.get(8));

        Path strikeOut = outDir.resolve("eurusd_gamma_proxy_by_strike_call_put.csv");
        List<String> strikeLines = Files.readAllLines(strikeOut);
        assertEquals(3, strikeLines.size());
        List<String> strikeDay2 = Arrays.asList(strikeLines.get(1).split(",", -1));
        assertEquals("2026-02-02", strikeDay2.get(0));
        assertEquals("1.090000", strikeDay2.get(2));
        assertEquals("1000000.00", strikeDay2.get(3));
        assertEquals("0.00", strikeDay2.get(4));
    }
}
