package com.markov.fx.pipeline;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LimitSignalParityTest {
    @TempDir
    Path tempDir;

    @Test
    void javaMatchesPythonForDefaultSignal() throws Exception {
        compareWindow(
                Path.of("data/na_o_van_only/eurusd_gamma_proxy_by_strike_call_put.csv"),
                Path.of("tick_data/eurusd_1h.csv"),
                LocalDate.parse("2025-11-11"),
                LocalDate.parse("2025-11-20"),
                List.of(),
                new LimitSignalCalculator.SignalSpec(
                        "python_default",
                        LimitSignalCalculator.WeightMode.GAMMA,
                        LimitSignalCalculator.LevelMode.WEIGHTED,
                        10,
                        0.0,
                        0.0,
                        10_000.0
                )
        );
    }

    @Test
    void javaMatchesPythonForAlternateSignal() throws Exception {
        compareWindow(
                Path.of("data/na_o_van_only/usdcad_gamma_proxy_by_strike_call_put.csv"),
                Path.of("tick_data/usdcad_1h.csv"),
                LocalDate.parse("2025-11-11"),
                LocalDate.parse("2025-11-20"),
                List.of(
                        "--weight-mode", "gamma",
                        "--level-mode", "nearest",
                        "--topn", "1",
                        "--max-distance-pips", "50",
                        "--distance-decay-power", "1",
                        "--pip-size", "0.0001"
                ),
                LimitSignalCalculator.ALT_SIGNAL
        );
    }

    private void compareWindow(
            Path strikeGamma,
            Path hourly,
            LocalDate startDate,
            LocalDate endDate,
            List<String> extraPythonArgs,
            LimitSignalCalculator.SignalSpec javaSpec
    ) throws Exception {
        Assumptions.assumeTrue(Files.exists(strikeGamma), "missing strike gamma fixture: " + strikeGamma);
        Assumptions.assumeTrue(Files.exists(hourly), "missing hourly fixture: " + hourly);

        Path pyOut = tempDir.resolve("python.csv");
        Path javaOut = tempDir.resolve("java.csv");

        List<String> cmd = new ArrayList<>();
        cmd.add("python3");
        cmd.add("scripts/backtest_walkforward_gamma_limits.py");
        cmd.add("--strike-gamma");
        cmd.add(strikeGamma.toString());
        cmd.add("--hourly");
        cmd.add(hourly.toString());
        cmd.add("--start-date");
        cmd.add(startDate.toString());
        cmd.add("--end-date");
        cmd.add(endDate.toString());
        cmd.add("--output");
        cmd.add(pyOut.toString());
        cmd.addAll(extraPythonArgs);

        Process process = new ProcessBuilder(cmd)
                .directory(Path.of(".").toAbsolutePath().normalize().toFile())
                .redirectErrorStream(true)
                .start();
        int rc = process.waitFor();
        if (rc != 0) {
            throw new AssertionError("Python comparison command failed rc=" + rc + " cmd=" + String.join(" ", cmd));
        }

        new LimitSignalCalculator().writeLimitFile(
                strikeGamma,
                hourly,
                startDate,
                endDate,
                javaSpec,
                javaOut
        );

        List<CSVRecord> pyRows = readRows(pyOut);
        List<CSVRecord> javaRows = readRows(javaOut);
        assertEquals(pyRows.size(), javaRows.size(), "row count mismatch");

        for (int i = 0; i < pyRows.size(); i++) {
            CSVRecord py = pyRows.get(i);
            CSVRecord jv = javaRows.get(i);
            assertEquals(py.get("date"), jv.get("date"), "date mismatch row " + i);
            assertSameFloat(py.get("ref_price_prev_close"), jv.get("ref_price_prev_close"), "ref row " + i);
            assertSameFloat(py.get("buy_limit"), jv.get("buy_limit"), "buy row " + i);
            assertSameFloat(py.get("sell_limit"), jv.get("sell_limit"), "sell row " + i);
            assertSameFloat(py.get("eod_close"), jv.get("eod_close"), "eod row " + i);
            assertEquals(py.get("buy_filled"), jv.get("buy_filled"), "buy_filled row " + i);
            assertEquals(py.get("sell_filled"), jv.get("sell_filled"), "sell_filled row " + i);
            assertSameInstant(py.get("buy_fill_time_utc"), jv.get("buy_fill_time_utc"), "buy_fill_time row " + i);
            assertSameInstant(py.get("sell_fill_time_utc"), jv.get("sell_fill_time_utc"), "sell_fill_time row " + i);
            assertSameFloat(py.get("buy_pnl_pips"), jv.get("buy_pnl_pips"), "buy pnl row " + i);
            assertSameFloat(py.get("sell_pnl_pips"), jv.get("sell_pnl_pips"), "sell pnl row " + i);
            assertSameFloat(py.get("net_pnl_pips"), jv.get("net_pnl_pips"), "net pnl row " + i);
        }
    }

    private List<CSVRecord> readRows(Path csv) throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(csv);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            return parser.getRecords();
        }
    }

    private void assertSameFloat(String expected, String actual, String label) {
        if ((expected == null || expected.isBlank()) && (actual == null || actual.isBlank())) {
            return;
        }
        double e = Double.parseDouble(expected);
        double a = Double.parseDouble(actual);
        assertEquals(e, a, 1e-9, label);
    }

    private void assertSameInstant(String expected, String actual, String label) {
        if ((expected == null || expected.isBlank()) && (actual == null || actual.isBlank())) {
            return;
        }
        assertEquals(Instant.parse(expected), Instant.parse(actual), label);
    }
}
