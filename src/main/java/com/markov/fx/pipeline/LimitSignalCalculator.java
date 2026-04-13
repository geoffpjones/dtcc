package com.markov.fx.pipeline;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class LimitSignalCalculator {
    public static final SignalSpec DEFAULT_SIGNAL = new SignalSpec(
            "default_gamma_weighted",
            WeightMode.GAMMA,
            LevelMode.WEIGHTED,
            10,
            0.0,
            0.0,
            10_000.0
    );
    public static final SignalSpec ALT_SIGNAL = new SignalSpec(
            "gamma_nearest_top1_md50_dec1",
            WeightMode.GAMMA,
            LevelMode.NEAREST,
            1,
            50.0,
            1.0,
            10_000.0
    );

    public List<LimitRow> buildRowsFromFiles(
            Path strikeGammaCsv,
            Path hourlyCsv,
            LocalDate startDate,
            LocalDate endDate,
            int topn
    ) throws IOException {
        return buildRowsFromFiles(
                strikeGammaCsv,
                hourlyCsv,
                startDate,
                endDate,
                new SignalSpec("ad_hoc_default", WeightMode.GAMMA, LevelMode.WEIGHTED, topn, 0.0, 0.0, 10_000.0)
        );
    }

    public List<LimitRow> buildRowsFromFiles(
            Path strikeGammaCsv,
            Path hourlyCsv,
            LocalDate startDate,
            LocalDate endDate,
            SignalSpec spec
    ) throws IOException {
        return buildRows(loadHourly(hourlyCsv), loadStrikeGamma(strikeGammaCsv), startDate, endDate, spec);
    }

    public void writeLimitFile(
            Path strikeGammaCsv,
            Path hourlyCsv,
            LocalDate startDate,
            LocalDate endDate,
            int topn,
            Path outputCsv
    ) throws IOException {
        writeLimitFile(
                strikeGammaCsv,
                hourlyCsv,
                startDate,
                endDate,
                new SignalSpec("ad_hoc_default", WeightMode.GAMMA, LevelMode.WEIGHTED, topn, 0.0, 0.0, 10_000.0),
                outputCsv
        );
    }

    public void writeLimitFile(
            Path strikeGammaCsv,
            Path hourlyCsv,
            LocalDate startDate,
            LocalDate endDate,
            SignalSpec spec,
            Path outputCsv
    ) throws IOException {
        List<LimitRow> rows = buildRowsFromFiles(strikeGammaCsv, hourlyCsv, startDate, endDate, spec);

        if (outputCsv.getParent() != null) {
            Files.createDirectories(outputCsv.getParent());
        }
        Path monthlyCsv = outputCsv.resolveSibling(stripExtension(outputCsv.getFileName().toString()) + "_monthly_summary.csv");

        try (BufferedWriter out = Files.newBufferedWriter(outputCsv);
             CSVPrinter csv = new CSVPrinter(out, CSVFormat.DEFAULT)) {
            csv.printRecord(
                    "date", "year_month", "ref_price_prev_close",
                    "buy_limit", "sell_limit", "eod_close",
                    "buy_filled", "sell_filled", "buy_fill_time_utc", "sell_fill_time_utc",
                    "buy_pnl_pips", "sell_pnl_pips", "net_pnl_pips", "cum_net_pnl_pips", "notes"
            );
            for (LimitRow row : rows) {
                csv.printRecord(
                        row.date(),
                        row.date().toString().substring(0, 7),
                        fmt(row.refPricePrevClose()),
                        fmt(row.buyLimit()),
                        fmt(row.sellLimit()),
                        fmt(row.eodClose()),
                        row.buyFilled() ? "1" : "0",
                        row.sellFilled() ? "1" : "0",
                        row.buyFillTimeUtc() == null ? "" : row.buyFillTimeUtc().toString(),
                        row.sellFillTimeUtc() == null ? "" : row.sellFillTimeUtc().toString(),
                        fmt(row.buyPnlPips()),
                        fmt(row.sellPnlPips()),
                        fmt(row.netPnlPips()),
                        fmt(row.cumNetPnlPips()),
                        row.notes()
                );
            }
        }

        Map<String, Double> monthly = new TreeMap<>();
        for (LimitRow row : rows) {
            monthly.merge(row.date().toString().substring(0, 7), row.netPnlPips(), Double::sum);
        }
        try (BufferedWriter out = Files.newBufferedWriter(monthlyCsv);
             CSVPrinter csv = new CSVPrinter(out, CSVFormat.DEFAULT)) {
            csv.printRecord("year_month", "net_pnl_pips");
            for (Map.Entry<String, Double> e : monthly.entrySet()) {
                csv.printRecord(e.getKey(), fmt(e.getValue()));
            }
        }
    }

    public List<LimitRow> buildRows(
            Map<LocalDate, List<HourBar>> hourly,
            Map<LocalDate, List<StrikeGammaRow>> strikeGamma,
            LocalDate startDate,
            LocalDate endDate,
            SignalSpec spec
    ) {
        Map<LocalDate, Double> prevClose = buildPrevClose(hourly);
        List<LimitRow> rows = new ArrayList<>();
        double cumulative = 0.0;

        for (LocalDate d = startDate; !d.isAfter(endDate); d = d.plusDays(1)) {
            List<HourBar> bars = hourly.getOrDefault(d, List.of());
            List<StrikeGammaRow> sg = strikeGamma.getOrDefault(d, List.of());
            Double ref = prevClose.get(d);

            if (bars.isEmpty() || sg.isEmpty() || ref == null || Double.isNaN(ref)) {
                rows.add(new LimitRow(d, nan(), nan(), nan(), nan(), false, false,
                        null, null, 0.0, 0.0, 0.0, cumulative, "missing_hourly_or_strike_gamma_or_ref|signal=" + spec.name()));
                continue;
            }

            // The signal uses the prior close as the only same-day reference to avoid lookahead.
            List<WeightedLevel> callBelow = new ArrayList<>();
            List<WeightedLevel> putAbove = new ArrayList<>();
            for (StrikeGammaRow row : sg) {
                double distancePips = Math.abs(row.strike() - ref) * spec.pipScale();
                if (spec.maxDistancePips() > 0.0 && distancePips > spec.maxDistancePips()) {
                    continue;
                }
                double callWeight = adjustWeight(selectWeight(row, true, spec.weightMode()), distancePips, spec.distanceDecayPower());
                double putWeight = adjustWeight(selectWeight(row, false, spec.weightMode()), distancePips, spec.distanceDecayPower());
                if (row.strike() < ref && callWeight > 0.0) {
                    callBelow.add(new WeightedLevel(row.strike(), callWeight, distancePips));
                }
                if (row.strike() > ref && putWeight > 0.0) {
                    putAbove.add(new WeightedLevel(row.strike(), putWeight, distancePips));
                }
            }

            double buy = selectLevel(callBelow, spec);
            double sell = selectLevel(putAbove, spec);
            double eod = bars.get(bars.size() - 1).close();

            boolean buyFilled = false;
            boolean sellFilled = false;
            Instant buyFillTs = null;
            Instant sellFillTs = null;
            for (HourBar bar : bars) {
                if (!Double.isNaN(buy) && !buyFilled && bar.low() <= buy && buy <= bar.high()) {
                    buyFilled = true;
                    buyFillTs = bar.ts();
                }
                if (!Double.isNaN(sell) && !sellFilled && bar.low() <= sell && sell <= bar.high()) {
                    sellFilled = true;
                    sellFillTs = bar.ts();
                }
            }

            double buyPnl = buyFilled ? (eod - buy) * 10_000.0 : 0.0;
            double sellPnl = sellFilled ? (sell - eod) * 10_000.0 : 0.0;
            double net = buyPnl + sellPnl;
            cumulative += net;
            String notes = buyFilled && sellFilled
                    ? "both_filled_intraday_order_ambiguous_with_1h_bars|signal=" + spec.name()
                    : "signal=" + spec.name();

            rows.add(new LimitRow(
                    d, ref, buy, sell, eod,
                    buyFilled, sellFilled, buyFillTs, sellFillTs,
                    buyPnl, sellPnl, net, cumulative, notes
            ));
        }
        return rows;
    }

    private Map<LocalDate, Double> buildPrevClose(Map<LocalDate, List<HourBar>> hourly) {
        Map<LocalDate, Double> prevClose = new LinkedHashMap<>();
        Double prev = null;
        List<LocalDate> sorted = new ArrayList<>(hourly.keySet());
        sorted.sort(Comparator.naturalOrder());
        for (LocalDate day : sorted) {
            prevClose.put(day, prev);
            List<HourBar> bars = hourly.get(day);
            if (bars != null && !bars.isEmpty()) {
                prev = bars.get(bars.size() - 1).close();
            }
        }
        return prevClose;
    }

    private Map<LocalDate, List<HourBar>> loadHourly(Path hourlyCsv) throws IOException {
        Map<LocalDate, List<HourBar>> out = new TreeMap<>();
        try (BufferedReader reader = Files.newBufferedReader(hourlyCsv);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord row : parser) {
                Instant ts = Instant.parse(row.get("timestamp_utc"));
                LocalDate d = ts.atZone(ZoneOffset.UTC).toLocalDate();
                out.computeIfAbsent(d, ignored -> new ArrayList<>()).add(new HourBar(
                        ts,
                        parseDouble(row.get("high")),
                        parseDouble(row.get("low")),
                        parseDouble(row.get("close"))
                ));
            }
        }
        for (List<HourBar> bars : out.values()) {
            bars.sort(Comparator.comparing(HourBar::ts));
        }
        return out;
    }

    private Map<LocalDate, List<StrikeGammaRow>> loadStrikeGamma(Path strikeGammaCsv) throws IOException {
        Map<LocalDate, List<StrikeGammaRow>> out = new TreeMap<>();
        try (BufferedReader reader = Files.newBufferedReader(strikeGammaCsv);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord row : parser) {
                LocalDate d = LocalDate.parse(row.get("date"));
                double strike = parseDouble(row.get("strike"));
                if (Double.isNaN(strike)) {
                    continue;
                }
                out.computeIfAbsent(d, ignored -> new ArrayList<>()).add(new StrikeGammaRow(
                        strike,
                        parseDoubleOrZero(row.get("active_call_notional")),
                        parseDoubleOrZero(row.get("active_put_notional")),
                        parseDoubleOrZero(row.get("call_gamma_abs_per_usd")),
                        parseDoubleOrZero(row.get("put_gamma_abs_per_usd"))
                ));
            }
        }
        return out;
    }

    private double selectLevel(List<WeightedLevel> levels, SignalSpec spec) {
        List<WeightedLevel> clean = levels.stream()
                .filter(v -> !Double.isNaN(v.price()) && !Double.isNaN(v.weight()) && v.weight() > 0.0)
                .sorted(Comparator.comparing(WeightedLevel::weight).reversed())
                .limit(spec.topn())
                .toList();
        if (clean.isEmpty()) {
            return nan();
        }
        if (spec.levelMode() == LevelMode.NEAREST) {
            return clean.stream().min(Comparator.comparing(WeightedLevel::distancePips)).orElseThrow().price();
        }
        double wsum = clean.stream().mapToDouble(WeightedLevel::weight).sum();
        if (wsum <= 0.0) {
            return nan();
        }
        double psum = clean.stream().mapToDouble(v -> v.price() * v.weight()).sum();
        return psum / wsum;
    }

    private static double selectWeight(StrikeGammaRow row, boolean callSide, WeightMode mode) {
        return switch (mode) {
            case GAMMA -> callSide ? row.callGammaAbsPerUsd() : row.putGammaAbsPerUsd();
            case NOTIONAL -> callSide ? row.activeCallNotional() : row.activePutNotional();
        };
    }

    private static double adjustWeight(double rawWeight, double distancePips, double decayPower) {
        if (rawWeight <= 0.0) {
            return 0.0;
        }
        if (decayPower <= 0.0) {
            return rawWeight;
        }
        return rawWeight / Math.pow(Math.max(distancePips, 1.0), decayPower);
    }

    private static double parseDouble(String value) {
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception e) {
            return nan();
        }
    }

    private static double parseDoubleOrZero(String value) {
        double parsed = parseDouble(value);
        return Double.isNaN(parsed) ? 0.0 : parsed;
    }

    private static String stripExtension(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx >= 0 ? fileName.substring(0, idx) : fileName;
    }

    private static String fmt(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "";
        }
        return String.format(Locale.US, "%.10f", value).replaceAll("0+$", "").replaceAll("\\.$", "");
    }

    private static double nan() {
        return Double.NaN;
    }

    public record SignalSpec(
            String name,
            WeightMode weightMode,
            LevelMode levelMode,
            int topn,
            double maxDistancePips,
            double distanceDecayPower,
            double pipScale
    ) {
    }

    public enum WeightMode {
        GAMMA,
        NOTIONAL
    }

    public enum LevelMode {
        WEIGHTED,
        NEAREST
    }

    public record LimitRow(
            LocalDate date,
            double refPricePrevClose,
            double buyLimit,
            double sellLimit,
            double eodClose,
            boolean buyFilled,
            boolean sellFilled,
            Instant buyFillTimeUtc,
            Instant sellFillTimeUtc,
            double buyPnlPips,
            double sellPnlPips,
            double netPnlPips,
            double cumNetPnlPips,
            String notes
    ) {
    }

    private record HourBar(Instant ts, double high, double low, double close) {
    }

    private record StrikeGammaRow(
            double strike,
            double activeCallNotional,
            double activePutNotional,
            double callGammaAbsPerUsd,
            double putGammaAbsPerUsd
    ) {
    }

    private record WeightedLevel(double price, double weight, double distancePips) {
    }
}
