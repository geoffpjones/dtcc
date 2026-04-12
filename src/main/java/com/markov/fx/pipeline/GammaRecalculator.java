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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

public class GammaRecalculator {
    private static final Logger LOG = Logger.getLogger(GammaRecalculator.class.getName());

    public void recalculate(
            List<String> pairs,
            Path optionsInput,
            Path tickDir,
            Path outDir,
            double volAssumption,
            boolean includeExotics
    ) throws IOException {
        Files.createDirectories(outDir);

        // Daily gamma is anchored to end-of-day spot by calendar date, using the final hourly close
        // observed for each UTC day as the spot reference.
        Map<String, Map<LocalDate, Double>> spotDaily = new LinkedHashMap<>();
        for (String pair : pairs) {
            Path hourlyPath = tickDir.resolve(pair.toLowerCase(Locale.ROOT) + "_1h.csv");
            if (!Files.exists(hourlyPath)) {
                LOG.warning("Skipping " + pair + ": missing hourly file " + hourlyPath);
                continue;
            }
            Map<LocalDate, Double> parsed = parseSpotDaily(hourlyPath);
            if (!parsed.isEmpty()) {
                spotDaily.put(pair, parsed);
            }
        }
        if (spotDaily.isEmpty()) {
            throw new IOException("No pairs with hourly spot data found in " + tickDir);
        }

        Map<String, PairBook> books = new LinkedHashMap<>();
        for (String pair : spotDaily.keySet()) {
            books.put(pair, new PairBook());
        }

        Map<String, List<String>> pairTokens = new HashMap<>();
        for (String pair : spotDaily.keySet()) {
            String a = pair.substring(0, 3);
            String b = pair.substring(3, 6);
            pairTokens.put(pair, List.of((a + " " + b).toUpperCase(Locale.ROOT), (b + " " + a).toUpperCase(Locale.ROOT)));
        }

        try (BufferedReader reader = Files.newBufferedReader(optionsInput);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord row : parser) {
                String upi = text(row, "UPI FISN");
                String upiUpper = upi.toUpperCase(Locale.ROOT);
                if (!"NEWT".equals(text(row, "Action type"))) {
                    continue;
                }
                if (!includeExotics && !isVanilla(upiUpper)) {
                    continue;
                }

                String side = optionSide(upiUpper);
                if (side == null) {
                    continue;
                }

                String matchedPair = null;
                for (Map.Entry<String, List<String>> e : pairTokens.entrySet()) {
                    if (e.getValue().stream().anyMatch(upiUpper::contains)) {
                        matchedPair = e.getKey();
                        break;
                    }
                }
                if (matchedPair == null) {
                    continue;
                }

                double strike = toDouble(text(row, "Strike Price"));
                LocalDate expiry = parseDay(text(row, "Expiration Date"));
                LocalDate start = firstNonNull(parseDay(text(row, "source_date")), parseDay(text(row, "Event timestamp")));
                if (strike <= 0.0 || expiry == null || start == null || expiry.isBefore(start)) {
                    continue;
                }

                String ccyA = matchedPair.substring(0, 3);
                String ccyB = matchedPair.substring(3, 6);
                // Exposure proxy assumption:
                // use notionals from either leg when denominated in one of the pair currencies.
                // This preserves relative strike weights without trying to fully normalize premium/exposure.
                double notional = pairCurrencyNotional(row, ccyA, ccyB);
                if (notional <= 0.0) {
                    continue;
                }

                StrikeKey key = new StrikeKey(side, round6(strike), expiry);
                PairBook book = books.get(matchedPair);
                book.add(start, key, notional);
                book.roll(expiry, key, notional);
            }
        }

        for (Map.Entry<String, Map<LocalDate, Double>> e : spotDaily.entrySet()) {
            writeOutputs(e.getKey(), e.getValue(), books.get(e.getKey()), outDir, volAssumption);
        }
    }

    private void writeOutputs(
            String pair,
            Map<LocalDate, Double> spotDaily,
            PairBook book,
            Path outDir,
            double volAssumption
    ) throws IOException {
        if (spotDaily.isEmpty()) {
            return;
        }

        LocalDate startDay = book.addsByDay.isEmpty()
                ? minDate(spotDaily.keySet())
                : minDate(union(book.addsByDay.keySet(), spotDaily.keySet()));
        LocalDate endDay = book.rollsByDay.isEmpty()
                ? maxDate(spotDaily.keySet())
                : maxDate(union(book.rollsByDay.keySet(), spotDaily.keySet()));

        Path dailyOut = outDir.resolve(pair.toLowerCase(Locale.ROOT) + "_gamma_proxy_daily_call_put.csv");
        Path strikeOut = outDir.resolve(pair.toLowerCase(Locale.ROOT) + "_gamma_proxy_by_strike_call_put.csv");

        try (BufferedWriter dailyWriter = Files.newBufferedWriter(dailyOut);
             BufferedWriter strikeWriter = Files.newBufferedWriter(strikeOut);
             CSVPrinter dailyCsv = new CSVPrinter(dailyWriter, CSVFormat.DEFAULT);
             CSVPrinter strikeCsv = new CSVPrinter(strikeWriter, CSVFormat.DEFAULT)) {

            dailyCsv.printRecord(
                    "date", "spot_close",
                    "added_call_notional", "added_put_notional",
                    "expiring_call_notional", "expiring_put_notional",
                    "active_call_notional", "active_put_notional", "active_total_notional",
                    "call_gamma_abs_per_usd", "put_gamma_abs_per_usd", "total_gamma_abs_per_usd"
            );
            strikeCsv.printRecord(
                    "date", "spot_close", "strike",
                    "active_call_notional", "active_put_notional",
                    "call_gamma_abs_per_usd", "put_gamma_abs_per_usd", "total_gamma_abs_per_usd",
                    "dist_spot_to_strike", "dist_spot_to_strike_pct"
            );

            Map<StrikeKey, Double> active = new HashMap<>();
            for (LocalDate d = startDay; !d.isAfter(endDay); d = d.plusDays(1)) {
                Double spot = spotDaily.get(d);

                double addCall = sumForSide(book.addsByDay.getOrDefault(d, Map.of()), "Call");
                double addPut = sumForSide(book.addsByDay.getOrDefault(d, Map.of()), "Put");
                double expCall = sumForSide(book.rollsByDay.getOrDefault(d, Map.of()), "Call");
                double expPut = sumForSide(book.rollsByDay.getOrDefault(d, Map.of()), "Put");

                if (spot != null && spot > 0.0) {
                    double callGamma = 0.0;
                    double putGamma = 0.0;
                    Map<Double, Double> byCallNotional = new TreeMap<>();
                    Map<Double, Double> byPutNotional = new TreeMap<>();
                    Map<Double, Double> byCallGamma = new TreeMap<>();
                    Map<Double, Double> byPutGamma = new TreeMap<>();

                    for (Map.Entry<StrikeKey, Double> activeEntry : active.entrySet()) {
                        StrikeKey key = activeEntry.getKey();
                        double notional = activeEntry.getValue();
                        if (notional == 0.0) {
                            continue;
                        }
                        long tauDays = key.expiry().toEpochDay() - d.toEpochDay();
                        if (tauDays < 0) {
                            continue;
                        }
                        // Tenor floor avoids singular behaviour on exact expiry date.
                        double tYears = Math.max(tauDays / 365.0, 1.0 / 365.0);
                        double gamma = Math.abs(notional * bsGamma(spot, key.strike(), tYears, volAssumption));
                        if ("Call".equals(key.side())) {
                            callGamma += gamma;
                            byCallNotional.merge(key.strike(), notional, Double::sum);
                            byCallGamma.merge(key.strike(), gamma, Double::sum);
                        } else {
                            putGamma += gamma;
                            byPutNotional.merge(key.strike(), notional, Double::sum);
                            byPutGamma.merge(key.strike(), gamma, Double::sum);
                        }
                    }

                    double activeCall = sumActiveForSide(active, "Call");
                    double activePut = sumActiveForSide(active, "Put");
                    dailyCsv.printRecord(
                            d,
                            fmt6(spot),
                            fmt2(addCall), fmt2(addPut),
                            fmt2(expCall), fmt2(expPut),
                            fmt2(activeCall), fmt2(activePut), fmt2(activeCall + activePut),
                            fmt6(callGamma), fmt6(putGamma), fmt6(callGamma + putGamma)
                    );

                    Set<Double> strikes = new TreeSet<>();
                    strikes.addAll(byCallNotional.keySet());
                    strikes.addAll(byPutNotional.keySet());
                    for (Double strike : strikes) {
                        // Strike-level output preserves the call/put split so downstream limit logic
                        // can separately weight support-like and resistance-like gamma clusters.
                        double callStrikeGamma = byCallGamma.getOrDefault(strike, 0.0);
                        double putStrikeGamma = byPutGamma.getOrDefault(strike, 0.0);
                        double dist = Math.abs(spot - strike);
                        strikeCsv.printRecord(
                                d,
                                fmt6(spot),
                                fmt6(strike),
                                fmt2(byCallNotional.getOrDefault(strike, 0.0)),
                                fmt2(byPutNotional.getOrDefault(strike, 0.0)),
                                fmt6(callStrikeGamma),
                                fmt6(putStrikeGamma),
                                fmt6(callStrikeGamma + putStrikeGamma),
                                fmt6(dist),
                                fmt6(dist / spot)
                        );
                    }
                }

                // day-T uses start-of-day inventory; rolls and new trades apply to T+1
                for (Map.Entry<StrikeKey, Double> rollEntry : book.rollsByDay.getOrDefault(d, Map.of()).entrySet()) {
                    double next = active.getOrDefault(rollEntry.getKey(), 0.0) - rollEntry.getValue();
                    if (Math.abs(next) < 1e-9) {
                        active.remove(rollEntry.getKey());
                    } else {
                        active.put(rollEntry.getKey(), next);
                    }
                }
                for (Map.Entry<StrikeKey, Double> addEntry : book.addsByDay.getOrDefault(d, Map.of()).entrySet()) {
                    active.merge(addEntry.getKey(), addEntry.getValue(), Double::sum);
                }
            }
        }

        LOG.info("Wrote gamma outputs for " + pair + ": " + dailyOut + " and " + strikeOut);
    }

    private static Map<LocalDate, Double> parseSpotDaily(Path hourlyPath) throws IOException {
        Map<LocalDate, SpotPoint> latest = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(hourlyPath);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord row : parser) {
                Instant ts = Instant.parse(row.get("timestamp_utc"));
                LocalDate day = ts.atZone(ZoneOffset.UTC).toLocalDate();
                double close = Double.parseDouble(row.get("close"));
                SpotPoint prev = latest.get(day);
                if (prev == null || ts.isAfter(prev.ts())) {
                    latest.put(day, new SpotPoint(ts, close));
                }
            }
        }
        Map<LocalDate, Double> out = new TreeMap<>();
        for (Map.Entry<LocalDate, SpotPoint> e : latest.entrySet()) {
            out.put(e.getKey(), e.getValue().close());
        }
        return out;
    }

    private static double pairCurrencyNotional(CSVRecord row, String ccyA, String ccyB) {
        double total = 0.0;
        String c1 = text(row, "Notional currency-Leg 1").toUpperCase(Locale.ROOT);
        String c2 = text(row, "Notional currency-Leg 2").toUpperCase(Locale.ROOT);
        if (c1.equals(ccyA) || c1.equals(ccyB)) {
            total += toDouble(text(row, "Notional amount-Leg 1"));
        }
        if (c2.equals(ccyA) || c2.equals(ccyB)) {
            total += toDouble(text(row, "Notional amount-Leg 2"));
        }
        return total;
    }

    private static String optionSide(String upiUpper) {
        if (upiUpper.contains("CALL")) {
            return "Call";
        }
        if (upiUpper.contains("PUT")) {
            return "Put";
        }
        return null;
    }

    private static boolean isVanilla(String upiUpper) {
        return upiUpper.contains(" VAN ") || upiUpper.contains("VANILLA");
    }

    private static LocalDate parseDay(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(value.substring(0, 10));
        } catch (Exception e) {
            return null;
        }
    }

    private static String text(CSVRecord row, String header) {
        return row.isMapped(header) ? row.get(header).trim() : "";
    }

    private static double toDouble(String value) {
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(value.replace(",", "").trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static double bsGamma(double s, double k, double tYears, double sigma) {
        if (s <= 0.0 || k <= 0.0 || tYears <= 0.0 || sigma <= 0.0) {
            return 0.0;
        }
        double volSqrtT = sigma * Math.sqrt(tYears);
        if (volSqrtT <= 0.0) {
            return 0.0;
        }
        // Black-Scholes gamma with zero-rate simplifying assumption.
        double d1 = (Math.log(s / k) + 0.5 * sigma * sigma * tYears) / volSqrtT;
        return normPdf(d1) / (s * volSqrtT);
    }

    private static double normPdf(double x) {
        return Math.exp(-0.5 * x * x) / Math.sqrt(2.0 * Math.PI);
    }

    private static double sumForSide(Map<StrikeKey, Double> values, String side) {
        double total = 0.0;
        for (Map.Entry<StrikeKey, Double> e : values.entrySet()) {
            if (side.equals(e.getKey().side())) {
                total += e.getValue();
            }
        }
        return total;
    }

    private static double sumActiveForSide(Map<StrikeKey, Double> active, String side) {
        double total = 0.0;
        for (Map.Entry<StrikeKey, Double> e : active.entrySet()) {
            if (side.equals(e.getKey().side())) {
                total += e.getValue();
            }
        }
        return total;
    }

    private static double round6(double value) {
        return Math.round(value * 1_000_000.0) / 1_000_000.0;
    }

    private static String fmt2(double value) {
        return String.format(Locale.US, "%.2f", value);
    }

    private static String fmt6(double value) {
        return String.format(Locale.US, "%.6f", value);
    }

    private static <T> T firstNonNull(T a, T b) {
        return a != null ? a : b;
    }

    private static LocalDate minDate(Set<LocalDate> dates) {
        return dates.stream().min(Comparator.naturalOrder()).orElseThrow();
    }

    private static LocalDate maxDate(Set<LocalDate> dates) {
        return dates.stream().max(Comparator.naturalOrder()).orElseThrow();
    }

    private static Set<LocalDate> union(Set<LocalDate> a, Set<LocalDate> b) {
        Set<LocalDate> out = new HashSet<>(a);
        out.addAll(b);
        return out;
    }

    private record SpotPoint(Instant ts, double close) {
    }

    private record StrikeKey(String side, double strike, LocalDate expiry) {
    }

    private static final class PairBook {
        private final Map<LocalDate, Map<StrikeKey, Double>> addsByDay = new TreeMap<>();
        private final Map<LocalDate, Map<StrikeKey, Double>> rollsByDay = new TreeMap<>();

        void add(LocalDate day, StrikeKey key, double notional) {
            addsByDay.computeIfAbsent(day, ignored -> new HashMap<>()).merge(key, notional, Double::sum);
        }

        void roll(LocalDate day, StrikeKey key, double notional) {
            rollsByDay.computeIfAbsent(day, ignored -> new HashMap<>()).merge(key, notional, Double::sum);
        }
    }
}
