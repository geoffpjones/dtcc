package com.markov.fx.pipeline;

import java.nio.file.Path;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record PipelineConfig(
        Path projectRoot,
        Path dataDir,
        Path tickDir,
        String dbPath,
        List<String> pairs,
        Path signalSelectionPath,
        boolean reportOnly,
        int maxSignalStalenessDays,
        LocalDate reportDate,
        LocalDate dtccBootstrapStart,
        LocalDate marketBootstrapStart,
        String dtccRegime,
        String dtccAsset,
        double volAssumption,
        int topn
) {
    static PipelineConfig parse(String[] args) {
        Map<String, String> m = parseFlags(args);
        Path projectRoot = Path.of(m.getOrDefault("project-root", ".")).toAbsolutePath().normalize();
        Path dataDir = projectRoot.resolve(m.getOrDefault("data-dir", "data")).normalize();
        Path tickDir = projectRoot.resolve(m.getOrDefault("tick-dir", "tick_data")).normalize();
        String dbPath = projectRoot.resolve(m.getOrDefault("db", "data/market-bars-5y.db")).normalize().toString();
        List<String> pairs = parsePairs(m.getOrDefault("pairs", "EURUSD,GBPUSD,AUDUSD,USDCAD,USDJPY"));
        Path defaultSignalSelection = projectRoot.resolve("config/signal_selection.csv").normalize();
        Path signalSelectionPath = m.containsKey("signal-selection")
                ? projectRoot.resolve(m.get("signal-selection")).normalize()
                : (Files.exists(defaultSignalSelection) ? defaultSignalSelection : null);
        boolean reportOnly = Boolean.parseBoolean(m.getOrDefault("report-only", "false"));
        int maxSignalStalenessDays = Integer.parseInt(m.getOrDefault("max-signal-staleness-days", "5"));
        LocalDate reportDate = LocalDate.parse(m.getOrDefault(
                "report-date",
                LocalDate.now(ZoneOffset.UTC).minusDays(1).toString()
        ));
        LocalDate dtccBootstrapStart = LocalDate.parse(m.getOrDefault("dtcc-bootstrap-start", "2025-04-10"));
        LocalDate marketBootstrapStart = LocalDate.parse(
                m.getOrDefault("market-bootstrap-start", reportDate.minusYears(5).toString())
        );
        String dtccRegime = m.getOrDefault("dtcc-regime", "CFTC");
        String dtccAsset = m.getOrDefault("dtcc-asset", "FX");
        double volAssumption = Double.parseDouble(m.getOrDefault("vol-assumption", "0.10"));
        int topn = Integer.parseInt(m.getOrDefault("topn", "10"));

        if (topn <= 0) {
            throw new IllegalArgumentException("--topn must be > 0");
        }
        if (maxSignalStalenessDays < 0) {
            throw new IllegalArgumentException("--max-signal-staleness-days must be >= 0");
        }

        return new PipelineConfig(
                projectRoot,
                dataDir,
                tickDir,
                dbPath,
                List.copyOf(pairs),
                signalSelectionPath,
                reportOnly,
                maxSignalStalenessDays,
                reportDate,
                dtccBootstrapStart,
                marketBootstrapStart,
                dtccRegime,
                dtccAsset,
                volAssumption,
                topn
        );
    }

    private static Map<String, String> parseFlags(String[] args) {
        Set<String> allowed = Set.of(
                "project-root", "data-dir", "tick-dir", "db",
                "pairs",
                "signal-selection", "report-only", "max-signal-staleness-days",
                "report-date", "dtcc-bootstrap-start", "market-bootstrap-start",
                "dtcc-regime", "dtcc-asset", "vol-assumption", "topn"
        );
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected positional argument: " + a);
            }
            String key = a.substring(2);
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown argument: --" + key);
            }
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for --" + key);
            }
            out.put(key, args[i + 1]);
            i++;
        }
        return out;
    }

    private static List<String> parsePairs(String rawPairs) {
        List<String> out = new ArrayList<>();
        for (String token : rawPairs.split(",")) {
            String pair = token.trim().toUpperCase();
            if (pair.isEmpty()) {
                continue;
            }
            if (!pair.matches("[A-Z]{6}")) {
                throw new IllegalArgumentException("--pairs must contain comma-separated 6-letter FX symbols: " + pair);
            }
            out.add(pair);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("--pairs must contain at least one symbol");
        }
        return out;
    }
}
