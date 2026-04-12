package com.markov.fx.pipeline;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public record PipelineConfig(
        Path projectRoot,
        Path dataDir,
        Path tickDir,
        String dbPath,
        String pythonExec,
        LocalDate reportDate,
        LocalDate dtccBootstrapStart,
        LocalDate marketBootstrapStart,
        String dtccRegime,
        String dtccAsset,
        double volAssumption,
        int topn,
        long commandTimeoutSeconds
) {
    static PipelineConfig parse(String[] args) {
        Map<String, String> m = parseFlags(args);
        Path projectRoot = Path.of(m.getOrDefault("project-root", ".")).toAbsolutePath().normalize();
        Path dataDir = projectRoot.resolve(m.getOrDefault("data-dir", "data")).normalize();
        Path tickDir = projectRoot.resolve(m.getOrDefault("tick-dir", "tick_data")).normalize();
        String dbPath = projectRoot.resolve(m.getOrDefault("db", "data/market-bars-5y.db")).normalize().toString();
        String pythonExec = m.getOrDefault("python", "python3");
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
        long timeout = Long.parseLong(m.getOrDefault("command-timeout-sec", "7200"));

        if (topn <= 0) {
            throw new IllegalArgumentException("--topn must be > 0");
        }
        if (timeout <= 0) {
            throw new IllegalArgumentException("--command-timeout-sec must be > 0");
        }

        return new PipelineConfig(
                projectRoot,
                dataDir,
                tickDir,
                dbPath,
                pythonExec,
                reportDate,
                dtccBootstrapStart,
                marketBootstrapStart,
                dtccRegime,
                dtccAsset,
                volAssumption,
                topn,
                timeout
        );
    }

    private static Map<String, String> parseFlags(String[] args) {
        Set<String> allowed = Set.of(
                "project-root", "data-dir", "tick-dir", "db", "python",
                "report-date", "dtcc-bootstrap-start", "market-bootstrap-start",
                "dtcc-regime", "dtcc-asset", "vol-assumption", "topn",
                "command-timeout-sec"
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
}
