package com.markov.fx.pipeline;

import com.markov.fx.ingest.DukascopyBi5Client;
import com.markov.fx.store.SqliteBarRepository;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DailyRocPipelineMain {
    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);

        Set<String> pairs = Set.of("EURUSD", "GBPUSD");
        SqliteBarRepository barRepository = new SqliteBarRepository(cfg.dbPath);
        DtccOptionTradeRepository optionRepo = new DtccOptionTradeRepository(cfg.dbPath);

        System.out.printf("Pipeline start reportDate=%s db=%s%n", cfg.reportDate, cfg.dbPath);

        ingestDtcc(cfg, pairs, optionRepo);
        updateDukascopy(cfg, pairs, barRepository);
        exportHourlyBars(cfg, pairs);

        Path optionsForGamma = cfg.dataDir.resolve("options_data_fx_pairs_for_gamma.csv");
        optionRepo.exportOptionsCsv(optionsForGamma, pairs);
        System.out.println("Exported options CSV for gamma: " + optionsForGamma);

        recalcGamma(cfg, optionsForGamma);
        Path reportCsv = produceLimitReports(cfg, pairs, optionRepo);
        System.out.println("Daily limit report written: " + reportCsv);
    }

    private static void ingestDtcc(Config cfg, Set<String> pairs, DtccOptionTradeRepository repo) throws Exception {
        DtccPublicClient client = new DtccPublicClient();
        LocalDate dtccStart = repo.latestSourceDate().map(d -> d.plusDays(1)).orElse(cfg.dtccBootstrapStart);
        LocalDate dtccEnd = cfg.reportDate;
        int files = client.ingestRange(cfg.dtccRegime, cfg.dtccAsset, dtccStart, dtccEnd, pairs, repo);
        System.out.printf("DTCC update complete start=%s end=%s files=%d%n", dtccStart, dtccEnd, files);
    }

    private static void updateDukascopy(Config cfg, Set<String> pairs, SqliteBarRepository barRepository) throws Exception {
        DukascopyDailyUpdater updater = new DukascopyDailyUpdater(new DukascopyBi5Client(), barRepository);
        for (String pair : pairs) {
            LocalDate start = barRepository.latestDate(pair).map(d -> d.plusDays(1)).orElse(cfg.marketBootstrapStart);
            long loaded = updater.updateSymbol(pair, start, cfg.reportDate);
            Optional<Instant> latest = barRepository.latestTimestamp(pair);
            System.out.printf("Dukascopy update %s loadedBars=%d latestTs=%s%n", pair, loaded, latest.orElse(null));
        }
    }

    private static void exportHourlyBars(Config cfg, Set<String> pairs) {
        HourlyBarCsvExporter exporter = new HourlyBarCsvExporter(cfg.dbPath);
        for (String pair : pairs) {
            Path out = cfg.tickDir.resolve(pair.toLowerCase() + "_1h.csv");
            int rows = exporter.exportHourly(pair, out);
            System.out.printf("Exported hourly bars %s rows=%d path=%s%n", pair, rows, out);
        }
    }

    private static void recalcGamma(Config cfg, Path optionsForGamma) throws Exception {
        List<String> cmd = List.of(
                cfg.pythonExec,
                "scripts/build_gamma_for_pairs.py",
                "--pairs", "EURUSD,GBPUSD",
                "--options-input", optionsForGamma.toString(),
                "--tick-dir", cfg.tickDir.toString(),
                "--out-dir", cfg.dataDir.toString(),
                "--vol-assumption", Double.toString(cfg.volAssumption)
        );
        runCommand(cmd, cfg.projectRoot);
    }

    private static Path produceLimitReports(Config cfg, Set<String> pairs, DtccOptionTradeRepository optionRepo) throws Exception {
        Path reportDir = cfg.dataDir.resolve("reports");
        Files.createDirectories(reportDir);
        Path reportCsv = reportDir.resolve("fx_limit_order_report_" + cfg.reportDate + ".csv");

        try (BufferedWriter w = Files.newBufferedWriter(reportCsv)) {
            w.write("date,pair,ref_price_prev_close,buy_limit,sell_limit,eod_close,notes");
            w.newLine();
            for (String pair : pairs) {
                Path pairOut = reportDir.resolve(pair.toLowerCase() + "_walkforward_limit_" + cfg.reportDate + ".csv");
                List<String> cmd = List.of(
                        cfg.pythonExec,
                        "scripts/backtest_walkforward_gamma_limits.py",
                        "--strike-gamma", cfg.dataDir.resolve(pair.toLowerCase() + "_gamma_proxy_by_strike_call_put.csv").toString(),
                        "--hourly", cfg.tickDir.resolve(pair.toLowerCase() + "_1h.csv").toString(),
                        "--start-date", cfg.reportDate.toString(),
                        "--end-date", cfg.reportDate.toString(),
                        "--topn", Integer.toString(cfg.topn),
                        "--output", pairOut.toString()
                );
                runCommand(cmd, cfg.projectRoot);

                Map<String, String> row = readFirstCsvRow(pairOut);
                if (row.isEmpty()) {
                    continue;
                }
                w.write(String.join(",",
                        csv(row.getOrDefault("date", cfg.reportDate.toString())),
                        csv(pair),
                        csv(row.getOrDefault("ref_price_prev_close", "")),
                        csv(row.getOrDefault("buy_limit", "")),
                        csv(row.getOrDefault("sell_limit", "")),
                        csv(row.getOrDefault("eod_close", "")),
                        csv(row.getOrDefault("notes", ""))));
                w.newLine();

                optionRepo.upsertLimitReportRow(cfg.reportDate, pair, row);
            }
        }
        return reportCsv;
    }

    private static Map<String, String> readFirstCsvRow(Path csvPath) throws IOException {
        try (BufferedReader r = Files.newBufferedReader(csvPath)) {
            String header = r.readLine();
            String first = r.readLine();
            if (header == null || first == null) {
                return Map.of();
            }
            List<String> h = Arrays.asList(header.split(",", -1));
            List<String> v = Arrays.asList(first.split(",", -1));
            Map<String, String> out = new LinkedHashMap<>();
            for (int i = 0; i < h.size(); i++) {
                out.put(h.get(i), i < v.size() ? v.get(i) : "");
            }
            return out;
        }
    }

    private static String csv(String v) {
        String text = v == null ? "" : v;
        if (text.contains(",") || text.contains("\"") || text.contains("\n")) {
            return "\"" + text.replace("\"", "\"\"") + "\"";
        }
        return text;
    }

    private static void runCommand(List<String> cmd, Path workDir) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(workDir.toFile());
        pb.redirectErrorStream(true);
        Process p = pb.start();

        try (BufferedReader r = p.inputReader()) {
            String line;
            while ((line = r.readLine()) != null) {
                System.out.println(line);
            }
        }

        int rc = p.waitFor();
        if (rc != 0) {
            throw new RuntimeException("Command failed rc=" + rc + " cmd=" + String.join(" ", cmd));
        }
    }

    private record Config(
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
            int topn
    ) {
        static Config parse(String[] args) {
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

            return new Config(
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
                    topn
            );
        }

        private static Map<String, String> parseFlags(String[] args) {
            Map<String, String> out = new HashMap<>();
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if (!a.startsWith("--")) {
                    continue;
                }
                String key = a.substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    out.put(key, args[i + 1]);
                    i++;
                } else {
                    out.put(key, "true");
                }
            }
            return out;
        }
    }
}
