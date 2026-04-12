package com.markov.fx.pipeline;

import com.markov.fx.ingest.DukascopyBi5Client;
import com.markov.fx.store.SqliteBarRepository;
import com.markov.fx.util.CsvUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DailyRocPipelineMain {
    private static final Logger LOG = Logger.getLogger(DailyRocPipelineMain.class.getName());

    public static void main(String[] args) throws Exception {
        PipelineConfig cfg = PipelineConfig.parse(args);
        List<String> pairs = List.of("EURUSD", "GBPUSD");
        Set<String> pairSet = Set.copyOf(pairs);
        SqliteBarRepository barRepository = new SqliteBarRepository(cfg.dbPath());
        DtccOptionTradeRepository optionRepo = new DtccOptionTradeRepository(cfg.dbPath());

        LOG.info(() -> "Pipeline start reportDate=" + cfg.reportDate() + " db=" + cfg.dbPath());

        ingestDtcc(cfg, pairSet, optionRepo);
        updateDukascopy(cfg, pairs, barRepository);
        exportHourlyBars(cfg, pairs);

        Path optionsForGamma = cfg.dataDir().resolve("options_data_fx_pairs_for_gamma.csv");
        optionRepo.exportOptionsCsv(optionsForGamma, pairs);
        LOG.info(() -> "Exported options CSV for gamma: " + optionsForGamma);

        recalcGamma(cfg, optionsForGamma);
        Path reportCsv = produceLimitReports(cfg, pairs, optionRepo);
        LOG.info(() -> "Daily limit report written: " + reportCsv);
    }

    private static void ingestDtcc(PipelineConfig cfg, Set<String> pairs, DtccOptionTradeRepository repo) throws Exception {
        DtccPublicClient client = new DtccPublicClient();
        LocalDate dtccStart = repo.latestSourceDate().map(d -> d.plusDays(1)).orElse(cfg.dtccBootstrapStart());
        LocalDate dtccEnd = cfg.reportDate();
        int files = client.ingestRange(cfg.dtccRegime(), cfg.dtccAsset(), dtccStart, dtccEnd, pairs, repo);
        LOG.info(() -> String.format("DTCC update complete start=%s end=%s files=%d", dtccStart, dtccEnd, files));
    }

    private static void updateDukascopy(PipelineConfig cfg, List<String> pairs, SqliteBarRepository barRepository) throws Exception {
        DukascopyDailyUpdater updater = new DukascopyDailyUpdater(new DukascopyBi5Client(), barRepository);
        for (String pair : pairs) {
            LocalDate start = barRepository.latestDate(pair).map(d -> d.plusDays(1)).orElse(cfg.marketBootstrapStart());
            long loaded = updater.updateSymbol(pair, start, cfg.reportDate());
            Optional<Instant> latest = barRepository.latestTimestamp(pair);
            LOG.info(() -> String.format("Dukascopy update %s loadedBars=%d latestTs=%s", pair, loaded, latest.orElse(null)));
        }
    }

    private static void exportHourlyBars(PipelineConfig cfg, List<String> pairs) {
        HourlyBarCsvExporter exporter = new HourlyBarCsvExporter(cfg.dbPath());
        for (String pair : pairs) {
            Path out = cfg.tickDir().resolve(pair.toLowerCase() + "_1h.csv");
            int rows = exporter.exportHourly(pair, out);
            LOG.info(() -> String.format("Exported hourly bars %s rows=%d path=%s", pair, rows, out));
        }
    }

    private static void recalcGamma(PipelineConfig cfg, Path optionsForGamma) throws Exception {
        List<String> cmd = List.of(
                cfg.pythonExec(),
                "scripts/build_gamma_for_pairs.py",
                "--pairs", "EURUSD,GBPUSD",
                "--options-input", optionsForGamma.toString(),
                "--tick-dir", cfg.tickDir().toString(),
                "--out-dir", cfg.dataDir().toString(),
                "--vol-assumption", Double.toString(cfg.volAssumption())
        );
        runCommand(cmd, cfg.projectRoot(), cfg.commandTimeoutSeconds());
    }

    private static Path produceLimitReports(PipelineConfig cfg, List<String> pairs, DtccOptionTradeRepository optionRepo) throws Exception {
        Path reportDir = cfg.dataDir().resolve("reports");
        Files.createDirectories(reportDir);
        Path reportCsv = reportDir.resolve("fx_limit_order_report_" + cfg.reportDate() + ".csv");

        try (BufferedWriter w = Files.newBufferedWriter(reportCsv)) {
            w.write("date,pair,ref_price_prev_close,buy_limit,sell_limit,eod_close,notes");
            w.newLine();
            for (String pair : pairs) {
                Path pairOut = reportDir.resolve(pair.toLowerCase() + "_walkforward_limit_" + cfg.reportDate() + ".csv");
                List<String> cmd = List.of(
                        cfg.pythonExec(),
                        "scripts/backtest_walkforward_gamma_limits.py",
                        "--strike-gamma", cfg.dataDir().resolve(pair.toLowerCase() + "_gamma_proxy_by_strike_call_put.csv").toString(),
                        "--hourly", cfg.tickDir().resolve(pair.toLowerCase() + "_1h.csv").toString(),
                        "--start-date", cfg.reportDate().toString(),
                        "--end-date", cfg.reportDate().toString(),
                        "--topn", Integer.toString(cfg.topn()),
                        "--output", pairOut.toString()
                );
                runCommand(cmd, cfg.projectRoot(), cfg.commandTimeoutSeconds());

                Map<String, String> row = readFirstCsvRow(pairOut);
                if (row.isEmpty()) {
                    LOG.warning("No rows written by limit script for " + pair + " at " + pairOut);
                    continue;
                }
                w.write(String.join(",",
                        CsvUtils.escape(row.getOrDefault("date", cfg.reportDate().toString())),
                        CsvUtils.escape(pair),
                        CsvUtils.escape(row.getOrDefault("ref_price_prev_close", "")),
                        CsvUtils.escape(row.getOrDefault("buy_limit", "")),
                        CsvUtils.escape(row.getOrDefault("sell_limit", "")),
                        CsvUtils.escape(row.getOrDefault("eod_close", "")),
                        CsvUtils.escape(row.getOrDefault("notes", ""))));
                w.newLine();

                optionRepo.upsertLimitReportRow(cfg.reportDate(), pair, row);
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
            List<String> h = CsvUtils.parseLine(header);
            List<String> v = CsvUtils.parseLine(first);
            Map<String, String> out = new LinkedHashMap<>();
            for (int i = 0; i < h.size(); i++) {
                out.put(h.get(i), i < v.size() ? v.get(i) : "");
            }
            return out;
        }
    }

    private static void runCommand(List<String> cmd, Path workDir, long timeoutSec) throws Exception {
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

        boolean done = p.waitFor(timeoutSec, TimeUnit.SECONDS);
        if (!done) {
            p.destroyForcibly();
            throw new RuntimeException("Command timed out after " + timeoutSec + "s: " + String.join(" ", cmd));
        }
        int rc = p.exitValue();
        if (rc != 0) {
            throw new RuntimeException("Command failed rc=" + rc + " cmd=" + String.join(" ", cmd));
        }
    }
}
