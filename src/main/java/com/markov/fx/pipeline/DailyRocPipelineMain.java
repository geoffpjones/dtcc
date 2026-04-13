package com.markov.fx.pipeline;

import com.markov.fx.ingest.DukascopyBi5Client;
import com.markov.fx.store.SqliteBarRepository;
import com.markov.fx.util.CsvUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

public class DailyRocPipelineMain {
    private static final Logger LOG = Logger.getLogger(DailyRocPipelineMain.class.getName());

    public static void main(String[] args) throws Exception {
        PipelineConfig cfg = PipelineConfig.parse(args);
        List<String> pairs = cfg.pairs();
        Set<String> pairSet = Set.copyOf(pairs);
        SqliteBarRepository barRepository = new SqliteBarRepository(cfg.dbPath());
        DtccOptionTradeRepository optionRepo = new DtccOptionTradeRepository(cfg.dbPath());
        PairSignalSelection signalSelection = loadSignalSelection(cfg);

        LOG.info(() -> "Pipeline start reportDate=" + cfg.reportDate() + " db=" + cfg.dbPath());

        if (!cfg.reportOnly()) {
            ingestDtcc(cfg, pairSet, optionRepo);
            updateDukascopy(cfg, pairs, barRepository);
            exportHourlyBars(cfg, pairs);

            Path optionsForGamma = resolveOptionsForGamma(cfg, optionRepo, pairs);

            recalcGamma(cfg, optionsForGamma);
        } else {
            LOG.info("Report-only mode enabled: skipping DTCC ingest, Dukascopy ingest, hourly export and gamma recomputation");
        }
        Path reportCsv = produceLimitReports(cfg, pairs, optionRepo, signalSelection);
        LOG.info(() -> "Daily limit report written: " + reportCsv);
    }

    private static Path resolveOptionsForGamma(
            PipelineConfig cfg,
            DtccOptionTradeRepository optionRepo,
            List<String> pairs
    ) {
        if (cfg.optionsInputCsv() != null) {
            LOG.info(() -> "Using explicit options input CSV for gamma: " + cfg.optionsInputCsv());
            return cfg.optionsInputCsv();
        }
        Path optionsForGamma = cfg.dataDir().resolve("options_data_fx_pairs_for_gamma.csv");
        optionRepo.exportOptionsCsv(optionsForGamma, pairs);
        LOG.info(() -> "Exported options CSV for gamma: " + optionsForGamma);
        return optionsForGamma;
    }

    private static PairSignalSelection loadSignalSelection(PipelineConfig cfg) throws IOException {
        if (cfg.signalSelectionPath() == null) {
            LOG.info("No --signal-selection provided; default signal will be selected for all pairs");
            return PairSignalSelection.defaultOnly();
        }
        PairSignalSelection selection = PairSignalSelection.load(cfg.signalSelectionPath());
        LOG.info(() -> "Loaded signal selection config from " + cfg.signalSelectionPath());
        return selection;
    }

    private static void ingestDtcc(PipelineConfig cfg, Set<String> pairs, DtccOptionTradeRepository repo) throws Exception {
        DtccPublicClient client = new DtccPublicClient();
        // Incremental watermark: start from day after most recently ingested DTCC file date.
        LocalDate dtccStart = repo.latestSourceDate().map(d -> d.plusDays(1)).orElse(cfg.dtccBootstrapStart());
        LocalDate dtccEnd = cfg.reportDate();
        int files = client.ingestRange(cfg.dtccRegime(), cfg.dtccAsset(), dtccStart, dtccEnd, pairs, repo);
        LOG.info(() -> String.format("DTCC update complete start=%s end=%s files=%d", dtccStart, dtccEnd, files));
    }

    private static void updateDukascopy(PipelineConfig cfg, List<String> pairs, SqliteBarRepository barRepository) throws Exception {
        DukascopyDailyUpdater updater = new DukascopyDailyUpdater(new DukascopyBi5Client(), barRepository);
        for (String pair : pairs) {
            // Market ingest is incremental by latest stored bar date per symbol.
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
        new GammaRecalculator().recalculate(
                cfg.pairs(),
                optionsForGamma,
                cfg.tickDir(),
                cfg.dataDir(),
                cfg.volAssumption(),
                false
        );
    }

    private static Path produceLimitReports(
            PipelineConfig cfg,
            List<String> pairs,
            DtccOptionTradeRepository optionRepo,
            PairSignalSelection signalSelection
    ) throws Exception {
        Path reportDir = cfg.dataDir().resolve("reports");
        Files.createDirectories(reportDir);
        Path reportCsv = reportDir.resolve("fx_limit_order_report_" + cfg.reportDate() + ".csv");
        LimitSignalCalculator calculator = new LimitSignalCalculator();

        try (BufferedWriter w = Files.newBufferedWriter(reportCsv)) {
            w.write("requested_trade_date,effective_signal_date,pair,ref_price_prev_close,eod_close,"
                    + "default_buy_limit,default_sell_limit,default_notes,"
                    + "alt_buy_limit,alt_sell_limit,alt_notes,"
                    + "selected_signal,selected_buy_limit,selected_sell_limit,selected_notes");
            w.newLine();
            for (String pair : pairs) {
                Path defaultOut = reportDir.resolve(pair.toLowerCase() + "_walkforward_limit_default_" + cfg.reportDate() + ".csv");
                Path altOut = reportDir.resolve(pair.toLowerCase() + "_walkforward_limit_alt_" + cfg.reportDate() + ".csv");
                Path strikeGamma = cfg.dataDir().resolve(pair.toLowerCase() + "_gamma_proxy_by_strike_call_put.csv");
                Path hourly = cfg.tickDir().resolve(pair.toLowerCase() + "_1h.csv");
                Optional<LocalDate> effectiveDate = ReportDateResolver.resolveEffectiveSignalDate(cfg.reportDate(), hourly, strikeGamma);
                LocalDate signalDate = effectiveDate.orElse(cfg.reportDate());
                if (!signalDate.equals(cfg.reportDate())) {
                    LocalDate finalSignalDate = signalDate;
                    LOG.info(() -> "Resolved report date for " + pair + " from " + cfg.reportDate() + " to effective signal date " + finalSignalDate);
                }
                // The daily pipeline is intentionally pure Java at runtime: strike gamma and
                // walk-forward limit generation both execute natively without shelling out.
                calculator.writeLimitFile(
                        strikeGamma,
                        hourly,
                        signalDate,
                        signalDate,
                        new LimitSignalCalculator.SignalSpec(
                                LimitSignalCalculator.DEFAULT_SIGNAL.name(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.weightMode(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.levelMode(),
                                cfg.topn(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.maxDistancePips(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.distanceDecayPower(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.pipScale()
                        ),
                        defaultOut
                );
                calculator.writeLimitFile(
                        strikeGamma,
                        hourly,
                        signalDate,
                        signalDate,
                        LimitSignalCalculator.ALT_SIGNAL,
                        altOut
                );
                List<LimitSignalCalculator.LimitRow> defaultRows = calculator.buildRowsFromFiles(
                        strikeGamma,
                        hourly,
                        signalDate,
                        signalDate,
                        new LimitSignalCalculator.SignalSpec(
                                LimitSignalCalculator.DEFAULT_SIGNAL.name(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.weightMode(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.levelMode(),
                                cfg.topn(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.maxDistancePips(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.distanceDecayPower(),
                                LimitSignalCalculator.DEFAULT_SIGNAL.pipScale()
                        )
                );
                List<LimitSignalCalculator.LimitRow> altRows = calculator.buildRowsFromFiles(
                        strikeGamma,
                        hourly,
                        signalDate,
                        signalDate,
                        LimitSignalCalculator.ALT_SIGNAL
                );
                if (defaultRows.isEmpty() || altRows.isEmpty()) {
                    LOG.warning("No rows written by Java limit calculator for " + pair + " at " + defaultOut + " / " + altOut);
                    continue;
                }
                LimitSignalCalculator.LimitRow defaultRow = defaultRows.get(0);
                LimitSignalCalculator.LimitRow altRow = altRows.get(0);
                long stalenessDays = ChronoUnit.DAYS.between(signalDate, cfg.reportDate());
                boolean stale = stalenessDays > cfg.maxSignalStalenessDays();
                LimitSignalCalculator.SignalSpec selectedSpec = signalSelection.selectedFor(pair);
                LimitSignalCalculator.LimitRow selectedRow =
                        selectedSpec.name().equals(LimitSignalCalculator.ALT_SIGNAL.name()) ? altRow : defaultRow;
                String selectedSignalName = stale ? "" : selectedSpec.name();
                String selectedBuy = stale ? "" : fmt(selectedRow.buyLimit());
                String selectedSell = stale ? "" : fmt(selectedRow.sellLimit());
                String selectedNotes = stale
                        ? "stale_signal|effective_signal_date=" + signalDate + "|staleness_days=" + stalenessDays
                        : selectedRow.notes();
                // Report contract: one row per pair per report date.
                w.write(String.join(",",
                        CsvUtils.escape(cfg.reportDate().toString()),
                        CsvUtils.escape(signalDate.toString()),
                        CsvUtils.escape(pair),
                        CsvUtils.escape(fmt(defaultRow.refPricePrevClose())),
                        CsvUtils.escape(fmt(defaultRow.eodClose())),
                        CsvUtils.escape(fmt(defaultRow.buyLimit())),
                        CsvUtils.escape(fmt(defaultRow.sellLimit())),
                        CsvUtils.escape(defaultRow.notes()),
                        CsvUtils.escape(fmt(altRow.buyLimit())),
                        CsvUtils.escape(fmt(altRow.sellLimit())),
                        CsvUtils.escape(altRow.notes()),
                        CsvUtils.escape(selectedSignalName),
                        CsvUtils.escape(selectedBuy),
                        CsvUtils.escape(selectedSell),
                        CsvUtils.escape(selectedNotes)));
                w.newLine();

                optionRepo.upsertLimitReportRow(
                        cfg.reportDate(),
                        signalDate,
                        pair,
                        defaultRow,
                        altRow,
                        stale ? null : selectedSpec,
                        stale ? null : selectedRow,
                        selectedNotes
                );
            }
        }
        return reportCsv;
    }

    private static String fmt(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "";
        }
        return Double.toString(value);
    }
}
