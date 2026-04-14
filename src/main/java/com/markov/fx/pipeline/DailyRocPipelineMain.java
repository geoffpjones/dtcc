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
        ExitParamSelection exitParamSelection = loadExitParams(cfg);

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
        Path reportCsv = produceLimitReports(cfg, pairs, optionRepo, signalSelection, exitParamSelection);
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

    private static ExitParamSelection loadExitParams(PipelineConfig cfg) throws IOException {
        if (cfg.exitParamPath() == null) {
            throw new IllegalArgumentException("No exit-params config supplied and config/exit_params.csv not found");
        }
        ExitParamSelection selection = ExitParamSelection.load(cfg.exitParamPath());
        LOG.info(() -> "Loaded exit param config from " + cfg.exitParamPath());
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
            PairSignalSelection signalSelection,
            ExitParamSelection exitParamSelection
    ) throws Exception {
        Path reportDir = cfg.dataDir().resolve("reports");
        Files.createDirectories(reportDir);
        Path reportCsv = reportDir.resolve("fx_limit_order_report_" + cfg.reportDate() + ".csv");
        LimitSignalCalculator calculator = new LimitSignalCalculator();

        try (BufferedWriter w = Files.newBufferedWriter(reportCsv)) {
            w.write("requested_trade_date,effective_signal_date,pair,ref_price_prev_close,eod_close,"
                    + "selected_signal,selected_buy_limit,selected_sell_limit,selected_notes,"
                    + "selected_exit_mode,selected_tp_pips,selected_sl_pips,selected_trail_pips,"
                    + "selected_buy_tp_level,selected_buy_sl_level,selected_sell_tp_level,selected_sell_sl_level");
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
                ExitParamSelection.ExitParams exitParams = exitParamSelection.selectedFor(pair);
                LimitSignalCalculator.LimitRow selectedRow =
                        selectedSpec.name().equals(LimitSignalCalculator.ALT_SIGNAL.name()) ? altRow : defaultRow;
                String selectedSignalName = stale ? "" : selectedSpec.name();
                SelectedLevels selectedLevels = stale
                        ? new SelectedLevels(null, null, null)
                        : chooseSelectedLevels(defaultRow, altRow, selectedSpec);
                String selectedBuy = stale ? "" : fmtNullable(selectedLevels.buyLevel());
                String selectedSell = stale ? "" : fmtNullable(selectedLevels.sellLevel());
                String selectedNotes = stale
                        ? "stale_signal|effective_signal_date=" + signalDate + "|staleness_days=" + stalenessDays
                        : mergeSelectedNotes(selectedRow.notes(), selectedLevels.notesSuffix());
                double pipSize = pipSize(pair);
                TradeLevels tradeLevels = stale ? new TradeLevels(null, null, null, null)
                        : tradeLevels(selectedLevels, exitParams, pipSize);
                // Report contract: one row per pair per report date.
                w.write(String.join(",",
                        CsvUtils.escape(cfg.reportDate().toString()),
                        CsvUtils.escape(signalDate.toString()),
                        CsvUtils.escape(pair),
                        CsvUtils.escape(fmt(defaultRow.refPricePrevClose())),
                        CsvUtils.escape(fmt(defaultRow.eodClose())),
                        CsvUtils.escape(selectedSignalName),
                        CsvUtils.escape(selectedBuy),
                        CsvUtils.escape(selectedSell),
                        CsvUtils.escape(selectedNotes),
                        CsvUtils.escape(stale ? "" : exitParams.mode()),
                        CsvUtils.escape(stale ? "" : Double.toString(exitParams.tpPips())),
                        CsvUtils.escape(stale ? "" : Double.toString(exitParams.slPips())),
                        CsvUtils.escape(stale ? "" : Double.toString(exitParams.trailPips())),
                        CsvUtils.escape(fmtNullable(tradeLevels.buyTp())),
                        CsvUtils.escape(fmtNullable(tradeLevels.buySl())),
                        CsvUtils.escape(fmtNullable(tradeLevels.sellTp())),
                        CsvUtils.escape(fmtNullable(tradeLevels.sellSl()))));
                w.newLine();

                optionRepo.upsertLimitReportRow(
                        cfg.reportDate(),
                        signalDate,
                        pair,
                        defaultRow,
                        altRow,
                        stale ? null : selectedSpec,
                        stale ? null : new LimitSignalCalculator.LimitRow(
                                selectedRow.date(),
                                selectedRow.refPricePrevClose(),
                                selectedLevels.buyLevel() == null ? Double.NaN : selectedLevels.buyLevel(),
                                selectedLevels.sellLevel() == null ? Double.NaN : selectedLevels.sellLevel(),
                                selectedRow.eodClose(),
                                selectedRow.buyFilled(),
                                selectedRow.sellFilled(),
                                selectedRow.buyFillTimeUtc(),
                                selectedRow.sellFillTimeUtc(),
                                selectedRow.buyPnlPips(),
                                selectedRow.sellPnlPips(),
                                selectedRow.netPnlPips(),
                                selectedRow.cumNetPnlPips(),
                                selectedNotes
                        ),
                        selectedNotes,
                        stale ? null : exitParams,
                        tradeLevels
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

    private static String fmtNullable(Double value) {
        if (value == null) {
            return "";
        }
        return fmt(value);
    }

    static SelectedLevels chooseSelectedLevels(
            LimitSignalCalculator.LimitRow defaultRow,
            LimitSignalCalculator.LimitRow altRow,
            LimitSignalCalculator.SignalSpec selectedSpec
    ) {
        LimitSignalCalculator.LimitRow preferred =
                selectedSpec.name().equals(LimitSignalCalculator.ALT_SIGNAL.name()) ? altRow : defaultRow;
        boolean preferredBuyValid = validBuy(preferred.buyLimit(), preferred.refPricePrevClose());
        boolean preferredSellValid = validSell(preferred.sellLimit(), preferred.refPricePrevClose());
        boolean defaultBuyValid = validBuy(defaultRow.buyLimit(), defaultRow.refPricePrevClose());
        boolean defaultSellValid = validSell(defaultRow.sellLimit(), defaultRow.refPricePrevClose());

        double preferredMarketBuyAnchor = marketBuyAnchor(preferred);
        double preferredMarketSellAnchor = marketSellAnchor(preferred);
        double defaultMarketBuyAnchor = marketBuyAnchor(defaultRow);
        double defaultMarketSellAnchor = marketSellAnchor(defaultRow);

        preferredBuyValid = preferredBuyValid && validBuy(preferred.buyLimit(), preferredMarketBuyAnchor);
        preferredSellValid = preferredSellValid && validSell(preferred.sellLimit(), preferredMarketSellAnchor);
        defaultBuyValid = defaultBuyValid && validBuy(defaultRow.buyLimit(), defaultMarketBuyAnchor);
        defaultSellValid = defaultSellValid && validSell(defaultRow.sellLimit(), defaultMarketSellAnchor);

        Double buy;
        if (preferredBuyValid) {
            buy = preferred.buyLimit();
        } else if (defaultBuyValid) {
            buy = defaultRow.buyLimit();
        } else {
            buy = null;
        }

        Double sell;
        if (preferredSellValid) {
            sell = preferred.sellLimit();
        } else if (defaultSellValid) {
            sell = defaultRow.sellLimit();
        } else {
            sell = null;
        }

        List<String> tags = new java.util.ArrayList<>();
        if (!preferredBuyValid && defaultBuyValid) {
            tags.add("fallback_default_buy");
        }
        if (!preferredSellValid && defaultSellValid) {
            tags.add("fallback_default_sell");
        }
        if (!preferredBuyValid && buy == null && !Double.isNaN(preferred.buyLimit())) {
            tags.add("invalid_buy_suppressed");
        }
        if (!preferredSellValid && sell == null && !Double.isNaN(preferred.sellLimit())) {
            tags.add("invalid_sell_suppressed");
        }
        return new SelectedLevels(buy, sell, tags.isEmpty() ? null : String.join("|", tags));
    }

    private static boolean validBuy(double buy, double ref) {
        return !Double.isNaN(buy) && !Double.isNaN(ref) && buy <= ref;
    }

    private static boolean validSell(double sell, double ref) {
        return !Double.isNaN(sell) && !Double.isNaN(ref) && sell >= ref;
    }

    private static double marketBuyAnchor(LimitSignalCalculator.LimitRow row) {
        return Double.isNaN(row.eodClose()) ? row.refPricePrevClose() : Math.min(row.refPricePrevClose(), row.eodClose());
    }

    private static double marketSellAnchor(LimitSignalCalculator.LimitRow row) {
        return Double.isNaN(row.eodClose()) ? row.refPricePrevClose() : Math.max(row.refPricePrevClose(), row.eodClose());
    }

    private static String mergeSelectedNotes(String base, String suffix) {
        if (suffix == null || suffix.isBlank()) {
            return base;
        }
        if (base == null || base.isBlank()) {
            return suffix;
        }
        return base + "|" + suffix;
    }

    record SelectedLevels(Double buyLevel, Double sellLevel, String notesSuffix) {
    }

    static TradeLevels tradeLevels(SelectedLevels selectedLevels, ExitParamSelection.ExitParams exitParams, double pipSize) {
        Double buyTp = selectedLevels.buyLevel() == null ? null : selectedLevels.buyLevel() + exitParams.tpPips() * pipSize;
        Double buySl = selectedLevels.buyLevel() == null ? null : selectedLevels.buyLevel() - exitParams.slPips() * pipSize;
        Double sellTp = selectedLevels.sellLevel() == null ? null : selectedLevels.sellLevel() - exitParams.tpPips() * pipSize;
        Double sellSl = selectedLevels.sellLevel() == null ? null : selectedLevels.sellLevel() + exitParams.slPips() * pipSize;
        return new TradeLevels(buyTp, buySl, sellTp, sellSl);
    }

    private static double pipSize(String pair) {
        return pair.endsWith("JPY") ? 0.01 : 0.0001;
    }

    record TradeLevels(Double buyTp, Double buySl, Double sellTp, Double sellSl) {
    }
}
