package com.markov.fx;

import com.markov.fx.engine.BacktestConfig;
import com.markov.fx.engine.BacktestEngine;
import com.markov.fx.engine.BacktestResult;
import com.markov.fx.engine.BootstrapEngine;
import com.markov.fx.engine.BootstrapSummary;
import com.markov.fx.engine.ModelSpec;
import com.markov.fx.ingest.BarIngestionService;
import com.markov.fx.ingest.BarSource;
import com.markov.fx.ingest.CsvBarSource;
import com.markov.fx.model.Bar;
import com.markov.fx.model.MarkovConfig;
import com.markov.fx.model.OnlineMarkovRegimeModel;
import com.markov.fx.store.BarRepository;
import com.markov.fx.store.SqliteBarRepository;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        String symbol = "EURUSD";
        Instant from = Instant.parse("2024-01-01T00:00:00Z");
        Instant to = Instant.parse("2024-01-02T00:00:00Z");

        BarSource source = new CsvBarSource(Path.of("data/eurusd_1m_sample.csv"));
        BarRepository repo = new SqliteBarRepository("data/fx-bars.db");

        BarIngestionService ingestionService = new BarIngestionService(source, repo);
        int ingested = ingestionService.ingest(symbol, from, to);

        List<Bar> bars = loadBars(repo, symbol, from, to);
        OnlineMarkovRegimeModel model = new OnlineMarkovRegimeModel(new MarkovConfig(3, 0.00005, 1.0));
        BacktestEngine backtestEngine = new BacktestEngine();
        BacktestResult result = backtestEngine.run(bars, model, new BacktestConfig(30));
        List<ModelSpec> specs = List.of(
                new ModelSpec("order2_thr3bp", new MarkovConfig(2, 0.00003, 1.0)),
                new ModelSpec("order3_thr5bp", new MarkovConfig(3, 0.00005, 1.0)),
                new ModelSpec("order4_thr7bp", new MarkovConfig(4, 0.00007, 1.0))
        );
        BootstrapEngine bootstrapEngine = new BootstrapEngine();
        List<BootstrapSummary> bootstrap = bootstrapEngine.run(
                bars,
                specs,
                new BacktestConfig(30),
                30,
                42L
        );

        System.out.println("Bars ingested: " + ingested);
        System.out.println("Bars loaded: " + bars.size());
        System.out.println("Learned states: " + model.learnedStates());
        System.out.printf("Accuracy: %.4f%n", result.accuracy());
        System.out.println("Evaluated predictions: " + result.evaluatedPredictions());
        System.out.println("Confusion matrix (predicted -> actual): " + result.confusionMatrix());
        for (BootstrapSummary s : bootstrap) {
            System.out.printf(
                    "Bootstrap %s: iter=%d mean=%.4f p10=%.4f p50=%.4f p90=%.4f%n",
                    s.spec().name(), s.iterations(), s.meanAccuracy(), s.p10Accuracy(), s.p50Accuracy(), s.p90Accuracy()
            );
        }
    }

    private static List<Bar> loadBars(BarRepository repo, String symbol, Instant from, Instant to) throws SQLException {
        return repo.load(symbol, from, to);
    }
}
