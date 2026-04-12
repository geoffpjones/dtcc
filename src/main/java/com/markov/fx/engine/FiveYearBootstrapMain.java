package com.markov.fx.engine;

import com.markov.fx.model.Bar;
import com.markov.fx.model.MarkovConfig;
import com.markov.fx.model.OnlineMarkovRegimeModel;
import com.markov.fx.store.SqliteBarRepository;

import java.time.Instant;
import java.util.List;

public class FiveYearBootstrapMain {
    public static void main(String[] args) throws Exception {
        String symbol = args.length >= 1 ? args[0] : "EURUSD";
        String dbPath = args.length >= 2 ? args[1] : "data/fx-bars-5y.db";
        Instant from = args.length >= 3 ? Instant.parse(args[2]) : Instant.parse("2021-03-22T00:00:00Z");
        Instant to = args.length >= 4 ? Instant.parse(args[3]) : Instant.parse("2026-03-22T00:00:00Z");
        int iterations = args.length >= 5 ? Integer.parseInt(args[4]) : 30;

        SqliteBarRepository repo = new SqliteBarRepository(dbPath);
        List<Bar> bars = repo.load(symbol, from, to);

        System.out.printf("Loaded %d bars for %s in [%s, %s)%n", bars.size(), symbol, from, to);

        List<ModelSpec> specs = List.of(
                new ModelSpec("order2_thr3bp", new MarkovConfig(2, 0.00003, 1.0)),
                new ModelSpec("order3_thr5bp", new MarkovConfig(3, 0.00005, 1.0)),
                new ModelSpec("order4_thr7bp", new MarkovConfig(4, 0.00007, 1.0))
        );

        BacktestConfig backtestConfig = new BacktestConfig(30);

        BacktestEngine backtestEngine = new BacktestEngine();
        OnlineMarkovRegimeModel baseModel = new OnlineMarkovRegimeModel(new MarkovConfig(3, 0.00005, 1.0));
        BacktestResult baseResult = backtestEngine.run(bars, baseModel, backtestConfig);
        System.out.printf(
                "Base model order3_thr5bp: evaluated=%d accuracy=%.6f learnedStates=%d%n",
                baseResult.evaluatedPredictions(),
                baseResult.accuracy(),
                baseModel.learnedStates()
        );

        BootstrapEngine bootstrapEngine = new BootstrapEngine();
        List<BootstrapSummary> summaries = bootstrapEngine.run(bars, specs, backtestConfig, iterations, 42L);

        for (BootstrapSummary s : summaries) {
            System.out.printf(
                    "Bootstrap %s: iter=%d mean=%.6f p10=%.6f p50=%.6f p90=%.6f%n",
                    s.spec().name(),
                    s.iterations(),
                    s.meanAccuracy(),
                    s.p10Accuracy(),
                    s.p50Accuracy(),
                    s.p90Accuracy()
            );
        }
    }
}
