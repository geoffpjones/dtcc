package com.markov.fx.engine;

import com.markov.fx.model.Bar;
import com.markov.fx.model.MarkovConfig;
import com.markov.fx.model.OnlineMarkovRegimeModel;
import com.markov.fx.model.Prediction;
import com.markov.fx.model.Regime;
import com.markov.fx.store.SqliteBarRepository;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;

public class WeekendFilteredBootstrapMain {
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final ZoneId SYDNEY = ZoneId.of("Australia/Sydney");

    public static void main(String[] args) throws Exception {
        String symbol = args.length >= 1 ? args[0] : "EURUSD";
        String dbPath = args.length >= 2 ? args[1] : "data/fx-bars-5y.db";
        Instant from = args.length >= 3 ? Instant.parse(args[2]) : Instant.parse("2021-03-22T00:00:00Z");
        Instant to = args.length >= 4 ? Instant.parse(args[3]) : Instant.parse("2026-03-22T00:00:00Z");
        int iterations = args.length >= 5 ? Integer.parseInt(args[4]) : 30;
        int gapResetMinutes = args.length >= 6 ? Integer.parseInt(args[5]) : 5;

        SqliteBarRepository repo = new SqliteBarRepository(dbPath);
        List<Bar> bars = repo.load(symbol, from, to);
        List<Bar> filtered = filterClosedBars(bars);

        System.out.printf("Loaded=%d FilteredOpen=%d RemovedClosed=%d%n", bars.size(), filtered.size(), bars.size() - filtered.size());

        List<ModelSpec> specs = List.of(
                new ModelSpec("order2_thr3bp", new MarkovConfig(2, 0.00003, 1.0)),
                new ModelSpec("order3_thr5bp", new MarkovConfig(3, 0.00005, 1.0)),
                new ModelSpec("order4_thr7bp", new MarkovConfig(4, 0.00007, 1.0))
        );

        BacktestConfig backtestConfig = new BacktestConfig(30);

        for (ModelSpec spec : specs) {
            GapAwareResult base = runGapAwareBacktest(filtered, spec.markovConfig(), backtestConfig, gapResetMinutes);
            System.out.printf(
                    "Base %s: evaluated=%d accuracy=%.6f%n",
                    spec.name(),
                    base.evaluatedPredictions,
                    base.accuracy()
            );

            List<Double> bootAcc = new ArrayList<>();
            SplittableRandom random = new SplittableRandom(42L + spec.name().hashCode());
            for (int i = 0; i < iterations; i++) {
                List<Bar> sample = bootstrapBars(filtered, random.split());
                GapAwareResult r = runGapAwareBacktest(sample, spec.markovConfig(), backtestConfig, gapResetMinutes);
                bootAcc.add(r.accuracy());
            }
            bootAcc.sort(Comparator.naturalOrder());
            double mean = bootAcc.stream().mapToDouble(x -> x).average().orElse(0.0);
            double p10 = percentile(bootAcc, 0.10);
            double p50 = percentile(bootAcc, 0.50);
            double p90 = percentile(bootAcc, 0.90);
            System.out.printf(
                    "Bootstrap %s: iter=%d mean=%.6f p10=%.6f p50=%.6f p90=%.6f%n",
                    spec.name(), iterations, mean, p10, p50, p90
            );
        }
    }

    private static List<Bar> filterClosedBars(List<Bar> bars) {
        List<Bar> out = new ArrayList<>(bars.size());
        for (Bar bar : bars) {
            ZonedDateTime ny = bar.timestamp().atZone(NEW_YORK);
            ZonedDateTime syd = bar.timestamp().atZone(SYDNEY);

            boolean closed =
                    (ny.getDayOfWeek() == DayOfWeek.FRIDAY && ny.getHour() >= 17)
                            || syd.getDayOfWeek() == DayOfWeek.SATURDAY
                            || syd.getDayOfWeek() == DayOfWeek.SUNDAY
                            || (syd.getDayOfWeek() == DayOfWeek.MONDAY && syd.getHour() < 7);

            if (!closed) {
                out.add(bar);
            }
        }
        return out;
    }

    private static GapAwareResult runGapAwareBacktest(List<Bar> bars, MarkovConfig cfg, BacktestConfig backtestConfig, int gapResetMinutes) {
        OnlineMarkovRegimeModel model = new OnlineMarkovRegimeModel(cfg);
        int evaluated = 0;
        int correct = 0;
        int observed = 0;

        Optional<Prediction> pending = Optional.empty();
        Instant prevTs = null;

        for (Bar bar : bars) {
            if (prevTs != null) {
                long gap = Duration.between(prevTs, bar.timestamp()).toMinutes();
                if (gap > gapResetMinutes) {
                    model.resetObservationContext();
                    pending = Optional.empty();
                }
            }

            Optional<Regime> realized = model.observeBar(bar);
            observed++;

            if (realized.isPresent() && pending.isPresent() && observed > backtestConfig.warmupBars()) {
                evaluated++;
                if (pending.get().predictedRegime() == realized.get()) {
                    correct++;
                }
            }

            pending = model.predictNext(bar.timestamp());
            prevTs = bar.timestamp();
        }

        return new GapAwareResult(evaluated, correct);
    }

    private static List<Bar> bootstrapBars(List<Bar> bars, SplittableRandom random) {
        if (bars.size() < 2) {
            return bars;
        }

        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < bars.size(); i++) {
            double prev = bars.get(i - 1).close();
            double curr = bars.get(i).close();
            returns.add((curr - prev) / prev);
        }

        List<Bar> sample = new ArrayList<>();
        Bar first = bars.getFirst();
        sample.add(first);
        double close = first.close();

        for (int i = 1; i < bars.size(); i++) {
            double r = returns.get(random.nextInt(returns.size()));
            double next = close * (1.0 + r);
            Instant ts = bars.get(i).timestamp();
            double hi = Math.max(close, next);
            double lo = Math.min(close, next);
            sample.add(new Bar(first.symbol(), ts, close, hi, lo, next, bars.get(i).volume()));
            close = next;
        }

        return sample;
    }

    private static double percentile(List<Double> sorted, double q) {
        int idx = (int) Math.floor((sorted.size() - 1) * q);
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    private record GapAwareResult(int evaluatedPredictions, int correctPredictions) {
        double accuracy() {
            if (evaluatedPredictions == 0) {
                return 0.0;
            }
            return (double) correctPredictions / evaluatedPredictions;
        }
    }
}
