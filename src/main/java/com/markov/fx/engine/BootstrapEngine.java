package com.markov.fx.engine;

import com.markov.fx.model.Bar;
import com.markov.fx.model.OnlineMarkovRegimeModel;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;

public class BootstrapEngine {
    public List<BootstrapSummary> run(
            List<Bar> bars,
            List<ModelSpec> specs,
            BacktestConfig backtestConfig,
            int iterations,
            long seed
    ) {
        List<BootstrapSummary> out = new ArrayList<>();
        for (ModelSpec spec : specs) {
            List<Double> accuracies = new ArrayList<>();
            SplittableRandom random = new SplittableRandom(seed + spec.name().hashCode());
            for (int i = 0; i < iterations; i++) {
                List<Bar> sample = bootstrapBars(bars, random.split());
                BacktestEngine backtest = new BacktestEngine();
                OnlineMarkovRegimeModel model = new OnlineMarkovRegimeModel(spec.markovConfig());
                BacktestResult r = backtest.run(sample, model, backtestConfig);
                accuracies.add(r.accuracy());
            }
            out.add(summarize(spec, accuracies));
        }
        return out;
    }

    private List<Bar> bootstrapBars(List<Bar> bars, SplittableRandom random) {
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

    private BootstrapSummary summarize(ModelSpec spec, List<Double> accuracies) {
        if (accuracies.isEmpty()) {
            return new BootstrapSummary(spec, 0, 0, 0, 0, 0, accuracies);
        }
        List<Double> sorted = new ArrayList<>(accuracies);
        sorted.sort(Comparator.naturalOrder());

        double mean = accuracies.stream().mapToDouble(x -> x).average().orElse(0.0);
        double p10 = percentile(sorted, 0.10);
        double p50 = percentile(sorted, 0.50);
        double p90 = percentile(sorted, 0.90);
        return new BootstrapSummary(spec, accuracies.size(), mean, p10, p50, p90, accuracies);
    }

    private double percentile(List<Double> sorted, double q) {
        int idx = (int) Math.floor((sorted.size() - 1) * q);
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, idx)));
    }
}
