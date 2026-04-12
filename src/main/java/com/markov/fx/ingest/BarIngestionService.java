package com.markov.fx.ingest;

import com.markov.fx.model.Bar;
import com.markov.fx.store.BarRepository;

import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public class BarIngestionService {
    private final BarSource source;
    private final BarRepository repository;

    public BarIngestionService(BarSource source, BarRepository repository) {
        this.source = source;
        this.repository = repository;
    }

    public int ingest(String symbol, Instant fromInclusive, Instant toExclusive) throws IOException, InterruptedException {
        List<Bar> bars = source.fetch(symbol, fromInclusive, toExclusive);
        bars.sort(Comparator.comparing(Bar::timestamp));
        repository.save(symbol, bars);
        return bars.size();
    }
}
