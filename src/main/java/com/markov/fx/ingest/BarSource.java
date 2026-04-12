package com.markov.fx.ingest;

import com.markov.fx.model.Bar;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public interface BarSource {
    List<Bar> fetch(String symbol, Instant fromInclusive, Instant toExclusive) throws IOException, InterruptedException;
}
