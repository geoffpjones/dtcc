package com.markov.fx.store;

import com.markov.fx.model.Bar;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public interface BarRepository {
    void save(String symbol, List<Bar> bars);

    List<Bar> load(String symbol, Instant fromInclusive, Instant toExclusive) throws SQLException;
}
