package com.markov.fx.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class StateKey {
    private final List<Regime> regimes;

    public StateKey(List<Regime> regimes) {
        this.regimes = Collections.unmodifiableList(new ArrayList<>(regimes));
    }

    public List<Regime> regimes() {
        return regimes;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StateKey other)) {
            return false;
        }
        return regimes.equals(other.regimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regimes);
    }

    @Override
    public String toString() {
        return regimes.toString();
    }
}
