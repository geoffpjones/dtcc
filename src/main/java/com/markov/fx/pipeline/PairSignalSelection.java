package com.markov.fx.pipeline;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class PairSignalSelection {
    private final Map<String, LimitSignalCalculator.SignalSpec> selections;

    private PairSignalSelection(Map<String, LimitSignalCalculator.SignalSpec> selections) {
        this.selections = Map.copyOf(selections);
    }

    public static PairSignalSelection defaultOnly() {
        return new PairSignalSelection(Map.of());
    }

    public static PairSignalSelection load(Path csvPath) throws IOException {
        Map<String, LimitSignalCalculator.SignalSpec> out = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(csvPath);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord row : parser) {
                String pair = row.get("pair").trim().toUpperCase(Locale.ROOT);
                String signal = row.get("selected_signal").trim();
                if (pair.isEmpty()) {
                    continue;
                }
                out.put(pair, namedSignal(signal));
            }
        }
        return new PairSignalSelection(out);
    }

    public LimitSignalCalculator.SignalSpec selectedFor(String pair) {
        return selections.getOrDefault(pair.toUpperCase(Locale.ROOT), LimitSignalCalculator.DEFAULT_SIGNAL);
    }

    private static LimitSignalCalculator.SignalSpec namedSignal(String name) {
        if (LimitSignalCalculator.ALT_SIGNAL.name().equals(name)) {
            return LimitSignalCalculator.ALT_SIGNAL;
        }
        if (LimitSignalCalculator.DEFAULT_SIGNAL.name().equals(name)) {
            return LimitSignalCalculator.DEFAULT_SIGNAL;
        }
        throw new IllegalArgumentException("Unknown signal name in selection file: " + name);
    }
}
