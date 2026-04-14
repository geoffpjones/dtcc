package com.markov.fx.pipeline;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ExitParamSelection {
    private final Map<String, ExitParams> selections;

    private ExitParamSelection(Map<String, ExitParams> selections) {
        this.selections = selections;
    }

    public static ExitParamSelection load(Path csvPath) throws IOException {
        Map<String, ExitParams> out = new HashMap<>();
        try (Reader reader = Files.newBufferedReader(csvPath);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord row : parser) {
                String pair = row.get("pair").trim().toUpperCase(Locale.ROOT);
                if (pair.isEmpty()) {
                    continue;
                }
                out.put(pair, new ExitParams(
                        row.get("mode").trim(),
                        Double.parseDouble(row.get("tp_pips").trim()),
                        Double.parseDouble(row.get("sl_pips").trim()),
                        row.get("trail_pips").trim().isEmpty() ? 0.0 : Double.parseDouble(row.get("trail_pips").trim())
                ));
            }
        }
        return new ExitParamSelection(out);
    }

    public ExitParams selectedFor(String pair) {
        ExitParams params = selections.get(pair.toUpperCase(Locale.ROOT));
        if (params == null) {
            throw new IllegalArgumentException("No exit params configured for pair " + pair);
        }
        return params;
    }

    public record ExitParams(String mode, double tpPips, double slPips, double trailPips) {
    }
}
