package com.markov.fx.ingest;

import com.markov.fx.model.Bar;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CsvBarSource implements BarSource {
    private final Path csvPath;

    public CsvBarSource(Path csvPath) {
        this.csvPath = csvPath;
    }

    @Override
    public List<Bar> fetch(String symbol, Instant fromInclusive, Instant toExclusive) throws IOException {
        List<Bar> bars = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String line;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (first && line.toLowerCase().contains("timestamp")) {
                    first = false;
                    continue;
                }
                first = false;

                String[] p = line.split(",");
                if (p.length < 6) {
                    continue;
                }
                Instant ts = Instant.parse(p[0].trim());
                if (ts.isBefore(fromInclusive) || !ts.isBefore(toExclusive)) {
                    continue;
                }
                bars.add(new Bar(
                        symbol,
                        ts,
                        Double.parseDouble(p[1].trim()),
                        Double.parseDouble(p[2].trim()),
                        Double.parseDouble(p[3].trim()),
                        Double.parseDouble(p[4].trim()),
                        Double.parseDouble(p[5].trim())
                ));
            }
        }
        return bars;
    }
}
