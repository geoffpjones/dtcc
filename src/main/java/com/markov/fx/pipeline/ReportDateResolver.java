package com.markov.fx.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Optional;

public final class ReportDateResolver {
    private ReportDateResolver() {
    }

    public static Optional<LocalDate> resolveEffectiveSignalDate(
            LocalDate requestedDate,
            Path hourlyCsv,
            Path strikeGammaCsv
    ) throws IOException {
        Optional<LocalDate> hourlyDate = latestHourlyDate(hourlyCsv);
        Optional<LocalDate> gammaDate = latestGammaDate(strikeGammaCsv);
        if (hourlyDate.isEmpty() || gammaDate.isEmpty()) {
            return Optional.empty();
        }
        LocalDate effective = requestedDate;
        if (hourlyDate.get().isBefore(effective)) {
            effective = hourlyDate.get();
        }
        if (gammaDate.get().isBefore(effective)) {
            effective = gammaDate.get();
        }
        return Optional.of(effective);
    }

    static Optional<LocalDate> latestHourlyDate(Path hourlyCsv) throws IOException {
        String last = lastDataLine(hourlyCsv);
        if (last == null || last.isBlank()) {
            return Optional.empty();
        }
        int comma = last.indexOf(',');
        if (comma <= 0) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.parse(last.substring(0, 10)));
    }

    static Optional<LocalDate> latestGammaDate(Path strikeGammaCsv) throws IOException {
        String last = lastDataLine(strikeGammaCsv);
        if (last == null || last.isBlank()) {
            return Optional.empty();
        }
        int comma = last.indexOf(',');
        if (comma <= 0) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.parse(last.substring(0, comma)));
    }

    private static String lastDataLine(Path csv) throws IOException {
        if (!Files.exists(csv)) {
            return null;
        }
        try (BufferedReader reader = Files.newBufferedReader(csv)) {
            String line = reader.readLine(); // skip header
            String last = null;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    last = line;
                }
            }
            return last;
        }
    }
}
