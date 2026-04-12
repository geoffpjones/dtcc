package com.markov.fx.ingest;

import com.markov.fx.model.Bar;
import com.markov.fx.store.SqliteBarRepository;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class DukascopyFiveYearIngestMain {
    public static void main(String[] args) throws Exception {
        String symbol = args.length >= 1 ? args[0] : "EURUSD";
        String dbPath = args.length >= 2 ? args[1] : "data/fx-bars.db";
        LocalDate startInclusiveArg = args.length >= 3 ? LocalDate.parse(args[2]) : null;
        LocalDate endExclusiveArg = args.length >= 4 ? LocalDate.parse(args[3]) : null;

        LocalDate endExclusive = endExclusiveArg != null ? endExclusiveArg : LocalDate.now(ZoneOffset.UTC);
        LocalDate startInclusive = startInclusiveArg != null ? startInclusiveArg : endExclusive.minusYears(5);

        SqliteBarRepository repo = new SqliteBarRepository(dbPath);
        DukascopyBi5Client client = new DukascopyBi5Client();

        long totalDays = ChronoUnit.DAYS.between(startInclusive, endExclusive);
        long loadedBars = 0;
        long failedDays = 0;

        for (LocalDate d = startInclusive; d.isBefore(endExclusive); d = d.plusDays(1)) {
            try {
                List<Bar> bars = fetchWithRetry(client, symbol, d, 8);
                repo.save(symbol, bars);
                loadedBars += bars.size();
            } catch (Exception e) {
                failedDays++;
                System.err.printf("Failed %s: %s%n", d, e.getMessage());
            }

            Thread.sleep(150L);
            long dayIndex = ChronoUnit.DAYS.between(startInclusive, d) + 1;
            if (dayIndex % 30 == 0 || dayIndex == totalDays) {
                System.out.printf(
                        "Progress: %d/%d days, loaded bars=%d, failed days=%d%n",
                        dayIndex,
                        totalDays,
                        loadedBars,
                        failedDays
                );
            }
        }

        System.out.printf(
                "Completed. symbol=%s days=%d bars=%d failedDays=%d db=%s%n",
                symbol,
                totalDays,
                loadedBars,
                failedDays,
                dbPath
        );
    }

    private static List<Bar> fetchWithRetry(DukascopyBi5Client client, String symbol, LocalDate day, int maxAttempts)
            throws IOException, InterruptedException {
        long sleepMs = 500L;
        IOException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return client.fetchDay(symbol, day);
            } catch (IOException e) {
                last = e;
                if (!isRetryable(e) || attempt == maxAttempts) {
                    throw e;
                }
                Thread.sleep(sleepMs);
                sleepMs = Math.min(10_000L, sleepMs * 2);
            }
        }
        throw last == null ? new IOException("Unknown download error") : last;
    }

    private static boolean isRetryable(IOException e) {
        String m = e.getMessage();
        return m != null && (m.contains("status=503") || m.contains("status=429") || m.contains("status=502"));
    }
}
