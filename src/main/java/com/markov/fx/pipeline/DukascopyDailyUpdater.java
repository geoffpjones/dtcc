package com.markov.fx.pipeline;

import com.markov.fx.ingest.DukascopyBi5Client;
import com.markov.fx.model.Bar;
import com.markov.fx.store.SqliteBarRepository;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.logging.Logger;

public class DukascopyDailyUpdater {
    private static final Logger LOG = Logger.getLogger(DukascopyDailyUpdater.class.getName());
    private final DukascopyBi5Client client;
    private final SqliteBarRepository barRepository;

    public DukascopyDailyUpdater(DukascopyBi5Client client, SqliteBarRepository barRepository) {
        this.client = client;
        this.barRepository = barRepository;
    }

    public long updateSymbol(String symbol, LocalDate startInclusive, LocalDate endInclusive) throws Exception {
        if (startInclusive.isAfter(endInclusive)) {
            return 0L;
        }
        long totalDays = ChronoUnit.DAYS.between(startInclusive, endInclusive.plusDays(1));
        long loadedBars = 0L;
        long failedDays = 0L;
        long idx = 0L;

        for (LocalDate d = startInclusive; !d.isAfter(endInclusive); d = d.plusDays(1)) {
            idx++;
            try {
                List<Bar> bars = fetchWithRetry(symbol, d, 8);
                barRepository.save(symbol, bars);
                loadedBars += bars.size();
            } catch (Exception e) {
                failedDays++;
                LOG.warning("Dukascopy failed symbol=" + symbol + " day=" + d + " error=" + e.getMessage());
            }
            Thread.sleep(150L);
            if (idx % 30 == 0 || idx == totalDays) {
                LOG.info(String.format(
                        "Dukascopy progress symbol=%s %d/%d days loadedBars=%d failedDays=%d",
                        symbol, idx, totalDays, loadedBars, failedDays
                ));
            }
        }
        return loadedBars;
    }

    private List<Bar> fetchWithRetry(String symbol, LocalDate day, int maxAttempts) throws IOException, InterruptedException {
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
        throw last == null ? new IOException("Unknown Dukascopy download error") : last;
    }

    private static boolean isRetryable(IOException e) {
        String m = e.getMessage();
        return m != null && (m.contains("status=503") || m.contains("status=429") || m.contains("status=502"));
    }
}
