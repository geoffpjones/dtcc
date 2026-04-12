package com.markov.fx.ingest;

import com.markov.fx.model.Bar;
import org.tukaani.xz.LZMAInputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class DukascopyBi5Client {
    private static final String ROOT = "https://datafeed.dukascopy.com/datafeed";

    private final HttpClient httpClient;

    public DukascopyBi5Client() {
        this.httpClient = HttpClient.newHttpClient();
    }

    public List<Bar> fetchDay(String symbol, LocalDate dayUtc) throws IOException, InterruptedException {
        URI uri = URI.create(buildDailyBi5Url(symbol, dayUtc));
        HttpRequest req = HttpRequest.newBuilder(uri).GET().build();
        HttpResponse<byte[]> res = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());

        if (res.statusCode() == 404) {
            return List.of();
        }
        if (res.statusCode() >= 400) {
            throw new IOException("Dukascopy request failed: status=" + res.statusCode() + " url=" + uri);
        }

        return decodeCandles(symbol, dayUtc, res.body());
    }

    static String buildDailyBi5Url(String symbol, LocalDate dayUtc) {
        int monthZeroBased = dayUtc.getMonthValue() - 1;
        return String.format(
                "%s/%s/%04d/%02d/%02d/BID_candles_min_1.bi5",
                ROOT,
                symbol,
                dayUtc.getYear(),
                monthZeroBased,
                dayUtc.getDayOfMonth()
        );
    }

    private List<Bar> decodeCandles(String symbol, LocalDate dayUtc, byte[] compressedBi5) throws IOException {
        List<Bar> bars = new ArrayList<>(1440);
        try (LZMAInputStream lzma = new LZMAInputStream(new ByteArrayInputStream(compressedBi5));
             DataInputStream in = new DataInputStream(lzma)) {

            long dayStartEpochSecond = dayUtc.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
            double pipScale = priceScale(symbol);

            while (true) {
                try {
                    int secondsOffset = in.readInt();
                    int open = in.readInt();
                    int close = in.readInt();
                    int low = in.readInt();
                    int high = in.readInt();
                    float volume = in.readFloat();

                    Instant ts = Instant.ofEpochSecond(dayStartEpochSecond + secondsOffset);
                    bars.add(new Bar(
                            symbol,
                            ts,
                            open / pipScale,
                            high / pipScale,
                            low / pipScale,
                            close / pipScale,
                            volume
                    ));
                } catch (EOFException eof) {
                    break;
                }
            }
        }
        return bars;
    }

    private static double priceScale(String symbol) {
        if (symbol.endsWith("JPY")) {
            return 1_000.0;
        }
        return 100_000.0;
    }
}
