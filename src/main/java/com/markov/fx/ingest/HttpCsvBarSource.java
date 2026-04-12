package com.markov.fx.ingest;

import com.markov.fx.model.Bar;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class HttpCsvBarSource implements BarSource {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final URI endpoint;

    public HttpCsvBarSource(URI endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public List<Bar> fetch(String symbol, Instant fromInclusive, Instant toExclusive) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(endpoint).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IOException("HTTP CSV fetch failed with status " + response.statusCode());
        }
        List<Bar> bars = new ArrayList<>();
        String[] lines = response.body().split("\\R");
        for (String line : lines) {
            if (line.isBlank() || line.toLowerCase().contains("timestamp")) {
                continue;
            }
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
        return bars;
    }
}
