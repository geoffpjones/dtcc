package com.markov.fx.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.logging.Logger;

public class DtccPublicClient {
    private static final String BASE = "https://pddata.dtcc.com/ppd/api";
    private static final Pattern FILE_DATE_RE = Pattern.compile("_(\\d{4})_(\\d{2})_(\\d{2})\\.zip$");
    private static final int HTTP_TIMEOUT_SECONDS = 90;
    private static final Logger LOG = Logger.getLogger(DtccPublicClient.class.getName());

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public int ingestRange(
            String regime,
            String asset,
            LocalDate startInclusive,
            LocalDate endInclusive,
            Set<String> pairs,
            DtccOptionTradeRepository repository
    ) throws Exception {
        if (startInclusive.isAfter(endInclusive)) {
            return 0;
        }

        List<String> files = listFiles(regime, asset);
        int parsedFiles = 0;

        for (String fileName : files) {
            LocalDate d = fileDate(fileName);
            if (d == null || d.isBefore(startInclusive) || d.isAfter(endInclusive)) {
                continue;
            }
            if (repository.isFileIngested(fileName)) {
                continue;
            }

            byte[] zipBytes = downloadZip(fileName);
            ParseResult parseResult = parseZip(fileName, d, zipBytes, pairs);
            repository.insertTradeRows(parseResult.optionRows());
            repository.markFileIngested(fileName, d, parseResult.totalRows(), parseResult.optionRows().size());
            parsedFiles++;
            LOG.info(() -> String.format(
                    "DTCC ingested file=%s rows=%d option_rows=%d",
                    fileName,
                    parseResult.totalRows(),
                    parseResult.optionRows().size()
            ));
        }

        return parsedFiles;
    }

    private List<String> listFiles(String regime, String asset) throws IOException, InterruptedException {
        String url = BASE + "/cumulative/" + regime + "/" + asset;
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(java.time.Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                .GET()
                .build();
        HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 400) {
            throw new IOException("DTCC list request failed status=" + res.statusCode() + " url=" + url);
        }
        List<String> out = new ArrayList<>();
        JsonNode root = objectMapper.readTree(res.body());
        if (root == null || !root.isArray()) {
            throw new IOException("Unexpected DTCC list JSON format (expected array)");
        }
        for (JsonNode node : root) {
            JsonNode fileName = node.get("fileName");
            if (fileName != null && fileName.isTextual() && !fileName.asText().isBlank()) {
                out.add(fileName.asText());
            }
        }
        return out;
    }

    private byte[] downloadZip(String fileName) throws IOException, InterruptedException {
        String prefix = fileName.split("_", 2)[0].toLowerCase();
        String url = BASE + "/report/cumulative/" + prefix + "/" + fileName;
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(java.time.Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                .GET()
                .build();
        HttpResponse<byte[]> res = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (res.statusCode() >= 400) {
            throw new IOException("DTCC download failed status=" + res.statusCode() + " url=" + url);
        }
        return res.body();
    }

    private ParseResult parseZip(String sourceFile, LocalDate sourceDate, byte[] zipBytes, Set<String> pairs) throws Exception {
        int totalRows = 0;
        List<OptionRow> optionRows = new ArrayList<>();

        try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(zipBytes), StandardCharsets.UTF_8)) {
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if (entry.isDirectory() || !entry.getName().toLowerCase().endsWith(".csv")) {
                    continue;
                }

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(zin, StandardCharsets.UTF_8));
                     CSVParser parser = CSVFormat.DEFAULT.builder()
                             .setHeader()
                             .setSkipHeaderRecord(true)
                             .setIgnoreSurroundingSpaces(false)
                             .build()
                             .parse(reader)) {
                    for (CSVRecord rec : parser) {
                        totalRows++;
                        Map<String, String> row = rec.toMap();
                        if (!isOptionRow(row)) {
                            continue;
                        }

                        String upi = row.getOrDefault("UPI FISN", "");
                        String pair = pairFromUpi(upi, pairs);
                        if (pair == null) {
                            continue;
                        }

                        List<String> fields = new ArrayList<>(rec.size());
                        for (int i = 0; i < rec.size(); i++) {
                            fields.add(rec.get(i));
                        }

                        OptionRow parsed = new OptionRow(
                                pair,
                                sourceFile,
                                sourceDate,
                                row.getOrDefault("Action type", ""),
                                upi,
                                toDouble(row.get("Strike Price")),
                                row.getOrDefault("Expiration Date", ""),
                                row.getOrDefault("Event timestamp", ""),
                                row.getOrDefault("Notional currency-Leg 1", ""),
                                toDouble(row.get("Notional amount-Leg 1")),
                                row.getOrDefault("Notional currency-Leg 2", ""),
                                toDouble(row.get("Notional amount-Leg 2")),
                                row.getOrDefault("Embedded Option type", ""),
                                row.getOrDefault("Option Type", ""),
                                row.getOrDefault("Option Style", ""),
                                row.getOrDefault("Product name", ""),
                                rowHash(sourceFile, totalRows, fields)
                        );
                        optionRows.add(parsed);
                    }
                }
                break; // process first csv entry only
            }
        }
        return new ParseResult(totalRows, optionRows);
    }

    private static boolean isOptionRow(Map<String, String> row) {
        String embedded = row.getOrDefault("Embedded Option type", "").trim();
        String otype = row.getOrDefault("Option Type", "").trim();
        String ostyle = row.getOrDefault("Option Style", "").trim();
        String pname = row.getOrDefault("Product name", "").toUpperCase();
        String upiFisn = row.getOrDefault("UPI FISN", "").toUpperCase();
        return !embedded.isEmpty() || !otype.isEmpty() || !ostyle.isEmpty() || pname.contains("OPTION") || upiFisn.contains("OPTION");
    }

    private static String pairFromUpi(String upiFisn, Set<String> pairs) {
        String upi = upiFisn == null ? "" : upiFisn.toUpperCase();
        for (String pair : pairs) {
            String a = pair.substring(0, 3);
            String b = pair.substring(3);
            if (upi.contains(a + " " + b) || upi.contains(b + " " + a)) {
                return pair;
            }
        }
        return null;
    }

    private static LocalDate fileDate(String filename) {
        Matcher m = FILE_DATE_RE.matcher(filename);
        if (!m.find()) {
            return null;
        }
        int y = Integer.parseInt(m.group(1));
        int mm = Integer.parseInt(m.group(2));
        int dd = Integer.parseInt(m.group(3));
        return LocalDate.of(y, mm, dd);
    }

    private static double toDouble(String v) {
        if (v == null || v.isBlank()) {
            return 0.0;
        }
        String clean = v.replace(",", "").trim();
        try {
            return Double.parseDouble(clean);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static String rowHash(String sourceFile, int rowNum, List<String> fields) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException("Missing SHA-256 MessageDigest provider", e);
        }
        digest.update(sourceFile.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '|');
        digest.update(Integer.toString(rowNum).getBytes(StandardCharsets.UTF_8));
        for (String f : fields) {
            digest.update((byte) '|');
            if (f != null) {
                digest.update(f.getBytes(StandardCharsets.UTF_8));
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private record ParseResult(int totalRows, List<OptionRow> optionRows) {
    }

    public record OptionRow(
            String pair,
            String sourceFile,
            LocalDate sourceDate,
            String actionType,
            String upiFisn,
            double strikePrice,
            String expirationDate,
            String eventTimestamp,
            String notionalCcyLeg1,
            double notionalAmtLeg1,
            String notionalCcyLeg2,
            double notionalAmtLeg2,
            String embeddedOptionType,
            String optionType,
            String optionStyle,
            String productName,
            String rowHash
    ) {
    }
}
