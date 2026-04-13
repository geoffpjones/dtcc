package com.markov.fx.pipeline;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class DtccOptionsCsvBackfillMain {
    private static final Logger LOG = Logger.getLogger(DtccOptionsCsvBackfillMain.class.getName());

    public static void main(String[] args) throws Exception {
        PipelineConfig cfg = PipelineConfig.parse(args);
        if (cfg.optionsInputCsv() == null) {
            throw new IllegalArgumentException("--options-input-csv is required for CSV backfill");
        }

        Path csvPath = cfg.optionsInputCsv();
        List<String> pairs = cfg.pairs();
        Set<String> pairSet = Set.copyOf(pairs);
        DtccOptionTradeRepository repo = new DtccOptionTradeRepository(cfg.dbPath());

        LOG.info(() -> "Starting DTCC CSV backfill from " + csvPath + " into " + cfg.dbPath());
        int inserted = backfill(csvPath, pairSet, repo);
        LOG.info(() -> "DTCC CSV backfill complete inserted_rows=" + inserted);
    }

    static int backfill(Path csvPath, Set<String> pairs, DtccOptionTradeRepository repo) throws Exception {
        int seen = 0;
        int inserted = 0;
        List<DtccPublicClient.OptionRow> batch = new ArrayList<>(10_000);

        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            for (CSVRecord rec : parser) {
                seen++;
                Map<String, String> row = rec.toMap();
                if (!DtccPublicClient.isOptionRow(row)) {
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

                String sourceDateText = row.getOrDefault("source_date", "");
                LocalDate sourceDate = sourceDateText.isBlank() ? inferSourceDate(row) : LocalDate.parse(sourceDateText);
                if (sourceDate == null) {
                    continue;
                }

                batch.add(new DtccPublicClient.OptionRow(
                        pair,
                        row.getOrDefault("source_file", "csv_backfill"),
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
                        rowHash(row.getOrDefault("source_file", "csv_backfill"), seen, fields)
                ));

                if (batch.size() >= 10_000) {
                    repo.insertTradeRows(batch);
                    inserted += batch.size();
                    batch.clear();
                }
            }
        }

        if (!batch.isEmpty()) {
            repo.insertTradeRows(batch);
            inserted += batch.size();
        }
        return inserted;
    }

    private static String pairFromUpi(String upiFisn, Set<String> pairs) {
        String upi = upiFisn == null ? "" : upiFisn.toUpperCase(Locale.ROOT);
        for (String pair : pairs) {
            String a = pair.substring(0, 3);
            String b = pair.substring(3);
            if (upi.contains(a + " " + b) || upi.contains(b + " " + a)) {
                return pair;
            }
        }
        return null;
    }

    private static LocalDate inferSourceDate(Map<String, String> row) {
        String eventTs = row.getOrDefault("Event timestamp", "");
        if (eventTs != null && eventTs.length() >= 10) {
            return LocalDate.parse(eventTs.substring(0, 10));
        }
        String effective = row.getOrDefault("Effective Date", "");
        if (effective != null && effective.length() >= 10) {
            return LocalDate.parse(effective.substring(0, 10));
        }
        return null;
    }

    private static double toDouble(String v) {
        if (v == null || v.isBlank()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(v.replace(",", "").trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static String rowHash(String sourceFile, int rowNum, List<String> fields) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
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
}
