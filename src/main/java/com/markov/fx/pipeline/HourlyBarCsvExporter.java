package com.markov.fx.pipeline;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class HourlyBarCsvExporter {
    private final String dbUrl;

    public HourlyBarCsvExporter(String dbPath) {
        this.dbUrl = "jdbc:sqlite:" + dbPath;
    }

    public int exportHourly(String symbol, Path outputCsv) {
        String sql = """
                SELECT ts_utc, open, high, low, close, volume
                FROM fx_bars
                WHERE symbol = ?
                ORDER BY ts_utc ASC
                """;

        try {
            Files.createDirectories(outputCsv.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output folder for " + outputCsv, e);
        }

        int rows = 0;
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql);
             BufferedWriter w = Files.newBufferedWriter(outputCsv)) {

            ps.setString(1, symbol);
            w.write("timestamp_utc,open,high,low,close,volume");
            w.newLine();

            try (ResultSet rs = ps.executeQuery()) {
                Instant hourTs = null;
                double o = 0.0;
                double h = 0.0;
                double l = 0.0;
                double cLast = 0.0;
                double v = 0.0;
                boolean haveHour = false;

                while (rs.next()) {
                    Instant ts = Instant.parse(rs.getString("ts_utc"));
                    Instant bucket = ts.truncatedTo(ChronoUnit.HOURS);
                    double bo = rs.getDouble("open");
                    double bh = rs.getDouble("high");
                    double bl = rs.getDouble("low");
                    double bc = rs.getDouble("close");
                    double bv = rs.getDouble("volume");

                    if (!haveHour || !bucket.equals(hourTs)) {
                        if (haveHour) {
                            writeHour(w, hourTs, o, h, l, cLast, v);
                            rows++;
                        }
                        hourTs = bucket;
                        o = bo;
                        h = bh;
                        l = bl;
                        cLast = bc;
                        v = bv;
                        haveHour = true;
                    } else {
                        h = Math.max(h, bh);
                        l = Math.min(l, bl);
                        cLast = bc;
                        v += bv;
                    }
                }

                if (haveHour) {
                    writeHour(w, hourTs, o, h, l, cLast, v);
                    rows++;
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Failed to export hourly bars for " + symbol + " to " + outputCsv, e);
        }
        return rows;
    }

    private static void writeHour(BufferedWriter w, Instant ts, double o, double h, double l, double c, double v) throws IOException {
        w.write(ts + "," + o + "," + h + "," + l + "," + c + "," + v);
        w.newLine();
    }
}
