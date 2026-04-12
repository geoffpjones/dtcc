package com.markov.fx.store;

import com.markov.fx.model.Bar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SqliteBarRepository implements BarRepository {
    private final String dbUrl;
    private final Path dbPath;

    public SqliteBarRepository(String dbPath) {
        this.dbPath = Path.of(dbPath).toAbsolutePath().normalize();
        this.dbUrl = "jdbc:sqlite:" + this.dbPath;
        init();
    }

    private void init() {
        try {
            Path parent = dbPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create DB directory for " + dbPath, e);
        }

        String ddl = """
                CREATE TABLE IF NOT EXISTS fx_bars (
                    symbol TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    PRIMARY KEY(symbol, ts_utc)
                )
                """;
        try (Connection c = DriverManager.getConnection(dbUrl); Statement st = c.createStatement()) {
            st.execute(ddl);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize SQLite schema", e);
        }
    }

    @Override
    public void save(String symbol, List<Bar> bars) {
        String sql = """
                INSERT INTO fx_bars(symbol, ts_utc, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, ts_utc) DO NOTHING
                """;

        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);
            for (Bar b : bars) {
                ps.setString(1, symbol);
                ps.setString(2, b.timestamp().toString());
                ps.setDouble(3, b.open());
                ps.setDouble(4, b.high());
                ps.setDouble(5, b.low());
                ps.setDouble(6, b.close());
                ps.setDouble(7, b.volume());
                ps.addBatch();
            }
            ps.executeBatch();
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save bars", e);
        }
    }

    @Override
    public List<Bar> load(String symbol, Instant fromInclusive, Instant toExclusive) throws SQLException {
        String sql = """
                SELECT ts_utc, open, high, low, close, volume
                FROM fx_bars
                WHERE symbol = ? AND ts_utc >= ? AND ts_utc < ?
                ORDER BY ts_utc ASC
                """;
        List<Bar> bars = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, symbol);
            ps.setString(2, fromInclusive.toString());
            ps.setString(3, toExclusive.toString());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    bars.add(new Bar(
                            symbol,
                            Instant.parse(rs.getString("ts_utc")),
                            rs.getDouble("open"),
                            rs.getDouble("high"),
                            rs.getDouble("low"),
                            rs.getDouble("close"),
                            rs.getDouble("volume")
                    ));
                }
            }
        }
        return bars;
    }

    public Optional<Instant> latestTimestamp(String symbol) {
        String sql = """
                SELECT MAX(ts_utc) AS max_ts
                FROM fx_bars
                WHERE symbol = ?
                """;
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, symbol);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String ts = rs.getString("max_ts");
                    if (ts != null && !ts.isBlank()) {
                        return Optional.of(Instant.parse(ts));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query latest timestamp for " + symbol, e);
        }
        return Optional.empty();
    }

    public Optional<LocalDate> latestDate(String symbol) {
        return latestTimestamp(symbol).map(ts -> ts.atZone(ZoneOffset.UTC).toLocalDate());
    }
}
