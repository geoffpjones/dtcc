package com.markov.fx.pipeline;

import com.markov.fx.util.CsvUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

public class DtccOptionTradeRepository {
    private static final Logger LOG = Logger.getLogger(DtccOptionTradeRepository.class.getName());
    private final String dbUrl;
    private final Path dbPath;

    public DtccOptionTradeRepository(String dbPath) {
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

        String tradesDdl = """
                CREATE TABLE IF NOT EXISTS dtcc_option_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    source_date TEXT NOT NULL,
                    action_type TEXT,
                    upi_fisn TEXT,
                    strike_price REAL,
                    expiration_date TEXT,
                    event_timestamp TEXT,
                    notional_ccy_leg1 TEXT,
                    notional_amt_leg1 REAL,
                    notional_ccy_leg2 TEXT,
                    notional_amt_leg2 REAL,
                    embedded_option_type TEXT,
                    option_type TEXT,
                    option_style TEXT,
                    product_name TEXT,
                    row_hash TEXT NOT NULL UNIQUE,
                    inserted_at_utc TEXT NOT NULL
                )
                """;

        String filesDdl = """
                CREATE TABLE IF NOT EXISTS dtcc_ingested_files (
                    source_file TEXT PRIMARY KEY,
                    source_date TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    option_row_count INTEGER NOT NULL,
                    ingested_at_utc TEXT NOT NULL
                )
                """;

        String reportsDdl = """
                CREATE TABLE IF NOT EXISTS gamma_limit_reports (
                    trade_date TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    ref_price_prev_close REAL,
                    buy_limit REAL,
                    sell_limit REAL,
                    eod_close REAL,
                    notes TEXT,
                    generated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (trade_date, pair)
                )
                """;

        try (Connection c = DriverManager.getConnection(dbUrl);
             Statement st = c.createStatement()) {
            st.execute(tradesDdl);
            st.execute("CREATE INDEX IF NOT EXISTS idx_dtcc_pair_date ON dtcc_option_trades(pair, source_date)");
            st.execute(filesDdl);
            st.execute(reportsDdl);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize DTCC SQLite schema", e);
        }
        LOG.info(() -> "SQLite schema ready at " + dbPath);
    }

    public Optional<LocalDate> latestSourceDate() {
        String sql = "SELECT MAX(source_date) AS max_source_date FROM dtcc_ingested_files";
        try (Connection c = DriverManager.getConnection(dbUrl);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                String v = rs.getString("max_source_date");
                if (v != null && !v.isBlank()) {
                    return Optional.of(LocalDate.parse(v));
                }
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query latest dtcc source_date", e);
        }
    }

    public boolean isFileIngested(String sourceFile) {
        String sql = "SELECT 1 FROM dtcc_ingested_files WHERE source_file = ? LIMIT 1";
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sourceFile);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check ingested file: " + sourceFile, e);
        }
    }

    public void insertTradeRows(Iterable<DtccPublicClient.OptionRow> rows) {
        String sql = """
                INSERT OR IGNORE INTO dtcc_option_trades (
                    pair, source_file, source_date, action_type, upi_fisn, strike_price, expiration_date, event_timestamp,
                    notional_ccy_leg1, notional_amt_leg1, notional_ccy_leg2, notional_amt_leg2,
                    embedded_option_type, option_type, option_style, product_name, row_hash, inserted_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        Instant now = Instant.now();
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);
            try {
                for (DtccPublicClient.OptionRow row : rows) {
                    ps.setString(1, row.pair());
                    ps.setString(2, row.sourceFile());
                    ps.setString(3, row.sourceDate().toString());
                    ps.setString(4, row.actionType());
                    ps.setString(5, row.upiFisn());
                    ps.setDouble(6, row.strikePrice());
                    ps.setString(7, row.expirationDate());
                    ps.setString(8, row.eventTimestamp());
                    ps.setString(9, row.notionalCcyLeg1());
                    ps.setDouble(10, row.notionalAmtLeg1());
                    ps.setString(11, row.notionalCcyLeg2());
                    ps.setDouble(12, row.notionalAmtLeg2());
                    ps.setString(13, row.embeddedOptionType());
                    ps.setString(14, row.optionType());
                    ps.setString(15, row.optionStyle());
                    ps.setString(16, row.productName());
                    ps.setString(17, row.rowHash());
                    ps.setString(18, now.toString());
                    ps.addBatch();
                }
                ps.executeBatch();
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert dtcc option rows", e);
        }
    }

    public void markFileIngested(String sourceFile, LocalDate sourceDate, int rowCount, int optionRowCount) {
        String sql = """
                INSERT INTO dtcc_ingested_files(source_file, source_date, row_count, option_row_count, ingested_at_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_file) DO UPDATE SET
                    source_date=excluded.source_date,
                    row_count=excluded.row_count,
                    option_row_count=excluded.option_row_count,
                    ingested_at_utc=excluded.ingested_at_utc
                """;
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sourceFile);
            ps.setString(2, sourceDate.toString());
            ps.setInt(3, rowCount);
            ps.setInt(4, optionRowCount);
            ps.setString(5, Instant.now().toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark ingested dtcc file: " + sourceFile, e);
        }
    }

    public void exportOptionsCsv(Path outputCsv, Iterable<String> pairs) {
        String sql = """
                SELECT action_type, upi_fisn, strike_price, expiration_date, source_date, event_timestamp,
                       notional_ccy_leg1, notional_amt_leg1, notional_ccy_leg2, notional_amt_leg2,
                       embedded_option_type, option_type, option_style, product_name
                FROM dtcc_option_trades
                WHERE pair = ?
                ORDER BY source_date ASC, id ASC
                """;
        try {
            Path parent = outputCsv.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed creating output directory for " + outputCsv, e);
        }

        try (BufferedWriter w = Files.newBufferedWriter(outputCsv);
             Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {

            w.write(String.join(",",
                    "Action type",
                    "UPI FISN",
                    "Strike Price",
                    "Expiration Date",
                    "source_date",
                    "Event timestamp",
                    "Notional currency-Leg 1",
                    "Notional amount-Leg 1",
                    "Notional currency-Leg 2",
                    "Notional amount-Leg 2",
                    "Embedded Option type",
                    "Option Type",
                    "Option Style",
                    "Product name"));
            w.newLine();

            for (String pair : pairs) {
                ps.setString(1, pair);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String line = String.join(",",
                                csv(rs.getString("action_type")),
                                csv(rs.getString("upi_fisn")),
                                csv(doubleText(rs.getDouble("strike_price"))),
                                csv(rs.getString("expiration_date")),
                                csv(rs.getString("source_date")),
                                csv(rs.getString("event_timestamp")),
                                csv(rs.getString("notional_ccy_leg1")),
                                csv(doubleText(rs.getDouble("notional_amt_leg1"))),
                                csv(rs.getString("notional_ccy_leg2")),
                                csv(doubleText(rs.getDouble("notional_amt_leg2"))),
                                csv(rs.getString("embedded_option_type")),
                                csv(rs.getString("option_type")),
                                csv(rs.getString("option_style")),
                                csv(rs.getString("product_name")));
                        w.write(line);
                        w.newLine();
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Failed to export options CSV to " + outputCsv, e);
        }
    }

    public void upsertLimitReportRow(LocalDate tradeDate, String pair, Map<String, String> row) {
        String sql = """
                INSERT INTO gamma_limit_reports(
                    trade_date, pair, ref_price_prev_close, buy_limit, sell_limit, eod_close, notes, generated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, pair) DO UPDATE SET
                    ref_price_prev_close=excluded.ref_price_prev_close,
                    buy_limit=excluded.buy_limit,
                    sell_limit=excluded.sell_limit,
                    eod_close=excluded.eod_close,
                    notes=excluded.notes,
                    generated_at_utc=excluded.generated_at_utc
                """;
        try (Connection c = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, tradeDate.toString());
            ps.setString(2, pair);
            setNullableDouble(ps, 3, parseNullableDouble(row.get("ref_price_prev_close")));
            setNullableDouble(ps, 4, parseNullableDouble(row.get("buy_limit")));
            setNullableDouble(ps, 5, parseNullableDouble(row.get("sell_limit")));
            setNullableDouble(ps, 6, parseNullableDouble(row.get("eod_close")));
            ps.setString(7, row.getOrDefault("notes", ""));
            ps.setString(8, Instant.now().toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save gamma_limit_reports row for " + pair + " " + tradeDate, e);
        }
    }

    private static String csv(String v) {
        return CsvUtils.escape(v);
    }

    private static String doubleText(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return "";
        }
        return Double.toString(v);
    }

    private static Double parseNullableDouble(String v) {
        if (v == null || v.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(v);
        } catch (Exception e) {
            return null;
        }
    }

    private static void setNullableDouble(PreparedStatement ps, int idx, Double value) throws SQLException {
        if (value == null || value.isNaN() || value.isInfinite()) {
            ps.setNull(idx, Types.REAL);
        } else {
            ps.setDouble(idx, value);
        }
    }
}
