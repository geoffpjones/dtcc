package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DtccOptionTradeRepositoryTest {
    @TempDir
    Path tempDir;

    @Test
    void upsertLimitReportRow_storesNullForMissingNumericValues() throws Exception {
        Path db = tempDir.resolve("pipeline.db");
        DtccOptionTradeRepository repo = new DtccOptionTradeRepository(db.toString());

        repo.upsertLimitReportRow(
                LocalDate.parse("2026-02-01"),
                "EURUSD",
                new LimitSignalCalculator.LimitRow(
                        LocalDate.parse("2026-02-01"), Double.NaN, 1.08, Double.NaN, Double.NaN,
                        false, false, null, null, 0.0, 0.0, 0.0, 0.0, "missing eod"
                ),
                new LimitSignalCalculator.LimitRow(
                        LocalDate.parse("2026-02-01"), Double.NaN, 1.07, 1.09, Double.NaN,
                        false, false, null, null, 0.0, 0.0, 0.0, 0.0, "alt signal"
                )
        );

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT ref_price_prev_close,buy_limit,sell_limit,eod_close,notes,alt_buy_limit,alt_sell_limit,alt_notes FROM gamma_limit_reports")) {
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble("ref_price_prev_close"));
            assertTrue(rs.wasNull());
            assertEquals(1.08, rs.getDouble("buy_limit"));
            assertEquals(0.0, rs.getDouble("sell_limit"));
            assertTrue(rs.wasNull());
            assertEquals(0.0, rs.getDouble("eod_close"));
            assertTrue(rs.wasNull());
            assertEquals("missing eod", rs.getString("notes"));
            assertEquals(1.07, rs.getDouble("alt_buy_limit"));
            assertEquals(1.09, rs.getDouble("alt_sell_limit"));
            assertEquals("alt signal", rs.getString("alt_notes"));
        }
    }
}
