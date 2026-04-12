package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DtccOptionTradeRepositoryTest {
    @TempDir
    Path tempDir;

    @Test
    void upsertLimitReportRow_storesNullForMissingNumericValues() throws Exception {
        Path db = tempDir.resolve("pipeline.db");
        DtccOptionTradeRepository repo = new DtccOptionTradeRepository(db.toString());

        repo.upsertLimitReportRow(LocalDate.parse("2026-02-01"), "EURUSD", Map.of(
                "ref_price_prev_close", "",
                "buy_limit", "1.08",
                "sell_limit", "nan",
                "notes", "missing eod"
        ));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT ref_price_prev_close,buy_limit,sell_limit,eod_close,notes FROM gamma_limit_reports")) {
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble("ref_price_prev_close"));
            assertTrue(rs.wasNull());
            assertEquals(1.08, rs.getDouble("buy_limit"));
            assertEquals(0.0, rs.getDouble("sell_limit"));
            assertTrue(rs.wasNull());
            assertEquals(0.0, rs.getDouble("eod_close"));
            assertTrue(rs.wasNull());
            assertEquals("missing eod", rs.getString("notes"));
        }
    }
}
