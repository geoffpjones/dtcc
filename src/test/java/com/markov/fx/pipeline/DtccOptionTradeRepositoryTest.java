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
                LocalDate.parse("2026-01-31"),
                "EURUSD",
                new LimitSignalCalculator.LimitRow(
                        LocalDate.parse("2026-02-01"), Double.NaN, 1.08, Double.NaN, Double.NaN,
                        false, false, null, null, 0.0, 0.0, 0.0, 0.0, "missing eod"
                ),
                new LimitSignalCalculator.LimitRow(
                        LocalDate.parse("2026-02-01"), Double.NaN, 1.07, 1.09, Double.NaN,
                        false, false, null, null, 0.0, 0.0, 0.0, 0.0, "alt signal"
                ),
                LimitSignalCalculator.ALT_SIGNAL,
                new LimitSignalCalculator.LimitRow(
                        LocalDate.parse("2026-02-01"), Double.NaN, 1.07, 1.09, Double.NaN,
                        false, false, null, null, 0.0, 0.0, 0.0, 0.0, "alt signal"
                ),
                "alt signal",
                new ExitParamSelection.ExitParams("trail_after_tp", 20.0, 30.0, 5.0),
                new DailyRocPipelineMain.TradeLevels(1.09, 1.04, 1.05, 1.12)
        );

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT effective_signal_date,ref_price_prev_close,buy_limit,sell_limit,eod_close,notes,alt_buy_limit,alt_sell_limit,alt_notes,selected_signal,selected_buy_limit,selected_sell_limit,selected_notes,selected_exit_mode,selected_tp_pips,selected_sl_pips,selected_trail_pips,selected_buy_tp_level,selected_buy_sl_level,selected_sell_tp_level,selected_sell_sl_level FROM gamma_limit_reports")) {
            assertTrue(rs.next());
            assertEquals("2026-01-31", rs.getString("effective_signal_date"));
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
            assertEquals("gamma_nearest_top1_md50_dec1", rs.getString("selected_signal"));
            assertEquals(1.07, rs.getDouble("selected_buy_limit"));
            assertEquals(1.09, rs.getDouble("selected_sell_limit"));
            assertEquals("alt signal", rs.getString("selected_notes"));
            assertEquals("trail_after_tp", rs.getString("selected_exit_mode"));
            assertEquals(20.0, rs.getDouble("selected_tp_pips"));
            assertEquals(30.0, rs.getDouble("selected_sl_pips"));
            assertEquals(5.0, rs.getDouble("selected_trail_pips"));
            assertEquals(1.09, rs.getDouble("selected_buy_tp_level"));
            assertEquals(1.04, rs.getDouble("selected_buy_sl_level"));
            assertEquals(1.05, rs.getDouble("selected_sell_tp_level"));
            assertEquals(1.12, rs.getDouble("selected_sell_sl_level"));
        }
    }
}
