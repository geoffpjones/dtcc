package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DtccOptionsCsvBackfillMainTest {
    @TempDir
    Path tempDir;

    @Test
    void backfill_loadsTargetPairRowsFromCsv() throws Exception {
        Path csv = tempDir.resolve("options.csv");
        Files.writeString(csv, String.join("\n",
                "Action type,UPI FISN,Strike Price,Expiration Date,Event timestamp,Notional currency-Leg 1,Notional amount-Leg 1,Notional currency-Leg 2,Notional amount-Leg 2,Embedded Option type,Option Type,Option Style,Product name,source_file,source_date",
                "NEWT,NA/O Van Put USD JPY,155,2026-04-30,2026-04-12T12:00:00Z,USD,1000000,JPY,155000000,,,,,dtcc_a.csv,2026-04-12",
                "NEWT,NA/Fwd NDF USD JPY,155,2026-04-30,2026-04-12T12:00:00Z,USD,1000000,JPY,155000000,,,,,dtcc_b.csv,2026-04-12"
        ));
        Path db = tempDir.resolve("test.db");
        DtccOptionTradeRepository repo = new DtccOptionTradeRepository(db.toString());

        int inserted = DtccOptionsCsvBackfillMain.backfill(csv, Set.of("USDJPY"), repo);
        assertEquals(1, inserted);

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("select pair, count(*) as cnt from dtcc_option_trades group by pair")) {
            rs.next();
            assertEquals("USDJPY", rs.getString("pair"));
            assertEquals(1, rs.getInt("cnt"));
        }
    }
}
