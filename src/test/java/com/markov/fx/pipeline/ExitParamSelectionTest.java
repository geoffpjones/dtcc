package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExitParamSelectionTest {
    @TempDir
    Path tempDir;

    @Test
    void load_readsPerPairExitParams() throws Exception {
        Path csv = tempDir.resolve("exit_params.csv");
        Files.writeString(csv, String.join("\n",
                "pair,mode,tp_pips,sl_pips,trail_pips",
                "EURUSD,trail_after_tp,20,30,5",
                "USDJPY,trail_after_tp,20,15,5"
        ));

        ExitParamSelection selection = ExitParamSelection.load(csv);

        assertEquals("trail_after_tp", selection.selectedFor("EURUSD").mode());
        assertEquals(20.0, selection.selectedFor("EURUSD").tpPips());
        assertEquals(30.0, selection.selectedFor("EURUSD").slPips());
        assertEquals(5.0, selection.selectedFor("USDJPY").trailPips());
    }
}
