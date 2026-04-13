package com.markov.fx.pipeline;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DtccPublicClientTest {
    @Test
    void isOptionRow_acceptsNaOVanRowsEvenWhenMetadataIsBlank() {
        assertTrue(DtccPublicClient.isOptionRow(Map.of(
                "UPI FISN", "NA/O Van Put USD JPY",
                "Embedded Option type", "",
                "Option Type", "",
                "Option Style", "",
                "Product name", ""
        )));
    }

    @Test
    void isOptionRow_rejectsPlainForwardRows() {
        assertFalse(DtccPublicClient.isOptionRow(Map.of(
                "UPI FISN", "NA/Fwd NDF USD JPY",
                "Embedded Option type", "",
                "Option Type", "",
                "Option Style", "",
                "Product name", ""
        )));
    }
}
