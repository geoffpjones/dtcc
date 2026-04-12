package com.markov.fx.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvUtilsTest {
    @Test
    void parseLine_handlesQuotedCommasAndEscapedQuotes() {
        String line = "a,\"b,c\",\"x\"\"y\",z";
        List<String> parts = CsvUtils.parseLine(line);
        assertEquals(List.of("a", "b,c", "x\"y", "z"), parts);
    }

    @Test
    void parseLine_handlesEmptyTrailingColumns() {
        String line = "a,b,,";
        List<String> parts = CsvUtils.parseLine(line);
        assertEquals(List.of("a", "b", "", ""), parts);
    }

    @Test
    void escape_quotesWhenNeeded() {
        assertEquals("\"a,b\"", CsvUtils.escape("a,b"));
        assertEquals("\"a\"\"b\"", CsvUtils.escape("a\"b"));
        assertEquals("abc", CsvUtils.escape("abc"));
    }
}
