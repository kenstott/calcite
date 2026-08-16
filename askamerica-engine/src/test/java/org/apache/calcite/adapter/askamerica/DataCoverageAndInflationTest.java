/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two honesty tools that answer from the warehouse's own state rather than from a model's
 * recollection: what years a table really holds ({@code data_coverage}), and what a dollar
 * from one year is worth in another ({@code adjust_inflation}).
 *
 * <p>Both fail in the same direction if they are wrong — quietly, and in a way that looks like
 * a finding. A missed interior gap turns a discontinuous series into a trend; a wrong deflator
 * turns price movement into growth. So the assertions here are about what each refuses to do
 * as much as what it computes.
 */
@Tag("unit")
class DataCoverageAndInflationTest {

    private static final double EPS = 1e-9;

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper();

    // ── data_coverage ─────────────────────────────────────────────────────────

    /** 2015-2017 and 2021-2023 loaded; 2018-2020 absent — the case bounds alone cannot show. */
    private static IngestedYears.Result holed() {
        Map<Integer, Long> counts = new LinkedHashMap<>();
        counts.put(2015, 100L);
        counts.put(2016, 110L);
        counts.put(2017, 120L);
        counts.put(2021, 130L);
        counts.put(2022, 140L);
        counts.put(2023, 150L);
        return new IngestedYears.Result(2015, 2023, null, counts, 0);
    }

    @Test void interiorGapsAreFoundBetweenTheLoadedBounds() {
        ArrayNode gaps = IngestedYears.gaps(holed());
        assertEquals(3, gaps.size(), "2018, 2019 and 2020 hold no rows");
        assertEquals(2018, gaps.get(0).asInt());
        assertEquals(2019, gaps.get(1).asInt());
        assertEquals(2020, gaps.get(2).asInt());
    }

    @Test void aContiguousTableReportsNoGaps() {
        Map<Integer, Long> counts = new LinkedHashMap<>();
        counts.put(2020, 5L);
        counts.put(2021, 6L);
        counts.put(2022, 7L);
        assertEquals(0,
            IngestedYears.gaps(new IngestedYears.Result(2020, 2022, null, counts, 0)).size());
    }

    @Test void declaredYearsWithNoRowsAreListedSeparatelyFromInteriorGaps() {
        // Declared 2013-2023, loaded 2015-2017 and 2021-2023: the two years before the data
        // starts belong in this list too, and interior_gaps alone would never mention them.
        ArrayNode missing = IngestedYears.missingVersusDeclared(holed(),
            Integer.valueOf(2013), Integer.valueOf(2023));
        assertEquals(5, missing.size());
        assertEquals(2013, missing.get(0).asInt());
        assertEquals(2014, missing.get(1).asInt());
        assertEquals(2018, missing.get(2).asInt());
        assertEquals(2020, missing.get(4).asInt());
    }

    @Test void detailCarriesTheRowCountsTheGapVerdictRestsOn() {
        ObjectNode detail = IngestedYears.detail(holed());
        assertEquals("measured", detail.path("status").asText());
        assertEquals(2015, detail.path("first_year").asInt());
        assertEquals(2023, detail.path("last_year").asInt());
        assertEquals(6, detail.path("years_present").size());
        assertEquals(750, detail.path("rows_total").asLong());
        assertEquals(120, detail.path("rows_by_year").path("2017").asLong());
        assertEquals(3, detail.path("interior_gaps").size());
        assertFalse(detail.has("rows_with_unreadable_year"),
            "nothing unparsed here, so the field must not appear at all");
    }

    @Test void rowsWithAnUnreadableYearAreCountedNotDropped() {
        Map<Integer, Long> counts = new LinkedHashMap<>();
        counts.put(2022, 40L);
        ObjectNode detail = IngestedYears.detail(
            new IngestedYears.Result(2022, 2022, null, counts, 7));
        assertEquals(7, detail.path("rows_with_unreadable_year").asLong());
        assertEquals(40, detail.path("rows_total").asLong(),
            "unreadable rows are reported beside the total, not folded into it");
    }

    @Test void aFailedProbeReportsItsStatusAndClaimsNoCoverage() {
        // The distinction that matters: "we could not measure" must never render as
        // "there is nothing here".
        ObjectNode detail =
            IngestedYears.detail(new IngestedYears.Result(null, null, "unavailable"));
        assertEquals("unavailable", detail.path("status").asText());
        assertFalse(detail.has("years_present"));
        assertFalse(detail.has("rows_total"));
        assertEquals(0,
            IngestedYears.gaps(new IngestedYears.Result(null, null, "unavailable")).size());
        assertEquals(0, IngestedYears.missingVersusDeclared(
            new IngestedYears.Result(null, null, "empty"),
            Integer.valueOf(2015), Integer.valueOf(2020)).size(),
            "an unmeasured table must not report every declared year as missing");
    }

    // ── per_capita ────────────────────────────────────────────────────────────

    @Test void fipsCodesArePaddedToTheWidthTheirGeographyUses() {
        // Population tables key on zero-padded codes; a caller's SQL routinely produces the
        // integer form, and joining "6" against "06" would drop California without a word.
        assertEquals("06", McpServer.normalizeFips("6", "state"));
        assertEquals("06", McpServer.normalizeFips("06", "state"));
        assertEquals("06", McpServer.normalizeFips(" 6 ", "state"));
        assertEquals("06037", McpServer.normalizeFips("6037", "county"));
        assertEquals("06037", McpServer.normalizeFips("06037", "county"));
        assertEquals("01001", McpServer.normalizeFips("1001", "county"));
    }

    @Test void aPlaceNameIsRefusedRatherThanJoinedOnHopefully() {
        // These must come back null so the row is reported unmatched and the caller is sent
        // to resolve_geo — anything else silently subtracts the place from the analysis.
        assertNull(McpServer.normalizeFips("CA", "state"));
        assertNull(McpServer.normalizeFips("California", "state"));
        assertNull(McpServer.normalizeFips("", "state"));
        assertNull(McpServer.normalizeFips(null, "state"));
        assertNull(McpServer.normalizeFips("06037", "state"),
            "a county code at state level is a level mismatch, not a state");
        assertNull(McpServer.normalizeFips("6O37", "county"), "letter O for zero");
    }

    @Test void bothToolsAreAdvertisedWithTheArgumentsTheirHandlersRead() throws Exception {
        // A tool whose advertised name or required arguments drift from what the handler
        // reads is worse than a missing tool: it appears in the client's list and fails only
        // when called. These are the names the tools/call switch matches on.
        java.lang.reflect.Method m = McpServer.class.getDeclaredMethod("handleToolsList",
            com.fasterxml.jackson.databind.JsonNode.class);
        m.setAccessible(true);
        JsonNode listed = ((ObjectNode) m.invoke(null, (JsonNode) null))
            .path("result").path("tools");

        JsonNode coverage = toolNamed(listed, "data_coverage");
        assertEquals("[\"schema\",\"table\"]", coverage.path("inputSchema").path("required")
            .toString());

        JsonNode inflation = toolNamed(listed, "adjust_inflation");
        JsonNode inflationProps = inflation.path("inputSchema").path("properties");
        assertEquals("[\"base_year\"]",
            inflation.path("inputSchema").path("required").toString(),
            "only base_year is always needed; sql and amount are alternative modes");
        for (String arg : new String[]{"sql", "value_col", "year_col", "amount", "from_year",
            "index"}) {
            assertTrue(inflationProps.has(arg), "adjust_inflation must advertise " + arg);
        }

        JsonNode sensitivity = toolNamed(listed, "sensitivity_analysis");
        assertEquals("[\"sql\",\"outcome\",\"predictors\",\"group_col\"]",
            sensitivity.path("inputSchema").path("required").toString());
        assertTrue(sensitivity.path("inputSchema").path("properties").has("term"));

        JsonNode capita = toolNamed(listed, "per_capita");
        assertEquals("[\"sql\",\"value_col\",\"geo_col\",\"year_col\"]",
            capita.path("inputSchema").path("required").toString(),
            "geo_level, per and population_source all have defaults");
        for (String arg : new String[]{"geo_level", "per", "population_source"}) {
            assertTrue(capita.path("inputSchema").path("properties").has(arg),
                "per_capita must advertise " + arg);
        }

        JsonNode event = toolNamed(listed, "event_study");
        assertEquals("[\"sql\",\"outcome\",\"unit_col\",\"time_col\",\"treatment_time_col\"]",
            event.path("inputSchema").path("required").toString());
        for (String arg : new String[]{"max_lead", "max_lag", "reference_period"}) {
            assertTrue(event.path("inputSchema").path("properties").has(arg),
                "event_study must advertise " + arg);
        }
    }

    private static JsonNode toolNamed(JsonNode tools, String name) {
        for (JsonNode t : tools) {
            if (name.equals(t.path("name").asText())) {
                return t;
            }
        }
        throw new AssertionError(name + " is not advertised in tools/list");
    }

    // ── adjust_inflation ──────────────────────────────────────────────────────

    /** A stand-in CPI vintage: five full years plus a partial current one. */
    private static Map<Integer, McpServer.CpiYear> cpi() {
        Map<Integer, McpServer.CpiYear> m = new TreeMap<>();
        m.put(2020, new McpServer.CpiYear(258.0, 12));
        m.put(2021, new McpServer.CpiYear(271.0, 12));
        m.put(2022, new McpServer.CpiYear(292.0, 12));
        m.put(2023, new McpServer.CpiYear(305.0, 12));
        m.put(2024, new McpServer.CpiYear(314.0, 12));
        m.put(2025, new McpServer.CpiYear(320.0, 4));
        return m;
    }

    @Test void theSeriesIsChosenByTheServerNotTheCaller() {
        assertEquals("CUUR0000SA0", McpServer.cpiSeriesFor(null), "cpi_u is the default");
        assertEquals("CUUR0000SA0", McpServer.cpiSeriesFor("cpi_u"));
        assertEquals("CUUR0000SA0L1E", McpServer.cpiSeriesFor("cpi_u_core"));

        // The PPI series lives in the same table and is not a consumer deflator; naming it
        // must fail rather than silently deflate against producer prices.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> McpServer.cpiSeriesFor("WPUFD49207"));
        assertTrue(e.getMessage().contains("cpi_u"), e.getMessage());
    }

    @Test void deflatorIsTheRatioOfTheTwoAnnualAverages() {
        ObjectNode out = McpServer.inflationHeader("cpi_u", "CUUR0000SA0", 2024, cpi());
        McpServer.scalarAdjustment(out, cpi(), 100.0, 2020, 2024);

        assertEquals(314.0 / 258.0, out.path("deflator").asDouble(), EPS);
        assertEquals(100.0 * (314.0 / 258.0), out.path("real_amount").asDouble(), EPS);
        assertEquals(258.0, out.path("from_year_cpi").asDouble(), EPS);
        assertEquals(314.0, out.path("base_year_cpi").asDouble(), EPS);
        assertEquals(100.0, out.path("nominal_amount").asDouble(), EPS);
    }

    @Test void convertingIntoTheSourceYearIsTheIdentity() {
        ObjectNode out = McpServer.inflationHeader("cpi_u", "CUUR0000SA0", 2022, cpi());
        McpServer.scalarAdjustment(out, cpi(), 4200.0, 2022, 2022);
        assertEquals(1.0, out.path("deflator").asDouble(), EPS);
        assertEquals(4200.0, out.path("real_amount").asDouble(), EPS);
    }

    @Test void aYearWithNoLoadedCpiIsRefusedNotInterpolated() {
        // 2019 sits just outside the loaded window. Interpolating from 2020, or applying a
        // remembered inflation rate, would produce a confident wrong number — the whole
        // failure this tool exists to prevent.
        Map<Integer, McpServer.CpiYear> cpi = cpi();
        IllegalArgumentException from = assertThrows(IllegalArgumentException.class,
            () -> McpServer.scalarAdjustment(
                McpServer.inflationHeader("cpi_u", "CUUR0000SA0", 2024, cpi), cpi,
                100.0, 2019, 2024));
        assertTrue(from.getMessage().contains("no CPI loaded for from_year 2019"),
            from.getMessage());

        IllegalArgumentException base = assertThrows(IllegalArgumentException.class,
            () -> McpServer.inflationHeader("cpi_u", "CUUR0000SA0", 2019, cpi));
        assertTrue(base.getMessage().contains("no CPI loaded for base_year 2019"),
            base.getMessage());
        assertTrue(base.getMessage().contains("data_coverage"),
            "the error should point at the tool that explains what IS loaded");
    }

    @Test void headerListsTheLoadedYearsAndNamesThePartialOnes() {
        ObjectNode out = McpServer.inflationHeader("cpi_u", "CUUR0000SA0", 2024, cpi());
        assertEquals(6, out.path("cpi_years_loaded").size());
        assertEquals(2020, out.path("cpi_years_loaded").get(0).asInt());
        assertEquals(12, out.path("base_year_months_averaged").asInt());
        assertFalse(out.has("warning"), "a full-year base needs no warning");

        JsonNode partial = out.path("partial_cpi_years");
        assertEquals(1, partial.size(), "only 2025 is short");
        assertEquals(2025, partial.get(0).path("year").asInt());
        assertEquals(4, partial.get(0).path("months_averaged").asInt());
    }

    @Test void yearsReadAsPartitionStringsAsWellAsIntegers() {
        // Most govdata year columns are hive partitions and arrive as VARCHAR, so the year a
        // caller points value_col at is as likely to be "2021" as 2021.
        assertEquals(Integer.valueOf(2021), McpServer.parseYear("2021"));
        assertEquals(Integer.valueOf(2021), McpServer.parseYear(" 2021 "));
        assertNull(McpServer.parseYear(null));
        assertNull(McpServer.parseYear("2021-03-01"), "a date is not a year");
        assertNull(McpServer.parseYear("Q1"));
        assertNull(McpServer.parseYear("20210"), "out of range rather than silently accepted");
    }

    @Test void aPartialBaseYearIsFlaggedRatherThanTreatedAsAnAnnualAverage() {
        ObjectNode out = McpServer.inflationHeader("cpi_u", "CUUR0000SA0", 2025, cpi());
        String warning = out.path("warning").asText();
        assertTrue(warning.contains("only 4"), warning);
        assertTrue(warning.contains("not an annual") || warning.contains("partial-year"),
            warning);
        assertEquals(4, out.path("base_year_months_averaged").asInt());
    }

    // ── argument reading ──────────────────────────────────────────────────────
    //
    // A missing base_year used to arrive as asInt()==0 and fail three layers later as
    // "no CPI loaded for base_year 0", which names the symptom instead of the omission.
    // Observed live 2026-08-14: an agent spent four calls guessing the parameter name.

    @Test void anAbsentArgumentIsNullRatherThanZero() {
        assertNull(McpServer.optInt(MAPPER.createObjectNode(), "base_year"),
            "a missing year must not read as the year 0");
    }

    @Test void anExplicitNullIsAlsoAbsentRatherThanZero() {
        ObjectNode args = MAPPER.createObjectNode();
        args.putNull("base_year");
        assertNull(McpServer.optInt(args, "base_year"));
    }

    @Test void theSpellingsCallersReachForAreAccepted() {
        ObjectNode target = MAPPER.createObjectNode();
        target.put("target_year", 2024);
        assertEquals(Integer.valueOf(2024), McpServer.optInt(target,
            "base_year", "to_year", "target_year"));

        ObjectNode to = MAPPER.createObjectNode();
        to.put("to_year", 2024);
        assertEquals(Integer.valueOf(2024), McpServer.optInt(to,
            "base_year", "to_year", "target_year"));
    }

    @Test void theCanonicalNameWinsOverAnAlias() {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("base_year", 2024);
        args.put("to_year", 1999);
        assertEquals(Integer.valueOf(2024), McpServer.optInt(args,
            "base_year", "to_year", "target_year"),
            "listed order decides, so the canonical spelling is not shadowed by an alias");
    }

    @Test void aYearSentAsTextIsReadRatherThanRefused() {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("base_year", "2024");
        assertEquals(Integer.valueOf(2024), McpServer.optInt(args, "base_year"));
    }

    @Test void aNonNumericValueIsAbsentRatherThanZero() {
        ObjectNode args = MAPPER.createObjectNode();
        args.put("base_year", "latest");
        assertNull(McpServer.optInt(args, "base_year"),
            "unparseable text must not collapse to 0 — that is the bug this guards");
    }
}
