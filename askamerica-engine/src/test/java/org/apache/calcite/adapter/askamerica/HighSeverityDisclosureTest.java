package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regression test for the fix to {@code enforceHighSeverityDisclosure}: measured live
 * (2026-09-21, session dc65f4962d2e) rejecting the same report 9 times in a row because the
 * gate required the raw SQL column identifier (e.g. "PRODN_PRACTICE_DESC") verbatim in the
 * report prose. The fix accepts a humanized alias and any rollup_values data labels instead.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class HighSeverityDisclosureTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @AfterEach void clearCallLog() throws Exception {
        callLog().clear();
    }

    @SuppressWarnings("unchecked")
    private static List<ObjectNode> callLog() throws Exception {
        Field f = McpServer.class.getDeclaredField("CALL_LOG");
        f.setAccessible(true);
        return (List<ObjectNode>) f.get(null);
    }

    private static void recordRollupContaminationQuery() throws Exception {
        Method m = McpServer.class.getDeclaredMethod("recordCall", String.class, JsonNode.class,
            long.class, int.class, ObjectNode.class, String.class);
        m.setAccessible(true);

        ObjectNode args = MAPPER.createObjectNode();
        args.put("sql", "SELECT year, SUM(value) FROM ag.nass_crop_production GROUP BY year");

        ObjectNode warning = MAPPER.createObjectNode();
        warning.put("type", "rollup_contamination");
        warning.put("severity", "high");
        warning.put("note", "Column PRODN_PRACTICE_DESC holds pre-aggregated rollup rows.");
        warning.put("column", "PRODN_PRACTICE_DESC");
        warning.putArray("rollup_values").add("ALL PRODUCTION PRACTICES");

        ObjectNode diagnostics = MAPPER.createObjectNode();
        ObjectNode inner = diagnostics.putObject("diagnostics");
        inner.putArray("warnings").add(warning);

        m.invoke(null, "query", args, 1000L, 32, diagnostics, null);
    }

    private static void recordLowCoverageQuery() throws Exception {
        Method m = McpServer.class.getDeclaredMethod("recordCall", String.class, JsonNode.class,
            long.class, int.class, ObjectNode.class, String.class);
        m.setAccessible(true);

        ObjectNode args = MAPPER.createObjectNode();
        args.put("sql", "SELECT * FROM energy.eia_electricity_generation "
            + "WHERE generation_year = 2023");

        ObjectNode warning = MAPPER.createObjectNode();
        warning.put("type", "low_coverage");
        warning.put("severity", "high");
        warning.put("note", "energy.eia_electricity_generation returned no rows for 2023.");
        warning.put("table", "energy.eia_electricity_generation");
        warning.put("year", 2023);

        ObjectNode diagnostics = MAPPER.createObjectNode();
        ObjectNode inner = diagnostics.putObject("diagnostics");
        inner.putArray("warnings").add(warning);

        m.invoke(null, "query", args, 1000L, 0, diagnostics, null);
    }

    private static Object section(String heading, String html) throws Exception {
        Class<?> secClass = Class.forName(
            "org.apache.calcite.adapter.askamerica.ReportPage$Section");
        Constructor<?> ctor = secClass.getDeclaredConstructor(String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(heading, html);
    }

    private static String enforce(List<Object> sections) throws Exception {
        Method m = McpServer.class.getDeclaredMethod("enforceHighSeverityDisclosure", List.class);
        m.setAccessible(true);
        return (String) m.invoke(null, sections);
    }

    @Test void rawColumnIdentifierVerbatimStillSatisfies() throws Exception {
        recordRollupContaminationQuery();
        List<Object> secs = List.of(section("Wheat acreage",
            "PRODN_PRACTICE_DESC rollups are a caveat here: totals may double-count."));
        assertNull(enforce(secs));
    }

    @Test void humanizedAliasSatisfiesWithoutRawIdentifier() throws Exception {
        recordRollupContaminationQuery();
        List<Object> secs = List.of(section("Wheat acreage",
            "Caution: the prodn practice breakdown includes rollup rows that may "
            + "double-count some totals."));
        assertNull(enforce(secs));
    }

    @Test void rollupValueLabelAloneSatisfies() throws Exception {
        recordRollupContaminationQuery();
        List<Object> secs = List.of(section("Wheat acreage",
            "Caveat: the ALL PRODUCTION PRACTICES total overlaps with its component rows."));
        assertNull(enforce(secs));
    }

    @Test void noDisclosureAtAllStillRejected() throws Exception {
        recordRollupContaminationQuery();
        List<Object> secs = List.of(section("Wheat acreage",
            "Wheat acreage rose steadily from 2010 to 2020."));
        assertNotNull(enforce(secs));
    }

    /** Regression for the CAVEAT_WORDS gap measured live 2026-09-22 (q12): a low_coverage
     *  caveat phrased as "returned zero rows" / "no figure ... is drawn from" -- ordinary,
     *  clear disclosure language that named none of the original word list's specific terms
     *  ("excluded", "unreliable", "caveat", "coverage gap", etc.) -- rejected the same report
     *  11 times in a row. */
    @Test void plainDisclosureOfAnEmptyQueryResultSatisfies() throws Exception {
        recordLowCoverageQuery();
        List<Object> secs = List.of(section("Generation mix",
            "energy.eia_electricity_generation was queried for 2023 and returned zero rows "
            + "both times; no figure in this report is drawn from that table."));
        assertNull(enforce(secs));
    }

    @Test void lowCoverageWithNoDisclosureStillRejected() throws Exception {
        recordLowCoverageQuery();
        List<Object> secs = List.of(section("Generation mix",
            "Coal produced about 17% of US electricity in 2025."));
        assertNotNull(enforce(secs));
    }
}
