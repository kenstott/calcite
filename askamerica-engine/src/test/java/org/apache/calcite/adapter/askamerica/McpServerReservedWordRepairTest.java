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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Columns named {@code year}, {@code period} and {@code value} are ordinary in this warehouse
 * and reserved in SQL, so the obvious query fails to parse on a token the caller has no reason
 * to think is special. Observed 2026-08-19: 11 of one session's 42 calls went to rediscovering
 * the quoting rule.
 *
 * <p>The rewrite cannot be driven by the parse error, because the parser does not reliably name
 * the word that broke it: {@code SELECT year} reports {@code Encountered "year ,"}, but
 * {@code WHERE period = 'M05'} reports {@code Incorrect syntax near the keyword 'AND'} — the
 * token *after* the real problem, because PERIOD opens the SQL:2011 period predicate.
 *
 * <p>So the rule is position plus the column list, and the hard half is position. {@code count},
 * {@code order}, {@code desc}, {@code state} and {@code type} are all reserved words that also
 * name columns here, so "is a column name" alone would rewrite {@code COUNT(*)} into
 * {@code "count"(*)} and {@code ORDER BY} into {@code "order" BY} — breaking statements that
 * were nearly right. These tests pin both halves.
 */
@Tag("unit")
public class McpServerReservedWordRepairTest {

    /** The real intersection this warehouse produces: ordinary columns that are also keywords. */
    private static final Set<String> CANDIDATES = new HashSet<>(Arrays.asList(
        "year", "period", "value", "count", "order", "desc", "state", "type"));

    private static String quote(String sql) {
        return McpServer.quoteBareReservedColumns(sql, CANDIDATES, new ArrayList<String>());
    }

    private static List<String> quotedWords(String sql) {
        List<String> out = new ArrayList<>();
        McpServer.quoteBareReservedColumns(sql, CANDIDATES, out);
        return out;
    }

    @Test void recognisesTheWordsThisWarehouseKeepsTrippingOver() {
        assertTrue(McpServer.isReservedWord("year"), "year");
        assertTrue(McpServer.isReservedWord("period"), "period");
        assertTrue(McpServer.isReservedWord("value"), "value");
    }

    @Test void doesNotClaimAnOrdinaryColumnIsReserved() {
        assertFalse(McpServer.isReservedWord("geo_name"));
        assertFalse(McpServer.isReservedWord("median_household_income"));
    }

    @Test void quotesAColumnInEverySelectGroupAndOrderPosition() {
        assertEquals(
            "SELECT \"year\", COUNT(*) AS n FROM census.acs1_income "
                + "GROUP BY \"year\" ORDER BY \"year\"",
            quote("SELECT year, COUNT(*) AS n FROM census.acs1_income "
                + "GROUP BY year ORDER BY year"));
    }

    @Test void leavesCountAloneEvenThoughCountIsAlsoAColumnName() {
        // The failure this guards: quoting on name alone yields "count"(*), which parses worse
        // than the statement it was trying to fix.
        assertTrue(CANDIDATES.contains("count"), "precondition: count is a column name here");
        assertTrue(quote("SELECT COUNT(*) FROM t").contains("COUNT(*)"));
    }

    @Test void leavesOrderByIntactEvenThoughOrderIsAlsoAColumnName() {
        assertTrue(CANDIDATES.contains("order"), "precondition: order is a column name here");
        assertEquals("SELECT geo_name FROM t ORDER BY geo_name DESC",
            quote("SELECT geo_name FROM t ORDER BY geo_name DESC"));
    }

    @Test void leavesOverOrderByIntactEvenThoughOrderIsAlsoAColumnName() {
        // D-016: the token right after OVER( opens a window-frame clause (PARTITION BY /
        // ORDER BY / ROWS / RANGE) and is always a keyword there, never a column reference --
        // but "(" alone is a legitimate identifier-position trigger everywhere else (e.g.
        // foo(order)), so this needs one more token of lookback than the plain ORDER BY case
        // above. Was producing RANK() OVER ("order" BY x), a parse failure, whenever the query
        // also referenced another reserved-word column that put a candidate in scope.
        assertTrue(CANDIDATES.contains("order"), "precondition: order is a column name here");
        assertEquals(
            "SELECT \"year\", RANK() OVER (ORDER BY indemnity_amount DESC) AS rnk "
                + "FROM ag.rma_crop_insurance",
            quote("SELECT year, RANK() OVER (ORDER BY indemnity_amount DESC) AS rnk "
                + "FROM ag.rma_crop_insurance"));
    }

    @Test void quotesAColumnUsedInAWherePredicate() {
        assertEquals(
            "SELECT \"year\", \"value\" FROM econ.inflation_metrics "
                + "WHERE series = 'CUUR0000SA0' AND \"period\" = 'M05' AND \"year\" = 2026",
            quote("SELECT year, value FROM econ.inflation_metrics "
                + "WHERE series = 'CUUR0000SA0' AND period = 'M05' AND year = 2026"));
    }

    @Test void quotesOnlyTheColumnPartOfAQualifiedReference() {
        assertEquals("SELECT a.\"year\" FROM t a", quote("SELECT a.year FROM t a"));
    }

    @Test void leavesALongerNameThatMerelyStartsWithTheWordAlone() {
        assertEquals("SELECT year_col, \"year\" FROM t", quote("SELECT year_col, year FROM t"));
    }

    @Test void leavesAStringLiteralAlone() {
        assertEquals("SELECT \"value\" FROM t WHERE series = 'value'",
            quote("SELECT value FROM t WHERE series = 'value'"));
    }

    @Test void leavesAnAlreadyQuotedIdentifierAlone() {
        assertEquals("SELECT \"year\" AS yr, \"period\" FROM t",
            quote("SELECT \"year\" AS yr, period FROM t"));
    }

    @Test void foldsToLowercaseBecauseThatIsWhatTheBareSpellingMeant() {
        assertEquals("SELECT \"year\" FROM t", quote("SELECT YEAR FROM t"));
    }

    @Test void isAnIdentityRewriteWhenNoCandidateAppears() {
        String sql = "SELECT geo_name FROM census.acs1_income WHERE state = 'CA'";
        assertEquals("SELECT geo_name FROM census.acs1_income WHERE \"state\" = 'CA'",
            quote(sql));
    }

    @Test void reportsExactlyTheWordsItRewrote() {
        assertEquals(Arrays.asList("year", "value", "period"),
            quotedWords("SELECT year, value FROM t WHERE period = 'M05' AND year = 2026"));
    }

    @Test void reportsNothingWhenItChangedNothing() {
        assertTrue(quotedWords("SELECT geo_name FROM t ORDER BY geo_name").isEmpty());
    }

    @Test void everySqlTakingToolReachesTheDatabaseThroughOneRepairedPath() throws Exception {
        // The gap this pins: the repair was first wired into the query tool alone, so the same
        // CAST(year AS INTEGER) that query accepted still failed under adjust_inflation. An
        // eval run met the identical error one tool over and paid for it a second time.
        // Guarding the call sites rather than the behaviour, because the behaviour needs a
        // live catalog and the regression is structural — a new tool calling executeQuery
        // directly is exactly how this comes back.
        String src = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(
            "src/main/java/org/apache/calcite/adapter/askamerica/McpServer.java")),
            java.nio.charset.StandardCharsets.UTF_8);

        assertFalse(src.contains("st.executeQuery(normalizeCallerSql(sql))"),
            "caller SQL must go through executeWithRepair, not straight to executeQuery");
        assertTrue(src.contains("static ResultSet executeWithRepair(Statement st, String sql)"),
            "the shared execution boundary must exist");

        int direct = src.split("stmt\\.executeQuery\\(effective\\)", -1).length - 1;
        assertEquals(0, direct,
            "runSqlRows must not bypass the repair either");
    }

    @Test void survivesAnUnterminatedLiteralWithoutLosingText() {
        String out = quote("SELECT value FROM t WHERE s = 'oops");
        assertTrue(out.contains("'oops"), "the tail must not be dropped: " + out);
    }

    /**
     * The candidate set above is built from real catalog COLUMN names (see
     * {@code reservedColumnWords()} in McpServer, which intersects the SQL parser's own
     * reserved-word list against {@code queryableColumnNames()}). A reserved word used only as
     * a computed ALIAS -- {@code AVG(x) AS trailing}, say -- is never a catalog column, so no
     * static list built that way, however long, could ever have caught it. Falling through to
     * {@link McpServer#isReservedWord(String)} directly (rather than gating on the catalog-
     * derived {@code candidates} set alone) catches this whole class, not just this one word.
     */
    @Test void quotesAReservedWordAliasThatIsNeverACatalogColumn() {
        assertTrue(McpServer.isReservedWord("trailing"), "trailing");
        assertFalse(CANDIDATES.contains("trailing"),
            "precondition: trailing is not a catalog column in this test's candidate set");
        assertEquals("SELECT AVG(x) AS \"trailing\" FROM t",
            quote("SELECT AVG(x) AS trailing FROM t"));
    }

    // ── LIMIT + FETCH FIRST conflict ─────────────────────────────────────────

    @Test void stripsLimitWhenFetchFirstIsAlsoPresent() {
        // Observed live: a caller defaulted to Postgres-style LIMIT out of habit while also
        // following this tool's own "Add FETCH FIRST N ROWS ONLY" guidance, producing exactly
        // this combination -- a certain parse failure in every dialect this server serves.
        assertEquals("SELECT * FROM t FETCH FIRST 500 ROWS ONLY",
            McpServer.stripRedundantLimitClause(
                "SELECT * FROM t LIMIT 5 FETCH FIRST 500 ROWS ONLY"));
    }

    @Test void stripsLimitRegardlessOfClauseOrder() {
        assertEquals("SELECT * FROM t FETCH FIRST 500 ROWS ONLY ",
            McpServer.stripRedundantLimitClause(
                "SELECT * FROM t FETCH FIRST 500 ROWS ONLY LIMIT 5"));
    }

    @Test void leavesSqlAloneWhenOnlyLimitIsPresent() {
        String sql = "SELECT * FROM t LIMIT 5";
        assertEquals(sql, McpServer.stripRedundantLimitClause(sql));
    }

    @Test void leavesSqlAloneWhenOnlyFetchFirstIsPresent() {
        String sql = "SELECT * FROM t FETCH FIRST 500 ROWS ONLY";
        assertEquals(sql, McpServer.stripRedundantLimitClause(sql));
    }

    @Test void leavesSqlAloneWhenNeitherIsPresent() {
        String sql = "SELECT * FROM t";
        assertEquals(sql, McpServer.stripRedundantLimitClause(sql));
    }
}
