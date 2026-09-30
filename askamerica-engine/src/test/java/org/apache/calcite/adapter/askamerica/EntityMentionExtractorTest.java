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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class EntityMentionExtractorTest {

    private static Set<String> norms(String text) {
        return EntityMentionExtractor.distinctNorms(EntityMentionExtractor.candidates(text));
    }

    @Test
    void findsMultiWordNamesWithConnectorsAndLegalSuffixes() {
        Set<String> n = norms("Shares of Bank of America Corp. and Procter & Gamble rose "
            + "while Johnson & Johnson fell.");
        assertTrue(n.contains("bank of america"), n.toString());
        assertTrue(n.contains("procter gamble"), n.toString());
        assertTrue(n.contains("johnson johnson"), n.toString());
    }

    @Test
    void skipsSentenceStartersMonthsAndPronouns() {
        Set<String> n = norms("The company said on Monday that it would pay in March.");
        assertTrue(n.isEmpty(), n.toString());
    }

    @Test
    void windowSearchFindsANameInsideAFullyCapitalizedHeadline() {
        Set<String> n = norms("Apple Inc Beats Estimates As Services Revenue Climbs");
        assertTrue(n.contains("apple"), n.toString());
        assertTrue(n.contains("services revenue"), n.toString());
    }

    @Test
    void longestNonOverlappingMatchWins() {
        String text = "Bank of America Merrill Lynch cut its forecast.";
        List<EntityMentionExtractor.Candidate> all = EntityMentionExtractor.candidates(text);
        Set<String> registry = new HashSet<>(Arrays.asList("bank of america merrill lynch",
            "bank of america", "merrill lynch", "america"));
        List<EntityMentionExtractor.Candidate> hits =
            EntityMentionExtractor.resolve(all, registry);
        assertEquals(1, hits.size());
        assertEquals("bank of america merrill lynch", hits.get(0).norm);
    }

    @Test
    void separateMentionsOfTheSameNameAreEachKept() {
        String text = "Tesla rose. Later, Tesla fell.";
        List<EntityMentionExtractor.Candidate> hits = EntityMentionExtractor.resolve(
            EntityMentionExtractor.candidates(text), new HashSet<>(Arrays.asList("tesla")));
        assertEquals(2, hits.size());
        assertEquals(1, EntityMentionExtractor.groupByNorm(hits).size());
        assertEquals(text.indexOf("Tesla"), hits.get(0).start);
        assertEquals(text.lastIndexOf("Tesla"), hits.get(1).start);
    }

    @Test
    void spanOffsetsPointAtTheSurfaceText() {
        String text = "Reuters reported that Goldman Sachs Group Inc. posted a gain.";
        for (EntityMentionExtractor.Candidate c : EntityMentionExtractor.candidates(text)) {
            assertEquals(c.surface, text.substring(c.start, c.end));
        }
    }

    @Test
    void tickersOnlyFromExplicitNewsPatterns() {
        Set<String> t = EntityMentionExtractor.tickers("Apple (NASDAQ: AAPL) and NYSE: IBM, "
            + "plus $TSLA; but NASA and CEO are not tickers, nor is a lone MSFT.");
        assertEquals(new HashSet<>(Arrays.asList("AAPL", "IBM", "TSLA")), t);
    }

    @Test
    void candidateExplosionIsRejectedNotTruncated() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 2500; i++) {
            b.append("Alpha").append(i).append("x Beta").append(i).append("y. ");
        }
        assertThrows(IllegalArgumentException.class,
            () -> EntityMentionExtractor.candidates(b.toString()));
    }

    @Test
    void sentenceSpansCoverTheText() {
        String text = "Acme rose. Beta fell! Gamma held.";
        List<int[]> spans = EntityMentionExtractor.sentenceSpans(text);
        assertEquals(3, spans.size());
        assertEquals("Acme rose.", text.substring(spans.get(0)[0], spans.get(0)[1]));
        assertEquals("Gamma held.", text.substring(spans.get(2)[0], spans.get(2)[1]));
    }

    @Test
    void bridgeSqlIsAnEqualityProbeWithQuotingAndNoScanPredicates() {
        String sql = McpServer.buildExtractEntitiesSql(Arrays.asList("apple", "o'reilly auto"));
        assertTrue(sql.contains("source_name_normalized IN ('apple', 'o''reilly auto')"), sql);
        assertFalse(sql.toUpperCase().contains(" LIKE "), "a LIKE is a full scan here");
        assertFalse(sql.toUpperCase().contains("JARO"), "fuzzy scoring is a full scan here");
        assertTrue(sql.contains("ref.entity_org_bridge"), sql);
        assertFalse(sql.contains("canonical_org_entity") || sql.contains("gleif_entities"),
            "joining the 10M-row canonical table here scans all of it (measured >70s)");
        assertThrows(IllegalArgumentException.class,
            () -> McpServer.buildExtractEntitiesSql(Arrays.<String>asList()));
    }

    @Test
    void canonicalSqlUsesALiteralKeyListNeverASubquery() {
        String sql = McpServer.buildCanonicalOrgSql(Arrays.asList("K1", "K2"),
            new HashSet<>(Arrays.asList("fec_committee_id", "lobbying_client_id")));
        assertTrue(sql.contains("canonical_entity_id IN ('K1', 'K2')"), sql);
        assertFalse(sql.toUpperCase().contains("(SELECT"), "an IN-subquery scans the table");
        assertTrue(sql.contains("GROUP BY canonical_entity_id"), sql);
    }

    @Test
    void canonicalSqlSelectsOnlyIdentifierColumnsTheDeployedTableHas() {
        String sql = McpServer.buildCanonicalOrgSql(Arrays.asList("K1"),
            new HashSet<>(Arrays.asList("fec_committee_id")));
        assertTrue(sql.contains("MAX(fec_committee_id) AS fec_committee_id"), sql);
        assertFalse(sql.contains("lobbying_client_id"),
            "a declared-but-undeployed column would fail the whole statement");
        String people = McpServer.buildExtractPersonsSql(Arrays.asList("nancy pelosi"),
            new HashSet<>(Arrays.asList("officials_judge_jid")));
        assertTrue(people.contains("officials_judge_jid"), people);
        assertFalse(people.contains("officials_member_bioguide_id"), people);
    }

    @Test
    void gleifSqlIsALiteralLeiList() {
        String sql = McpServer.buildGleifSql(Arrays.asList("ABC123"));
        assertTrue(sql.contains("lei IN ('ABC123')"), sql);
        assertTrue(sql.contains("ref.gleif_entities"), sql);
    }

    private static EntityMentionExtractor.Candidate byName(String text, String surface) {
        for (EntityMentionExtractor.Candidate c : EntityMentionExtractor.candidates(text)) {
            if (c.surface.equals(surface)) {
                return c;
            }
        }
        throw new AssertionError("no candidate '" + surface + "' in " + text);
    }

    @Test
    void personKeyDropsMiddleNamesInitialsAndSuffixes() {
        String text = "Rep. Nancy P. Pelosi met Martin Luther King Jr. and Kevin McCarthy.";
        assertEquals("nancy pelosi", byName(text, "Nancy P. Pelosi").personKey);
        assertEquals("martin king", byName(text, "Martin Luther King Jr").personKey);
        assertEquals("kevin mccarthy", byName(text, "Kevin McCarthy").personKey);
        // A single word is never a personal name key.
        assertEquals(null, byName(text, "Pelosi").personKey);
    }

    @Test
    void personKeyRejectsCommonWordsAndNonNames() {
        for (EntityMentionExtractor.Candidate c : EntityMentionExtractor.candidates(
                "On Monday The Company said Shares fell in March.")) {
            assertEquals(null, c.personKey, c.surface);
        }
    }

    @Test
    void registryNamesParseInEitherOrder() {
        assertEquals("nancy pelosi", McpServer.personKeyOfCanonicalName("Nancy Pelosi"));
        assertEquals("nancy pelosi", McpServer.personKeyOfCanonicalName("PELOSI, NANCY"));
        assertEquals("nancy pelosi", McpServer.personKeyOfCanonicalName("Pelosi, Nancy P."));
        assertEquals("kevin mccarthy", McpServer.personKeyOfCanonicalName("McCarthy, Kevin"));
    }

    @Test
    void bareSurnameJoinsTheOnePersonNamedInFull() {
        String text = "Nancy Pelosi spoke first. Later, Pelosi said the bill would pass. "
            + "Kevin McCarthy disagreed, and McCarthy left.";
        List<EntityMentionExtractor.Candidate> all = EntityMentionExtractor.candidates(text);
        Set<String> people = new HashSet<>(Arrays.asList("nancy pelosi", "kevin mccarthy"));
        List<EntityMentionExtractor.Candidate> accepted = EntityMentionExtractor.resolve(all,
            c -> c.personKey != null && people.contains(c.personKey));
        java.util.Map<String, String> lastToGroup = new java.util.HashMap<>();
        lastToGroup.put("pelosi", "p:nancy pelosi");
        lastToGroup.put("mccarthy", "p:kevin mccarthy");
        java.util.Map<String, List<EntityMentionExtractor.Candidate>> sm =
            EntityMentionExtractor.surnameMentions(all, accepted, lastToGroup);
        assertEquals(1, sm.get("p:nancy pelosi").size());
        assertEquals(1, sm.get("p:kevin mccarthy").size());
        assertEquals(text.indexOf("Pelosi said"), sm.get("p:nancy pelosi").get(0).start);
    }

    @Test
    void surnameInsideAnAcceptedFullNameIsNotCountedTwice() {
        String text = "Nancy Pelosi spoke.";
        List<EntityMentionExtractor.Candidate> all = EntityMentionExtractor.candidates(text);
        List<EntityMentionExtractor.Candidate> accepted = EntityMentionExtractor.resolve(all,
            c -> "nancy pelosi".equals(c.personKey));
        java.util.Map<String, String> lastToGroup = new java.util.HashMap<>();
        lastToGroup.put("pelosi", "p:nancy pelosi");
        assertTrue(EntityMentionExtractor.surnameMentions(all, accepted, lastToGroup)
            .isEmpty());
    }

    @Test
    void personSqlProbesBothNameOrdersByEquality() {
        String sql = McpServer.buildExtractPersonsSql(Arrays.asList("nancy pelosi"),
            new HashSet<>(Arrays.asList("officials_member_bioguide_id")));
        assertTrue(sql.contains("lower(canonical_name) IN ('nancy pelosi', 'pelosi, nancy')"),
            sql);
        assertFalse(sql.toUpperCase().contains(" LIKE "), sql);
        assertTrue(sql.contains("officials_member_bioguide_id"), sql);
        assertTrue(sql.contains("ref.canonical_person_entity"), sql);
    }

    @Test
    void geoSqlProbesStateNamesAndFullCountyNamesOnly() {
        String states = McpServer.buildExtractStatesSql(Arrays.asList("north carolina"));
        assertTrue(states.contains("lower(state_name) IN ('north carolina')"), states);
        String counties = McpServer.buildExtractCountiesSql(
            Arrays.asList("mecklenburg county"));
        assertTrue(counties.contains("lower(county_code) IN ('mecklenburg county')"), counties);
        assertFalse(counties.contains("county_name IN"),
            "bare county names would turn every 'Orange' into a county");
    }

    @Test
    void bareSurnameGoesToTheNamedPersonEvenWhenItAlsoMatchesAnOrganisation() {
        String text = "Mitch McConnell said no. Later, McConnell left.";
        List<EntityMentionExtractor.Candidate> all = EntityMentionExtractor.candidates(text);
        // The registry has the person AND an organisation literally named "McConnell".
        List<EntityMentionExtractor.Candidate> accepted = EntityMentionExtractor.resolve(all,
            c -> "mitch mcconnell".equals(c.personKey) || "mcconnell".equals(c.norm));
        EntityMentionExtractor.Surnames sn = EntityMentionExtractor.applySurnames(all,
            accepted, new HashSet<>(Arrays.asList("mitch mcconnell")));
        for (EntityMentionExtractor.Candidate c : sn.kept) {
            assertFalse("mcconnell".equals(c.norm),
                "the bare surname must not stay behind as an organisation match");
        }
        assertEquals(1, sn.mentions.get("p:mitch mcconnell").size());
        assertEquals(text.lastIndexOf("McConnell"),
            sn.mentions.get("p:mitch mcconnell").get(0).start);
    }

    @Test
    void sharedSurnameStaysWithWhateverItMatchedBefore() {
        String text = "Mitch McConnell met Mary McConnell. Later, McConnell left.";
        List<EntityMentionExtractor.Candidate> all = EntityMentionExtractor.candidates(text);
        List<EntityMentionExtractor.Candidate> accepted = EntityMentionExtractor.resolve(all,
            c -> "mitch mcconnell".equals(c.personKey) || "mary mcconnell".equals(c.personKey)
                || "mcconnell".equals(c.norm));
        EntityMentionExtractor.Surnames sn = EntityMentionExtractor.applySurnames(all,
            accepted, new HashSet<>(Arrays.asList("mitch mcconnell", "mary mcconnell")));
        assertTrue(sn.mentions.isEmpty(), "two McConnells named: the bare surname is ambiguous");
        boolean bareKept = false;
        for (EntityMentionExtractor.Candidate c : sn.kept) {
            bareKept |= "mcconnell".equals(c.norm);
        }
        assertTrue(bareKept, "an unattributable surname keeps its own match");
    }
}
