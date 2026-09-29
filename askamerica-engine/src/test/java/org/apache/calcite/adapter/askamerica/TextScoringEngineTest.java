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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TextScoringEngineTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Finance domain: most of these headlines use finance vocabulary. */
    private static TextScoringEngine.Sentiment sent(String t) {
        return TextScoringEngine.sentiment(t, "finance");
    }

    @Test
    void clearlyPositiveAndNegativeHeadlines() {
        TextScoringEngine.Sentiment up = sent("Shares surge after the company beat estimates "
            + "and raised guidance");
        assertEquals("positive", up.label);
        assertTrue(up.score > 0.8, "score " + up.score);
        TextScoringEngine.Sentiment down = sent("Stock plunges as firm cuts guidance amid "
            + "fraud investigation");
        assertEquals("negative", down.label);
        assertTrue(down.score < -0.8, "score " + down.score);
    }

    @Test
    void inflectionsMatchTheBaseForm() {
        assertEquals("positive", sent("The shares surged").label);
        assertEquals("positive", sent("Profits are surging").label);
        assertEquals("negative", sent("Sales declined").label);
        assertEquals("negative", sent("Revenue is declining").label);
    }

    @Test
    void negationFlipsPolarityAtReducedWeight() {
        TextScoringEngine.Sentiment plain = sent("results were strong");
        TextScoringEngine.Sentiment negated = sent("results were not strong");
        assertEquals("positive", plain.label);
        assertEquals("negative", negated.label);
        assertEquals(1, negated.negatedTerms);
        assertEquals(0.5, negated.negativeMass, 1e-9);
        assertTrue(negated.matched.containsKey("not strong"));
    }

    @Test
    void negationDoesNotCrossASentenceBoundary() {
        TextScoringEngine.Sentiment s = sent("Demand is not the issue. Growth was strong.");
        assertEquals(0, s.negatedTerms);
        assertEquals("positive", s.label);
    }

    @Test
    void intensifiersAndDampenersScale() {
        double strong = sent("shares fell sharply").negativeMass;
        double plain = sent("shares fell").negativeMass;
        double mild = sent("shares fell slightly").negativeMass;
        assertTrue(strong > plain && plain > mild, strong + " " + plain + " " + mild);
    }

    @Test
    void phraseBeatsItsComponentWords() {
        TextScoringEngine.Sentiment s = sent("The firm beat expectations");
        assertEquals(2.0, s.positiveMass, 1e-9);
        assertEquals(1, s.positiveTerms);
    }

    @Test
    void noLexiconTermsIsNoSignalNotNeutral() {
        TextScoringEngine.Sentiment s = sent("The annual meeting is held on Tuesday");
        assertNull(s.score);
        assertEquals("no_signal", s.label);
        assertEquals(0.0, s.confidence, 0);
    }

    @Test
    void mixedTextIsNeutralAndConfidenceGrowsWithEvidence() {
        TextScoringEngine.Sentiment s = sent("Shares rose but the outlook weakened");
        assertEquals("neutral", s.label);
        TextScoringEngine.Sentiment little = sent("a gain");
        TextScoringEngine.Sentiment lots = sent("gain rally surge jump climb rebound strong");
        assertTrue(lots.confidence > little.confidence);
    }

    @Test
    void relevanceRanksOnTopicAboveOffTopic() {
        String on = "Apple stock rose after Apple reported record iPhone sales.";
        String off = "The weather in Ohio was mild and the corn harvest is under way.";
        TextScoringEngine.Relevance a = TextScoringEngine.relevance(on, "Apple stock",
            Collections.<String>emptyList());
        TextScoringEngine.Relevance b = TextScoringEngine.relevance(off, "Apple stock",
            Collections.<String>emptyList());
        assertTrue(a.score > 0.9, "on-topic " + a.score);
        assertEquals(0.0, b.score, 1e-9);
        assertTrue(a.exactPhrase);
        assertTrue(b.missingTerms.contains("apple"));
    }

    @Test
    void aliasCountsAsAMatchAndLeadPositionMatters() {
        TextScoringEngine.Relevance viaAlias = TextScoringEngine.relevance(
            "AAPL climbed in early trading on strong demand", "Apple",
            Arrays.asList("AAPL"));
        assertTrue(viaAlias.aliasHit);
        assertTrue(viaAlias.score >= 0.8);
        String filler = "market wide commentary about many unrelated things and more ";
        TextScoringEngine.Relevance early = TextScoringEngine.relevance(
            "Tesla " + filler + filler + filler, "Tesla", Collections.<String>emptyList());
        TextScoringEngine.Relevance late = TextScoringEngine.relevance(
            filler + filler + filler + "Tesla", "Tesla", Collections.<String>emptyList());
        assertTrue(early.leadHit);
        assertFalse(late.leadHit);
        assertTrue(early.score > late.score);
    }

    @Test
    void queryOfOnlyStopwordsIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> TextScoringEngine.queryTerms(
            "the of and"));
    }

    @Test
    void scoreTextWeightsSentimentByRelevance() throws Exception {
        JsonNode texts = MAPPER.readTree("["
            + "{\"id\":\"a\",\"title\":\"Acme surges\",\"text\":\"Acme shares surge on "
            + "strong earnings.\"},"
            + "{\"id\":\"b\",\"text\":\"Unrelated retailer collapses, fraud probe widens.\"}"
            + "]");
        ObjectNode out = TextScoringEngine.scoreText(TextScoringEngine.parseItems(texts),
            "Acme", Collections.<String>emptyList());
        JsonNode agg = out.get("aggregate");
        // The negative text is irrelevant to Acme, so it must not drag the aggregate.
        assertTrue(agg.get("relevance_weighted_sentiment").asDouble() > 0.9,
            agg.toString());
        assertEquals("a", out.get("results").get(0).get("id").asText());
    }

    @Test
    void scoreTextReportsNoWeightedSentimentWhenNothingIsRelevant() throws Exception {
        JsonNode texts = MAPPER.readTree("[\"Retailer collapses amid fraud probe.\"]");
        ObjectNode out = TextScoringEngine.scoreText(TextScoringEngine.parseItems(texts),
            "Acme", Collections.<String>emptyList());
        assertTrue(out.get("aggregate").get("relevance_weighted_sentiment").isNull());
        assertNotNull(out.get("aggregate").get("note"));
    }

    @Test
    void inputValidation() throws Exception {
        assertThrows(IllegalArgumentException.class,
            () -> TextScoringEngine.parseItems(MAPPER.readTree("[]")));
        assertThrows(IllegalArgumentException.class,
            () -> TextScoringEngine.parseItems(MAPPER.readTree("[\"  \"]")));
        assertThrows(IllegalArgumentException.class,
            () -> TextScoringEngine.parseItems(MAPPER.readTree("[{\"id\":\"x\"}]")));
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < TextScoringEngine.MAX_TEXT_CHARS / 4 + 1; i++) {
            big.append("gain ");
        }
        String tooLong = big.toString();
        assertThrows(IllegalArgumentException.class, () -> TextScoringEngine.parseItems(
            MAPPER.createArrayNode().add(tooLong)));
    }

    @Test
    void generalIsTheDefaultAndCarriesNoFinanceVocabulary() {
        assertEquals("general", TextScoringEngine.DEFAULT_DOMAIN);
        assertEquals("no_signal", TextScoringEngine.sentiment(
            "The firm cut its guidance").label);
        assertEquals("negative", TextScoringEngine.sentiment(
            "The firm cut its guidance", "finance").label);
        assertEquals("positive", TextScoringEngine.sentiment("Strong growth and a "
            + "successful launch").label);
    }

    @Test
    void domainOverlaysChangePolarity() {
        assertEquals("negative", sent("the firm cut its forecast").label);
        TextScoringEngine.Sentiment h = TextScoringEngine.sentiment(
            "The patient tested positive for infection", "health");
        assertEquals("negative", h.label);
        assertEquals("positive", TextScoringEngine.sentiment(
            "Scan shows the tumor in remission", "health").label);
        // A bare "positive" is a valenced word in general, but not in health.
        assertEquals("positive", TextScoringEngine.sentiment("a positive result").label);
        assertEquals("no_signal", TextScoringEngine.sentiment("a positive result",
            "health").label);
        assertEquals("no_signal", TextScoringEngine.sentiment("The bill will cut taxes",
            "politics").label);
        assertEquals("negative", TextScoringEngine.sentiment("Senator indicted in scandal",
            "politics").label);
        assertEquals("negative", TextScoringEngine.sentiment("Plant recall over a defect",
            "manufacturing").label);
        assertEquals("positive", TextScoringEngine.sentiment("Throughput and uptime improved",
            "manufacturing").label);
    }

    @Test
    void unknownDomainIsRejectedNotDefaulted() {
        assertThrows(IllegalArgumentException.class,
            () -> TextScoringEngine.sentiment("gain", "astrology"));
    }

    private static String label(String text, String domain) {
        return TextScoringEngine.sentiment(text, domain).label;
    }

    @Test
    void everyListedDomainLoadsAndScores() {
        for (String d : TextScoringEngine.DOMAINS) {
            assertEquals("positive", label("Strong growth and a successful launch", d) ,
                d + " must keep universal terms not overridden by its overlay");
        }
    }

    @Test
    void newDomainOverlaysApplyTheirVocabulary() {
        assertEquals("negative", label("Grid blackout after a pipeline rupture", "energy"));
        assertEquals("positive", label("A bumper crop and record harvest", "agriculture"));
        assertEquals("negative", label("Drought and blight cause crop failure", "agriculture"));
        assertEquals("negative", label("Oil spill and deforestation", "environment"));
        assertEquals("positive", label("Reforestation and ecosystem recovery", "environment"));
        // Direction words are neutral in this domain: "rise" alone carries no polarity.
        assertEquals("no_signal", label("Sales will rise", "environment"));
        assertEquals("positive", label("The defendant was acquitted", "legal"));
        assertEquals("negative", label("The executive was convicted of fraud", "legal"));
        assertEquals("negative", label("Foreclosures and evictions climb", "housing"));
        assertEquals("positive", label("New construction and housing starts", "housing"));
        assertEquals("positive", label("Strong hiring and job creation", "labor"));
        assertEquals("negative", label("Layoffs and a strike", "labor"));
        assertEquals("negative", label("Shooting leaves two dead", "public_safety"));
        assertEquals("positive", label("Crews rescued the family", "public_safety"));
        assertEquals("negative", label("Ransomware breach and outage", "technology"));
        assertEquals("positive", label("Funding round and product launch", "technology"));
    }

    @Test
    void domainSpecificTermsDoNotLeakIntoOtherDomains() {
        assertEquals("no_signal", label("Ransomware", "housing"));
        assertEquals("no_signal", label("Foreclosure", "technology"));
    }
}
