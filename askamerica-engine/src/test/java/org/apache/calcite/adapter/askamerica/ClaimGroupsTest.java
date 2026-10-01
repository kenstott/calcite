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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A validation sorts each claim by who asserted it, scores the author and each speaker from
 * their own claims, and audits the sources the piece relays without scoring them.
 *
 * <p>Measured live 2026-10-01 (an op-ed validation, 20 claims): 11 claims relayed figures from
 * sources the piece cited and 9 were the author's own, but all 20 rendered in one table under
 * one rating, so figures the piece had only relayed correctly read as credit for the accuracy
 * of its own claims. That is the opening for citejacking: a piece that cites sources accurately
 * while its own conclusions contradict or stretch them. The report also linked the article only
 * among its citations and handed the reader an http link that died with the engine process.
 */
@Tag("unit")
class ClaimGroupsTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static ObjectNode claim(String assertion, String group, String verdict) {
    ObjectNode c = MAPPER.createObjectNode();
    c.put("assertion", assertion);
    if (group != null) {
      c.put("group", group);
    }
    c.put("verdict", verdict);
    return c;
  }

  /** An author claim offering no cited evidence; {@code errs} may be null. */
  private static ObjectNode author(String assertion, String verdict, String errs) {
    ObjectNode c = claim(assertion, "author_claims", verdict);
    c.putArray("rests_on");
    if (errs != null) {
      c.put("errs", errs);
    }
    return c;
  }

  private static ObjectNode restingOn(ObjectNode c, int claimNumber, String support) {
    c.putArray("rests_on").add(claimNumber);
    c.put("support", support);
    c.put("source_finding", "Rents rose 1.4 percent; wages were not measured");
    return c;
  }

  private static ArrayNode mixedClaims() {
    ArrayNode claims = MAPPER.createArrayNode();
    claims.add(claim("The study found rents rose 1.4 percent", "fidelity", "true"));
    claims.add(restingOn(author("Wages at the low end were suppressed", "mostly false",
        "toward_thesis"), 1, "contradicted"));
    claims.add(claim("The committee reported 85,000 lost children", "fidelity", "mostly true"));
    ObjectNode quoted = claim("Crossings nearly stopped overnight", "subject_claims",
        "mostly true");
    quoted.put("speaker", "Senator Vale");
    quoted.putArray("rests_on");
    quoted.put("errs", "neutral");
    claims.add(quoted);
    return claims;
  }

  private static ArrayNode claims(ObjectNode... all) {
    ArrayNode claims = MAPPER.createArrayNode();
    for (ObjectNode c : all) {
      claims.add(c);
    }
    return claims;
  }

  @Test void wellFormedClaimsAreAccepted() {
    assertNull(ClaimScoring.enforce(mixedClaims()));
  }

  @Test void aClaimWithNoGroupIsRefusedByNumber() {
    ArrayNode claims = mixedClaims();
    claims.add(claim("Removals doubled", null, "mostly true"));
    claims.add(claim("Removals tripled", "claims_accuracy", "false"));
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("[5, 6]"), problem);
  }

  @Test void aSubjectClaimNeedsItsSpeaker() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(3)).remove("speaker");
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("claim 4") && problem.contains("`speaker`"), problem);
  }

  @Test void aScoredClaimSaysWhatItRestsOn() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(3)).remove("rests_on");
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("claim 4") && problem.contains("`rests_on`"), problem);
  }

  @Test void restsOnNamesAnotherClaim() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(1)).putArray("rests_on").add(2).add(9);
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("entry 2") && problem.contains("entry 9"), problem);
  }

  @Test void aClaimRestingOnEvidenceSaysHowWellItIsCarriedAndWhatTheEvidenceFound() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(1)).remove("support");
    ((ObjectNode) claims.get(1)).remove("source_finding");
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("`support`") && problem.contains("`source_finding`"), problem);
  }

  @Test void aClaimThatFellShortSaysWhichWayItErrs() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(3)).remove("errs");
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("claim 4") && problem.contains("`errs`"), problem);

    // A true claim its evidence contradicts fell short too.
    ArrayNode citejacked = claims(claim("The study found rents rose 1.4 percent", "fidelity",
        "true"), restingOn(author("Rents are stable", "true", null), 1, "contradicted"));
    assertNotNull(ClaimScoring.enforce(citejacked));
  }

  @Test void aSingleClaimNeedsNoGroup() {
    assertNull(ClaimScoring.enforce(claims(claim("Rents climbed every year", null, "true"))));
  }

  @Test void relayingASourceAccuratelyEarnsNoCredit() {
    ArrayNode claims = claims(
        claim("The study found rents rose 1.4 percent", "fidelity", "true"),
        claim("The bureau counted 3 million arrivals", "fidelity", "true"),
        claim("The committee reported 85,000 lost children", "fidelity", "true"),
        author("Rents doubled", "false", "toward_thesis"),
        author("Arrivals tripled", "false", "toward_thesis"),
        author("Every child was lost", "false", "toward_thesis"));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(3, score.path("graded").asInt());
    assertEquals(0, score.path("honesty_score").asInt(-1));
    assertEquals("very dishonest", score.path("honesty").asText());
    assertEquals(100, score.path("bias_score").asInt());
    assertEquals("very biased", score.path("bias").asText());
    assertFalse(ClaimScoring.isScored(ClaimScoring.GROUP_FIDELITY));
  }

  @Test void anAuthorWhoseClaimsHoldUpIsHonestWithNoLean() {
    ArrayNode claims = claims(author("Rents rose", "true", null),
        author("Arrivals rose", "true", null), author("Wages rose", "true", null));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(100, score.path("honesty_score").asInt());
    assertEquals("honest", score.path("honesty").asText());
    assertFalse(score.has("bias_score"));
    assertEquals("no errors to lean either way", score.path("bias").asText());
  }

  @Test void citejackingCostsTheClaimItsCredit() {
    ObjectNode source = claim("The study found rents rose 1.4 percent", "fidelity", "true");
    assertEquals(0.0, ClaimScoring.credit(
        restingOn(author("Rents are stable", "true", "toward_thesis"), 1, "contradicted")));
    assertEquals(0.5, ClaimScoring.credit(
        restingOn(author("Rents rose everywhere", "true", "toward_thesis"), 1, "overreach")));
    assertEquals(0.25, ClaimScoring.credit(
        restingOn(author("Rents fell", "mostly false", "toward_thesis"), 1, "overreach")));
    assertEquals(1.0, ClaimScoring.credit(
        restingOn(author("Rents rose 1.4 percent", "true", null), 1, "supported")));
    assertEquals(1.0, ClaimScoring.credit(
        restingOn(author("Rents rose", "true", null), 1, "decorative")));

    ArrayNode claims = claims(source,
        restingOn(author("Rents are stable", "true", "toward_thesis"), 1, "contradicted"),
        author("Rents rose", "true", null),
        restingOn(author("Rents rose everywhere", "true", "toward_thesis"), 1, "overreach"));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(50, score.path("honesty_score").asInt());
    assertEquals(2, score.path("citejacked").size());
    assertEquals(2, score.path("citejacked").get(0).asInt());
    assertEquals(4, score.path("citejacked").get(1).asInt());
  }

  @Test void theAuthorAndEachSpeakerAreScoredFromTheirOwnClaims() {
    ArrayNode claims = mixedClaims();
    // The author, one speaker, then the source audit.
    assertEquals(3, ClaimScoring.blocks(claims).size());
    JsonNode author = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(1, author.path("graded").asInt());
    assertEquals(0, author.path("honesty_score").asInt(-1));
    JsonNode speaker = ClaimScoring.score(claims, ClaimScoring.blocks(claims).get(1));
    assertEquals(1, speaker.path("graded").asInt());
    assertEquals(75, speaker.path("honesty_score").asInt());
    assertEquals(0, speaker.path("bias_score").asInt(-1));
  }

  @Test void tooFewClaimsGetAScoreButNoCharacterization() {
    JsonNode author = ClaimScoring.score(mixedClaims(), ClaimScoring.GROUP_AUTHOR);
    assertEquals("too few checkable claims to characterize", author.path("honesty").asText());
    assertEquals("too few errors to characterize", author.path("bias").asText());
  }

  @Test void claimsThatCouldNotBeGradedAreLeftOutOfTheScore() {
    ArrayNode claims = claims(author("Rents rose", "true", null),
        author("Morale collapsed", "not checkable here", null),
        author("Arrivals fell last month", "stale vintage", null));
    assertNull(ClaimScoring.enforce(claims));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(1, score.path("graded").asInt());
    assertEquals(2, score.path("excluded").asInt());
    assertEquals(100, score.path("honesty_score").asInt());
  }

  @Test void biasIsTheNetLeanOfTheError() {
    ArrayNode claims = claims(author("Rents doubled", "false", "toward_thesis"),
        author("Arrivals halved", "false", "against_thesis"),
        author("Wages fell", "false", "toward_thesis"),
        author("Jobs vanished", "false", "neutral"));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(4, score.path("errors").asInt());
    assertEquals(25, score.path("bias_score").asInt());
    assertEquals("somewhat biased", score.path("bias").asText());
  }

  @Test void eachBlockRendersItsOwnScoreTallyAndTable() {
    String html = McpServer.claimsSection(mixedClaims()).html;
    int author = html.indexOf("<h3>Author");
    int speaker = html.indexOf("<h3>Claims by Senator Vale");
    int audit = html.indexOf("<h3>Source Audit");
    int detail = html.indexOf("<p class=\"note\">Verdicts:");
    assertTrue(author >= 0 && author < speaker && speaker < audit && audit < detail, html);

    String authorBlock = html.substring(author, speaker);
    String speakerBlock = html.substring(speaker, audit);
    String auditBlock = html.substring(audit, detail);
    assertTrue(authorBlock.contains("Honesty 0/100"), authorBlock);
    assertTrue(authorBlock.contains("Bias +100/100"), authorBlock);
    assertTrue(authorBlock.contains("Citejacking:</strong> #2"), authorBlock);
    assertTrue(authorBlock.contains("In this article only."), authorBlock);
    assertTrue(authorBlock.contains("<strong>1</strong> assertions checked"), authorBlock);
    assertTrue(authorBlock.contains("<td>#1 — contradicted</td>"), authorBlock);
    // The correctly relayed study figure is no credit to the author's own claims.
    assertFalse(authorBlock.contains("<strong>1</strong> true"), authorBlock);
    assertFalse(authorBlock.contains("rents rose 1.4 percent"), authorBlock);

    assertTrue(speakerBlock.contains("Honesty 75/100"), speakerBlock);
    assertTrue(speakerBlock.contains("Crossings nearly stopped"), speakerBlock);
    assertTrue(speakerBlock.contains("<td>none cited</td>"), speakerBlock);

    assertTrue(auditBlock.contains("Not scored."), auditBlock);
    assertFalse(auditBlock.contains("Honesty"), auditBlock);
    assertTrue(auditBlock.contains("<strong>2</strong> assertions checked"), auditBlock);
    assertTrue(auditBlock.contains("rents rose 1.4 percent"), auditBlock);
    assertTrue(auditBlock.contains("85,000 lost children"), auditBlock);
    assertFalse(auditBlock.contains("Wages at the low end"), auditBlock);

    assertFalse(html.contains("Pinocchio"), html);
    assertTrue(html.contains("<dt>What that evidence found</dt>"), html);
  }

  @Test void claimNumbersFollowTheOrderGiven() {
    String html = McpServer.claimsSection(mixedClaims()).html;
    assertTrue(html.contains("<td>3</td><td>The committee reported 85,000 lost children"), html);
    assertTrue(html.contains("<td>2</td><td>Wages at the low end"), html);
  }

  @Test void anUnknownGroupIsRefused() {
    ArrayNode claims = claims(claim("Rents doubled", "claims_accuracy", "false"));
    assertThrows(IllegalArgumentException.class, () -> McpServer.claimsSection(claims));
  }

  @Test void theArtifactGetsTheClaimsAsDataPerBlock() {
    JsonNode v = ReportArtifact.validation("https://example.com/op-ed", mixedClaims());
    assertEquals("https://example.com/op-ed", v.path("source_url").asText());
    assertEquals("this article only", v.path("scope").asText());
    assertEquals(3, v.path("groups").size());

    JsonNode author = v.path("groups").get(0);
    assertEquals("author_claims", author.path("group").asText());
    assertEquals(0, author.path("score").path("honesty_score").asInt(-1));
    assertEquals(2, author.path("score").path("citejacked").get(0).asInt());
    assertEquals(1, author.path("tally").size());
    assertEquals(1, author.path("tally").path("mostly false").asInt());
    assertEquals("Wages at the low end were suppressed",
        author.path("claims").get(0).path("assertion").asText());

    JsonNode speaker = v.path("groups").get(1);
    assertEquals("subject_claims", speaker.path("group").asText());
    assertEquals("Senator Vale", speaker.path("speaker").asText());
    assertEquals(75, speaker.path("score").path("honesty_score").asInt());
    assertEquals(4, speaker.path("claims").get(0).path("n").asInt());

    JsonNode audit = v.path("groups").get(2);
    assertEquals("fidelity", audit.path("group").asText());
    assertFalse(audit.has("score"));
    assertFalse(audit.has("pinocchios"));
    assertEquals(1, audit.path("tally").path("true").asInt());
    assertEquals(1, audit.path("tally").path("mostly true").asInt());
    assertEquals(2, audit.path("claims").size());
    assertEquals(1, audit.path("claims").get(0).path("n").asInt());
    assertEquals(3, audit.path("claims").get(1).path("n").asInt());
  }

  @Test void theLocalReportLinkIsAFileLink() {
    String link = McpServer.reportFileLink("Rents [rose]",
        "file:/Users/x/.askamerica/reports/20261001-074149-rents.html");
    assertEquals("[Rents [rose)](file:/Users/x/.askamerica/reports/20261001-074149-rents.html)",
        link);
  }

  @Test void theArticleUnderReviewIsLinkedAboveTheFirstSection() {
    String url = "https://thehill.com/opinion/immigration/6116385-some-op-ed/";
    String html = ReportPage.render("Title", "Subtitle",
        Collections.singletonList(new ReportPage.Section("Summary", "<p>Finding.</p>")),
        null, null, Collections.<ReportPage.Source>emptyList(), null, null,
        Collections.<ReportPage.Filter>emptyList(), url);
    int link = html.indexOf("<a href=\"" + url + "\">");
    assertTrue(link >= 0, "the article link is missing");
    assertTrue(html.indexOf("<h1>") < link && link < html.indexOf("Finding."),
        "the article link belongs in the header, above the summary");
  }

  @Test void aReportThatIsNotAValidationHasNoArticleLine() {
    String html = ReportPage.render("Title", null,
        Collections.singletonList(new ReportPage.Section("Summary", "<p>Finding.</p>")),
        null, null, Collections.<ReportPage.Source>emptyList(), null, null,
        Collections.<ReportPage.Filter>emptyList(), null);
    assertFalse(html.contains("class=\"under-review\""));
  }
}
