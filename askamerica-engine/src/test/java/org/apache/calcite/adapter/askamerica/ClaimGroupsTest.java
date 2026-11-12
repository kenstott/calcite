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
    if (FELL_SHORT.contains(verdict)) {
      c.put("inaccuracy", "The figure in the claim differs from the one in the evidence");
    }
    return c;
  }

  private static final java.util.List<String> FELL_SHORT = java.util.Arrays.asList(
      "mostly true", "partially false", "partly false", "mostly false", "false");

  /** An author claim offering no cited evidence; {@code errs} may be null. */
  /** Daily eval news q4 (2026-11-12): a headline presenting EIA's 2027 forecast as output
   *  already reached was graded partially false, and seven true secondary figures lifted
   *  the piece to 1 Pinocchio, below the unaided arm's 2. */
  @Test void trueDetailDoesNotLiftTheRatingAboveTheCentralClaim() {
    ObjectNode central = author("US crude production has hit a record 14.3 million b/d",
        "partially false", "toward_thesis");
    central.put("central", true);
    ArrayNode claims = claims(central,
        author("Crude inventories fell to the lowest October level since 2014", "true", null),
        author("Henry Hub averages $2.87 from November through March", "true", null),
        author("Almost 70 Bcf more gas will be in storage", "true", null),
        author("Diesel will average $5.03 a gallon", "true", null),
        author("LNG exports rise from 15 to 19 Bcf/d", "true", null),
        author("Brent averages $86 in 2027", "true", null),
        author("Inventories rise in 2027 as net imports increase", "true", null));
    assertEquals(50, ClaimScoring.score(claims, ClaimScoring.headlineBlock(claims))
        .path("accuracy_score").asInt());
    assertEquals(2, ClaimScoring.pinocchios(claims));
  }

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
        "toward_thesis"), 1, "contradicted").put("central", true));
    claims.add(claim("The committee reported 85,000 lost children", "fidelity", "mostly true"));
    ObjectNode quoted = claim("Crossings nearly stopped overnight", "subject_claims",
        "mostly true");
    quoted.put("speaker", "Senator Vale");
    quoted.putArray("rests_on");
    quoted.put("errs", "neutral");
    quoted.put("central", true);
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

  @Test void aSourceAuditClaimIsNeverCentral() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(0)).put("central", true);
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("claim 1 is grouped `fidelity` and marked `central`"), problem);
  }

  @Test void anOpinionIsGradedNotCheckable() {
    ArrayNode claims = mixedClaims();
    ((ObjectNode) claims.get(1)).put("kind", "opinion");
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("claim 2 is `kind`: \"opinion\" with verdict 'mostly false'"),
        problem);
  }

  /** q14 opinion-r3 (2026-11-11): ten claims, all source audits, none central, published. */
  @Test void aReportOfOnlySourceAuditsIsRefused() {
    String problem = ClaimScoring.enforce(claims(
        claim("The study found rents rose 1.4 percent", "fidelity", "true"),
        claim("The bureau counted 3 million arrivals", "fidelity", "true")));
    assertNotNull(problem);
    assertTrue(problem.contains("every claim is grouped `fidelity`"), problem);
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

  @Test void aSentenceAssertingACauseIsGradedAsACausalClaim() {
    ArrayNode claims = claims(author("Snow falls from the sky", "true", null),
        author("Snow falls from the sky because Russian planes are dropping it",
            "partially false", "toward_thesis"));
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("claim 2") && problem.contains("'because'"), problem);

    ObjectNode cause = restingOn(author(
        "Snow falls from the sky because Russian planes are dropping it", "false",
        "toward_thesis"), 1, "decorative");
    cause.put("kind", "causal");
    cause.put("central", true);
    assertNull(ClaimScoring.enforce(claims(claims.get(0).deepCopy(), cause)));
  }

  @Test void aScoredBlockLeadsWithTheClaimsThatHeldUpLeast() {
    ObjectNode cause = author("Rents are higher due to immigration", "false", "toward_thesis");
    cause.put("kind", "causal");
    ArrayNode claims = claims(author("Rents rose 4 percent", "true", null),
        author("Morale collapsed", "not checkable here", null),
        author("Arrivals rose", "mostly true", "neutral"), cause);
    assertEquals(java.util.Arrays.asList(4, 3, 1, 2),
        ClaimScoring.order(claims, ClaimScoring.GROUP_AUTHOR));

    String html = McpServer.claimsSection(claims).html;
    int table = html.indexOf("<tbody>");
    assertTrue(html.indexOf("<td>4</td><td>Rents are higher due to immigration") > table
        && html.indexOf("<td>4</td>") < html.indexOf("<td>1</td><td>Rents rose 4 percent"), html);
    assertTrue(html.contains("<strong>false</strong> &middot; causal claim"), html);

    JsonNode block = ReportArtifact.validation("https://example.com/op-ed", claims)
        .path("groups").get(0);
    assertEquals(4, block.path("claims").get(0).path("n").asInt());
    assertEquals(2, block.path("claims").get(3).path("n").asInt());
  }

  /** An assertion no evidence was found for is neither honest nor dishonest: it is left out of
   *  the honesty score, as it is of the accuracy score, and counted beside them. */
  @Test void anAssertionWithNoEvidenceFoundIsLeftOutOfHonesty() {
    ArrayNode claims = claims(author("Rents rose 4 percent", "true", null),
        author("Everyone knows the city is finished", "unsupported", "toward_thesis"),
        author("Morale collapsed", "not checkable here", null));
    assertEquals(0.25, ClaimScoring.credit(claims.get(1)), 1e-9);
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(2, score.path("graded").asInt());
    assertEquals(1, score.path("excluded").asInt());
    assertEquals(1, score.path("unsupported").asInt());
    assertEquals(100, score.path("honesty_score").asInt());
    assertTrue(java.util.Arrays.asList(McpServer.VERDICTS).contains("unsupported"));
  }

  @Test void aFailedCentralClaimCapsTheHonestyScore() {
    ObjectNode cause = author("Rents are higher due to immigration", "false", "toward_thesis");
    cause.put("kind", "causal");
    cause.put("central", true);
    ArrayNode claims = claims(author("Rents rose 4 percent", "true", null),
        author("Arrivals rose 9 percent", "true", null),
        author("Vacancy fell", "true", null), cause);
    assertNull(ClaimScoring.enforce(claims));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(75, score.path("honesty_before_cap").asInt());
    assertEquals(25, score.path("honesty_score").asInt());
    assertEquals("dishonest", score.path("honesty").asText());
    assertEquals(4, score.path("central").asInt());
    assertEquals(java.util.Arrays.asList(4, 1, 2, 3),
        ClaimScoring.order(claims, ClaimScoring.GROUP_AUTHOR));

    String html = McpServer.claimsSection(claims).html;
    assertTrue(html.contains("Honesty 25/100 — dishonest (capped by central claim #4; 75 "
        + "before the cap)"), html);
    assertTrue(html.contains("&middot; causal claim &middot; central claim"), html);

    // A central claim that held up caps nothing.
    ((ObjectNode) claims.get(3)).put("verdict", "mostly true").put("errs", "neutral");
    JsonNode held = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertFalse(held.has("honesty_before_cap"));
    assertEquals(94, held.path("honesty_score").asInt());
  }

  @Test void eachPartyNeedsExactlyOneCentralClaim() {
    ArrayNode none = claims(author("Rents rose 4 percent", "true", null),
        author("Vacancy fell", "true", null));
    String problem = ClaimScoring.enforce(none);
    assertNotNull(problem);
    assertTrue(problem.contains("the author's claims have 0 claims marked `central`"), problem);

    ((ObjectNode) none.get(0)).put("central", true);
    assertNull(ClaimScoring.enforce(none));
    ((ObjectNode) none.get(1)).put("central", true);
    assertTrue(ClaimScoring.enforce(none).contains("have 2 claims marked `central`"));
  }

  @Test void aPersonalAttackIsNotCheckableAndCountedOnItsOwnLine() {
    ObjectNode attack = author("John stinks like fish", "not checkable here", null);
    attack.put("kind", "attack");
    attack.put("reason", "name-calling: no fact about John is asserted");
    ArrayNode claims = claims(author("Rents rose 4 percent", "true", null).put("central", true),
        author("Lots of people think John is dumb", "unsupported", "toward_thesis"), attack);
    assertNull(ClaimScoring.enforce(claims));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(2, score.path("graded").asInt());
    assertEquals(1, score.path("attacks").asInt());

    String html = McpServer.claimsSection(claims).html;
    assertTrue(html.contains("<strong>not checkable here</strong> &middot; personal attack"),
        html);
    assertTrue(html.contains("1 fell short, 1 personal attack not gradable. In this article "
        + "only."), html);
    assertTrue(html.contains("a personal attack — not checkable: name-calling"), html);

    attack.put("verdict", "unsupported");
    String problem = ClaimScoring.enforce(claims(claims.get(0).deepCopy(), attack));
    assertNotNull(problem);
    assertTrue(problem.contains("its verdict is 'not checkable here'"), problem);
  }

  @Test void theHeadlineStatesTheVerdictTheScoresGive() {
    String subject = "The Ledger article's claims about rents and immigration";
    ArrayNode bad = claims(
        author("Immigration drove rents up", "false", "toward_thesis").put("central", true),
        author("Lots of people think so", "unsupported", "toward_thesis"),
        author("Rents doubled", "mostly false", "toward_thesis"),
        author("Rents rose 4 percent", "true", null));
    // The central claim is false, so the piece is: one true detail does not lift it.
    assertEquals(subject + " are very biased and false; 1 claim could not be confirmed.",
        ClaimScoring.headline(subject + ".", bad));

    ArrayNode good = claims(author("Rents rose 4 percent", "true", null).put("central", true),
        author("Wages rose 3 percent", "true", null),
        author("Vacancy fell", "true", null));
    String headline = ClaimScoring.headline(subject, good);
    assertEquals(subject + " are balanced and accurate.", headline);
    assertEquals(headline, ClaimScoring.headline(headline, good));

    ArrayNode few = claims(author("Rents rose 4 percent", "true", null).put("central", true));
    assertEquals(subject + " are too few to rate.", ClaimScoring.headline(subject, few));

    assertNull(ClaimScoring.headlineProblem(subject));
    String problem = ClaimScoring.headlineProblem("Rents are up because of immigration");
    assertNotNull(problem);
    assertTrue(problem.contains("subject of the headline"), problem);
  }

  @Test void theMiddleVerdictIsPartiallyFalse() {
    assertEquals(0.5, ClaimScoring.credit(author("Rents rose 4 percent", "partially false",
        "toward_thesis")));
    assertNotNull(McpServer.claimsSection(claims(claim("Rents rose", null, "partially false"))));
    assertThrows(IllegalArgumentException.class,
        () -> McpServer.claimsSection(claims(claim("Rents rose", null, "partially true"))));
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
    ArrayNode claims = claims(author("Rents rose", "true", null).put("central", true),
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

  @Test void theLocalReportLinkIsTheHttpLinkWithTheFilePathAsPlainText() {
    String page = McpServer.localReportPage("Rents [rose]",
        "http://127.0.0.1:56654/a/abc.html",
        "/Users/x/.askamerica/reports/20261001-074149-rents.html", "20261001-074149-rents");
    assertTrue(page.contains("[Rents [rose)](http://127.0.0.1:56654/a/abc.html)"), page);
    assertTrue(page.contains("restore_report with report_id `20261001-074149-rents`"), page);
    assertTrue(page.contains("/Users/x/.askamerica/reports/20261001-074149-rents.html"), page);
    assertFalse(page.contains("file:"), page);
  }

  @Test void withoutAnHttpServerOnlyThePlainPathAndRestoreReportAreGiven() {
    String page = McpServer.localReportPage("Rents", null,
        "/Users/x/.askamerica/reports/r.html", "r");
    assertFalse(page.contains("]("), page);
    assertTrue(page.contains("/Users/x/.askamerica/reports/r.html"), page);
    assertTrue(page.contains("restore_report"), page);
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

  /** Daily eval q14, run r1 (2026-11-10): two claims graded 'unsupported' and none false
   *  made the headline read "partly false". */
  @Test void unsupportedClaimsAreCountedBesideTheAccuracyNotInIt() {
    ObjectNode central = author("Deaths fell to the lowest rate since 2014", "true", null);
    central.put("central", true);
    ArrayNode claims = claims(central,
        author("Deaths fell 2.5 percent", "true", null),
        author("Q1 deaths fell 5 percent", "true", null),
        author("Bicyclist deaths rose 4 percent", "unsupported", "neutral"),
        author("Pedestrian deaths fell", "unsupported", "neutral"));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR);
    assertEquals(3, score.path("checked").asInt());
    assertEquals(2, score.path("unsupported").asInt());
    assertEquals(100, score.path("accuracy_score").asInt());
    assertEquals(100, score.path("honesty_score").asInt());
    assertEquals("are accurate; 2 claims could not be confirmed",
        ClaimScoring.verdictClause(score));
  }

  @Test void fewerThanThreeCheckedClaimsAreTooFewToRate() {
    ObjectNode central = author("Deaths fell", "true", null);
    central.put("central", true);
    ArrayNode claims = claims(central,
        author("Bicyclist deaths rose 4 percent", "unsupported", "neutral"),
        author("Pedestrian deaths fell", "unsupported", "neutral"));
    assertEquals("are too few to rate; 2 claims could not be confirmed",
        ClaimScoring.verdictClause(ClaimScoring.score(claims, ClaimScoring.GROUP_AUTHOR)));
  }

  /** Daily eval q14, run r3 (2026-11-11): nine of ten claims, all figures the article reported
   *  as fact, were grouped fidelity, and the headline read "too few to rate". */
  @Test void moreSourceAuditClaimsThanScoredOnesAreRefused() {
    ObjectNode central = author("Deaths fell to the lowest rate since 2014", "mostly true",
        "neutral");
    central.put("central", true);
    ArrayNode claims = claims(central,
        claim("Deaths fell 2.5% to 16,750, NHTSA said", "fidelity", "true"),
        claim("2021 deaths rose 10.8% to 43,230", "fidelity", "true"));
    String problem = ClaimScoring.enforce(claims);
    assertNotNull(problem);
    assertTrue(problem.contains("2 claims are grouped `fidelity` and only 1 are scored"),
        problem);
    claims.add(author("Deaths fell 2.5% to 16,750", "true", null));
    assertNull(ClaimScoring.enforce(claims));
  }

  /** Daily eval q14 (2026-11-10): an estimate that matched NHTSA's own was graded 'mostly
   *  true' with no difference named, and the hedge cost the article a Pinocchio. */
  @Test void aVerdictBelowTrueMustNameTheInaccuracy() {
    ObjectNode central = author("Deaths fell to 16,750", "mostly true", "neutral");
    central.put("central", true);
    central.remove("inaccuracy");
    String refusal = ClaimScoring.enforce(claims(central,
        author("Deaths fell 2.5 percent", "true", null)));
    assertNotNull(refusal);
    assertTrue(refusal.contains("claim 1 is graded 'mostly true' with no `inaccuracy`"),
        refusal);
    assertTrue(refusal.contains("the verdict MUST be 'true'"), refusal);
  }

  @Test void aHedgeAboutTheFigureIsNotAnInaccuracy() {
    ObjectNode central = author("Deaths fell to 16,750", "mostly true", "neutral");
    central.put("central", true);
    central.put("inaccuracy", "It is a preliminary estimate, not primary-confirmed");
    String refusal = ClaimScoring.enforce(claims(central,
        author("Deaths fell 2.5 percent", "true", null)));
    assertNotNull(refusal);
    assertTrue(refusal.contains("names no difference between the claim and the evidence"),
        refusal);
  }

  /** Daily eval q14, run inaccuracy-r1 (2026-11-11): five 'mostly true' verdicts whose
   *  `inaccuracy` only hedged, each carrying a number, all passed. */
  @Test void aHedgeCarryingANumberIsStillNotAnInaccuracy() {
    String[] hedges = {
        "1.02 matches NHTSA; 'lowest since 2014' is Reuters' characterization I could not "
            + "confirm from NHTSA tables.",
        "NHTSA's earlier Q1 release said -4.3%; later revised to about 5% per Reuters. "
            + "Revision, not error.",
        "Not in warehouse; reported by NHTSA via Reuters, primary page 403.",
        "Attributed to unnamed experts; consistent with NHTSA/GHSA literature but not tested "
            + "here."};
    for (String hedge : hedges) {
      ObjectNode central = author("Deaths fell to 16,750", "mostly true", "neutral");
      central.put("central", true);
      central.put("inaccuracy", hedge);
      String refusal = ClaimScoring.enforce(claims(central,
          author("Deaths fell 2.5 percent", "true", null)));
      assertNotNull(refusal, hedge);
    }
  }

  @Test void aStatedDifferenceFromAnEstimateIsAnInaccuracy() {
    ObjectNode central = author("Deaths fell to 16,750", "mostly true", "neutral");
    central.put("central", true);
    central.put("inaccuracy", "The claim says 16,750; NHTSA's estimate is 16,950, not 16,750");
    assertNull(ClaimScoring.enforce(claims(central,
        author("Deaths fell 2.5 percent", "true", null))));
  }

  @Test void unsupportedAndTrueClaimsNeedNoInaccuracy() {
    ObjectNode central = author("Deaths fell to 16,750", "true", null);
    central.put("central", true);
    assertNull(ClaimScoring.enforce(claims(central,
        author("Bicyclist deaths rose 4 percent", "unsupported", "neutral"))));
  }

  /** Daily eval q14, run inaccuracy-r3 (2026-11-11): every checked claim true, headline
   *  "accurate", and the agent still wrote "1 Pinocchio". */
  @Test void thePinocchioRatingComesFromTheClaimScores() {
    ObjectNode central = author("Deaths fell to 16,750", "true", null);
    central.put("central", true);
    ArrayNode claims = claims(central,
        author("Deaths fell 2.5 percent", "true", null),
        author("Q1 deaths fell 5 percent", "true", null),
        author("Bicyclist deaths rose 4 percent", "unsupported", "neutral"));
    assertEquals(0, ClaimScoring.pinocchios(claims));
    assertNull(ClaimScoring.pinocchioProblem("**Rating: 0 Pinocchios** on the 0-4 Pinocchio "
        + "scale.", 0));
    String wrong = ClaimScoring.pinocchioProblem("Rating: 1 Pinocchio (mostly accurate).", 0);
    assertNotNull(wrong);
    assertTrue(wrong.contains("The rating MUST be 0 Pinocchios"), wrong);
    assertNotNull(ClaimScoring.pinocchioProblem("Rating: mostly accurate.", 0));
    assertNull(ClaimScoring.pinocchioProblem("Rating: one Pinocchio.", 1));
    assertNull(ClaimScoring.pinocchioProblem("No rating.", -1));
  }

  /** Daily eval q15 (2026-11-11): 23 true, 1 mostly true and 1 partially false ("record price")
   *  scored about 97 and was rated 0, though the piece had a named inaccuracy. */
  @Test void aPartiallyFalseClaimRulesOutZeroPinocchios() {
    ObjectNode central = author("Capacity prices rose tenfold", "true", null);
    central.put("central", true);
    ArrayNode claims = claims(central);
    for (int i = 0; i < 22; i++) {
      claims.add(author("Fact " + i, "true", null));
    }
    claims.add(author("Prices rose 2 percent", "mostly true", "neutral"));
    ObjectNode record = author("Despite a record price", "partially false", "toward_thesis");
    record.put("inaccuracy", "the claim says record; the price is below the prior two auctions");
    claims.add(record);
    assertEquals(1, ClaimScoring.pinocchios(claims));
    String clause = ClaimScoring.verdictClause(
        ClaimScoring.score(claims, ClaimScoring.headlineBlock(claims)));
    assertTrue(clause.contains("mostly accurate"), clause);
  }

  /** Daily eval q16 rerun (2026-11-11): 2 checked author claims left the piece unrated, and the
   *  agent rated it 2 Pinocchios on a quoted secretary's misstatement. */
  @Test void tooFewAuthorClaimsAreRatedWithTheFidelityClaims() {
    ObjectNode central = author("Ground beef averaged $7.16 in August", "true", null);
    central.put("central", true);
    ObjectNode quoted = claim("A pound of ground beef was $6.50", "subject_claims",
        "mostly false");
    quoted.put("speaker", "Secretary Rollins");
    quoted.put("errs", "toward_thesis");
    ArrayNode claims = claims(central,
        author("The price was $6.631 a year earlier", "true", null),
        claim("The herd is 86.2 million, smallest in 75 years", "fidelity", "true"),
        claim("CPA counts 450,000 fewer ranchers since 1997", "fidelity", "mostly true"),
        claim("Diesel averaged $6.5107 per AAA", "fidelity", "true"),
        quoted);
    assertEquals(ClaimScoring.BLOCK_AUTHOR_WITH_FIDELITY, ClaimScoring.headlineBlock(claims));
    JsonNode score = ClaimScoring.score(claims, ClaimScoring.headlineBlock(claims));
    assertEquals(5, score.path("checked").asInt());
    assertEquals(0, ClaimScoring.pinocchios(claims));
  }

  @Test void enoughAuthorClaimsAreRatedWithoutTheFidelityClaims() {
    ObjectNode central = author("Deaths fell to 16,750", "true", null);
    central.put("central", true);
    ArrayNode claims = claims(central,
        author("Deaths fell 2.5 percent", "true", null),
        author("Q1 deaths fell 5 percent", "true", null),
        claim("NHTSA says the rate is lowest since 2014", "fidelity", "false"));
    assertEquals("author_claims", ClaimScoring.headlineBlock(claims));
    assertEquals(0, ClaimScoring.pinocchios(claims));
  }

  @Test void aMostlyTrueClaimAloneStillAllowsZeroPinocchios() {
    ObjectNode central = author("Deaths fell to 16,750", "true", null);
    central.put("central", true);
    ArrayNode claims = claims(central);
    for (int i = 0; i < 10; i++) {
      claims.add(author("Fact " + i, "true", null));
    }
    claims.add(author("Deaths fell 2.5 percent", "mostly true", "neutral"));
    assertEquals(0, ClaimScoring.pinocchios(claims));
  }

  @Test void tooFewCheckedClaimsEarnNoPinocchioRating() {
    ObjectNode central = author("Deaths fell", "true", null);
    central.put("central", true);
    assertEquals(-1, ClaimScoring.pinocchios(claims(central,
        author("Bicyclist deaths rose 4 percent", "unsupported", "neutral"))));
  }

  @Test void aMostlyFalseRecordEarnsThreePinocchios() {
    ObjectNode central = author("Wages were suppressed", "mostly false", "toward_thesis");
    central.put("central", true);
    assertEquals(3, ClaimScoring.pinocchios(claims(central,
        author("Rents rose 9 percent", "mostly false", "toward_thesis"),
        author("Rents rose in 40 cities", "mostly false", "toward_thesis"))));
  }

  /** Daily eval q14, run rating-r3 (2026-11-11): the experts' pandemic explanation, a causal
   *  claim, was graded 'mostly true' because the cause was "not directly measured here". */
  @Test void aCausalClaimGradedMostlyRightOnAHedgeIsRefused() {
    ObjectNode fact = author("Deaths rose in 2021", "true", null);
    fact.put("central", true);
    ObjectNode cause = author("Emptier roads caused riskier driving", "mostly true", "neutral");
    cause.put("kind", "causal");
    cause.put("inaccuracy", "Causal mechanism is an expert view, not directly measured here; "
        + "experts unnamed.");
    String refusal = ClaimScoring.enforce(claims(fact, cause));
    assertNotNull(refusal);
    assertTrue(refusal.contains("claim 2 is graded 'mostly true'"), refusal);

    cause.put("verdict", "mostly false");
    cause.put("inaccuracy", "No evidence establishes the link; it is not measured here.");
    String falseCause = ClaimScoring.enforce(claims(fact, cause));
    assertTrue(falseCause == null || !falseCause.contains("claim 2 is graded"), falseCause);
  }
}
