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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A validation sorts each claim into the group its verdict grades, and rates each group from
 * its own claims.
 *
 * <p>Measured live 2026-10-01 (an op-ed validation, 20 claims): 11 claims relayed figures from
 * sources the piece cited and 9 were the author's own, but all 20 rendered in one table under
 * both Pinocchio banners, so figures the piece had only relayed correctly read as credit for
 * the accuracy of its own claims. The report also linked the article only among its citations
 * and handed the reader an http link that died with the engine process.
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

  private static ObjectNode rating(int count) {
    ObjectNode r = MAPPER.createObjectNode();
    r.put("count", count);
    r.put("explanation", "why");
    return r;
  }

  private static ObjectNode split(int fidelity, int claimsAccuracy) {
    ObjectNode p = MAPPER.createObjectNode();
    p.set("fidelity", rating(fidelity));
    p.set("claims_accuracy", rating(claimsAccuracy));
    return p;
  }

  private static ArrayNode mixedClaims() {
    ArrayNode claims = MAPPER.createArrayNode();
    claims.add(claim("The study found rents rose 1.4 percent", "fidelity", "true"));
    claims.add(claim("Wages at the low end were suppressed", "claims_accuracy", "mostly false"));
    claims.add(claim("The committee reported 85,000 lost children", "fidelity", "mostly true"));
    return claims;
  }

  @Test void aClaimWithNoGroupIsRefusedByNumber() {
    ArrayNode claims = mixedClaims();
    claims.add(claim("Crossings nearly stopped overnight", null, "mostly true"));
    String problem = McpServer.enforceClaimGroups(claims, split(1, 2));
    assertNotNull(problem);
    assertTrue(problem.contains("[4]"), problem);
  }

  @Test void claimsInBothGroupsNeedASplitRating() {
    assertNotNull(McpServer.enforceClaimGroups(mixedClaims(), rating(2)));
    assertNull(McpServer.enforceClaimGroups(mixedClaims(), split(0, 2)));
  }

  @Test void claimsInOneGroupNeedASingleRating() {
    ArrayNode claims = MAPPER.createArrayNode();
    claims.add(claim("Rents climbed every year", "claims_accuracy", "true"));
    claims.add(claim("Native employment held steady", "claims_accuracy", "true"));
    assertNotNull(McpServer.enforceClaimGroups(claims, split(0, 0)));
    assertNull(McpServer.enforceClaimGroups(claims, rating(0)));
  }

  @Test void aGroupHoldingAFalseClaimCannotBeRatedZero() {
    String problem = McpServer.enforceClaimGroups(mixedClaims(), split(0, 0));
    assertNotNull(problem);
    assertTrue(problem.contains("claims_accuracy"), problem);
  }

  @Test void aSingleClaimNeedsNoGroup() {
    ArrayNode claims = MAPPER.createArrayNode();
    claims.add(claim("Rents climbed every year", null, "true"));
    assertNull(McpServer.enforceClaimGroups(claims, MAPPER.missingNode()));
  }

  @Test void eachGroupRendersItsOwnRatingTallyAndTable() {
    String html = McpServer.claimsSection(mixedClaims(), split(0, 2)).html;
    int fidelity = html.indexOf("<h3>Fidelity");
    int accuracy = html.indexOf("<h3>Claims Accuracy");
    int detail = html.indexOf("<h3>Claim detail");
    assertTrue(fidelity >= 0 && fidelity < accuracy && accuracy < detail, html);

    String fidelityBlock = html.substring(fidelity, accuracy);
    String accuracyBlock = html.substring(accuracy, detail);
    assertTrue(fidelityBlock.contains("Fidelity: No Pinocchios"), fidelityBlock);
    assertTrue(fidelityBlock.contains("<strong>2</strong> assertions checked"), fidelityBlock);
    assertTrue(fidelityBlock.contains("rents rose 1.4 percent"), fidelityBlock);
    assertTrue(fidelityBlock.contains("85,000 lost children"), fidelityBlock);
    assertFalse(fidelityBlock.contains("Wages at the low end"), fidelityBlock);

    assertTrue(accuracyBlock.contains("Claims Accuracy: 2 of 4 Pinocchios"), accuracyBlock);
    assertTrue(accuracyBlock.contains("<strong>1</strong> assertions checked"), accuracyBlock);
    assertTrue(accuracyBlock.contains("Wages at the low end"), accuracyBlock);
    // The correctly relayed study figure is no credit to the piece's own claims.
    assertFalse(accuracyBlock.contains("<strong>1</strong> true"), accuracyBlock);
    assertFalse(accuracyBlock.contains("rents rose 1.4 percent"), accuracyBlock);
  }

  @Test void claimNumbersFollowTheOrderGiven() {
    String html = McpServer.claimsSection(mixedClaims(), split(0, 2)).html;
    assertTrue(html.contains("<td>3</td><td>The committee reported 85,000 lost children"), html);
    assertTrue(html.contains("<td>2</td><td>Wages at the low end"), html);
  }

  @Test void theArtifactGetsTheClaimsAsDataPerGroup() {
    JsonNode v = ReportArtifact.validation("https://example.com/op-ed", mixedClaims(),
        split(0, 2));
    assertEquals("https://example.com/op-ed", v.path("source_url").asText());
    assertEquals(2, v.path("groups").size());

    JsonNode fidelity = v.path("groups").get(0);
    assertEquals("fidelity", fidelity.path("group").asText());
    assertEquals(0, fidelity.path("pinocchios").path("count").asInt(-1));
    assertEquals(1, fidelity.path("tally").path("true").asInt());
    assertEquals(1, fidelity.path("tally").path("mostly true").asInt());
    assertEquals(2, fidelity.path("claims").size());
    assertEquals(1, fidelity.path("claims").get(0).path("n").asInt());
    assertEquals(3, fidelity.path("claims").get(1).path("n").asInt());

    JsonNode accuracy = v.path("groups").get(1);
    assertEquals("claims_accuracy", accuracy.path("group").asText());
    assertEquals(2, accuracy.path("pinocchios").path("count").asInt(-1));
    assertEquals(1, accuracy.path("tally").size());
    assertEquals(1, accuracy.path("tally").path("mostly false").asInt());
    assertEquals("Wages at the low end were suppressed",
        accuracy.path("claims").get(0).path("assertion").asText());
  }

  @Test void aSingleRatingRidesOnTheOnlyGroup() {
    ArrayNode claims = MAPPER.createArrayNode();
    claims.add(claim("Rents climbed every year", "claims_accuracy", "true"));
    claims.add(claim("Native employment held steady", "claims_accuracy", "true"));
    JsonNode v = ReportArtifact.validation("https://example.com/op-ed", claims, rating(0));
    assertEquals(1, v.path("groups").size());
    assertEquals(0, v.path("groups").get(0).path("pinocchios").path("count").asInt(-1));
    assertEquals(2, v.path("groups").get(0).path("tally").path("true").asInt());
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
