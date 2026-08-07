/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The exemplars are the server's only lever on question quality that works before a query
 * runs, and they carry a specific weight: a set that shows only answerable
 * questions teaches that every question has an answer here, which is the failure a grounding
 * tool exists to prevent. These pin the properties that make the set teach rather than anchor
 * — enough of them, spanning enough shapes, with the honest refusals present.
 */
class QuestionGuidanceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test void enoughContrastiveExemplarsToGeneralizeFrom() {
    assertTrue(QuestionGuidance.EXEMPLARS.size() >= 6,
        "one or two exemplars get copied literally; a set gets generalized from. Found "
            + QuestionGuidance.EXEMPLARS.size());
  }

  @Test void exemplarsSpanDistinctQuestionShapes() {
    Set<String> shapes = QuestionGuidance.exemplarShapes();
    assertTrue(shapes.size() >= 4,
        "a single shape teaches one rewrite, not the move behind it. Found " + shapes);
  }

  @Test void atLeastOneExemplarTeachesTheHonestNo() {
    boolean found = false;
    for (QuestionGuidance.Exemplar e : QuestionGuidance.EXEMPLARS) {
      if ("honest-refusal".equals(e.shape)) {
        found = true;
      }
    }
    assertTrue(found, "without a refusal exemplar the set teaches that everything is "
        + "answerable here, which is the failure mode this guards against");
  }

  @Test void everyExemplarIsContrastiveAndExplainsItself() {
    for (QuestionGuidance.Exemplar e : QuestionGuidance.EXEMPLARS) {
      assertFalse(e.vague.trim().isEmpty(), "missing the vague form");
      assertFalse(e.sharpened.trim().isEmpty(), "missing the rewrite");
      assertFalse(e.why.trim().isEmpty(),
          "the rewrite without its reason is a template to copy, not a move to learn");
      assertTrue(e.sharpened.length() > e.vague.length(),
          "a rewrite no longer than the original has not added the grain, window, or "
              + "outcome that makes it answerable: " + e.vague);
    }
  }

  @Test void theRenderedBlockCarriesEveryExemplarAndItsShape() {
    String block = QuestionGuidance.exemplarBlock();
    for (QuestionGuidance.Exemplar e : QuestionGuidance.EXEMPLARS) {
      assertTrue(block.contains(e.vague), "missing from the rendered block: " + e.vague);
      assertTrue(block.contains(e.sharpened), "missing rewrite for: " + e.vague);
    }
    for (String shape : QuestionGuidance.exemplarShapes()) {
      assertTrue(block.contains("[" + shape + "]"), "shape not labelled: " + shape);
    }
  }

  @Test void theRubricStatesTheMetaTestThatSeparatesAQuestionFromATopic() {
    assertTrue(QuestionGuidance.RUBRIC.contains("topic, not a question"),
        "the shape-of-the-answer test is the one rule that catches the rest");
  }

  // ── Prompt templates ──────────────────────────────────────────────────────

  @Test void everyTemplateDeclaresADescriptionForEachArgument() {
    for (QuestionGuidance.Template t : QuestionGuidance.TEMPLATES) {
      assertEquals(t.args.length, t.argDescriptions.length,
          "argument descriptions out of step with arguments in " + t.name);
      for (String arg : t.args) {
        assertTrue(t.body.contains("{" + arg + "}"),
            "template " + t.name + " declares '" + arg + "' but never uses it");
      }
    }
  }

  @Test void promptsListIsWellFormedForAClientToRender() {
    ObjectNode list = QuestionGuidance.promptsList();
    JsonNode prompts = list.get("prompts");
    assertEquals(QuestionGuidance.TEMPLATES.size(), prompts.size());
    Set<String> names = new HashSet<>();
    for (JsonNode p : prompts) {
      assertFalse(p.path("name").asText().isEmpty());
      assertFalse(p.path("description").asText().isEmpty());
      assertTrue(p.path("arguments").isArray());
      assertTrue(names.add(p.get("name").asText()), "duplicate prompt name");
    }
  }

  @Test void aFilledTemplateSubstitutesEveryPlaceholder() throws Exception {
    QuestionGuidance.Template t = QuestionGuidance.template("trend_check");
    assertNotNull(t);
    ObjectNode args = MAPPER.createObjectNode();
    args.put("measure", "violent crime per 100k");
    args.put("grain", "agency");
    args.put("window", "2015-2023");
    String text = QuestionGuidance.promptGet(t, args)
        .get("messages").get(0).get("content").get("text").asText();
    assertTrue(text.contains("violent crime per 100k"));
    assertTrue(text.contains("2015-2023"));
    assertFalse(text.contains("{measure}"));
    assertFalse(text.contains("Unfilled placeholders"));
  }

  @Test void anUnfilledPlaceholderIsCalledOutRatherThanQuietlyDropped() {
    QuestionGuidance.Template t = QuestionGuidance.template("trend_check");
    ObjectNode args = MAPPER.createObjectNode();
    args.put("measure", "violent crime per 100k");
    String text = QuestionGuidance.promptGet(t, args)
        .get("messages").get(0).get("content").get("text").asText();
    assertTrue(text.contains("Unfilled placeholders"),
        "a template that loses its grain and window renders as exactly the vague question "
            + "it was meant to replace");
    assertTrue(text.contains("{grain}"));
    assertTrue(text.contains("{window}"));
  }

  @Test void templateLookupIsCaseInsensitiveAndRejectsUnknownNames() {
    assertNotNull(QuestionGuidance.template("Ranking"));
    assertNull(QuestionGuidance.template("no_such_prompt"));
    assertNull(QuestionGuidance.template(null));
  }

  @Test void thePointerSendsTheReaderToTheFullExemplarSet() {
    assertTrue(QuestionGuidance.EXEMPLAR_POINTER.contains("query tool's description"),
        "the short form only works if it says where the long form is");
  }
}
