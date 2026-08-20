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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * search_catalog takes {@code query}; callers reach for {@code keyword}. Across six eval runs
 * that cost a wasted round trip four times. The alias is an explicit rename, not a relaxation
 * of the unknown-argument rejection, which exists because silently ignoring a misnamed field
 * once made this server answer as though data were absent.
 */
@Tag("unit")
class McpServerArgAliasTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static ObjectNode args(String... kv) {
    ObjectNode o = MAPPER.createObjectNode();
    for (int i = 0; i < kv.length; i += 2) {
      o.put(kv[i], kv[i + 1]);
    }
    return o;
  }

  @Test @DisplayName("keyword is renamed to query")
  void renamesKeyword() {
    ObjectNode a = args("keyword", "median income");
    McpServer.applyArgAliases("search_catalog", a);
    assertEquals("median income", a.path("query").asText());
    assertFalse(a.has("keyword"), "the alias must not survive alongside the real name");
  }

  @Test @DisplayName("the plural and the other common synonyms work too")
  void renamesTheOtherSynonyms() {
    for (String alias : new String[] {"keywords", "term", "terms", "q", "search"}) {
      ObjectNode a = args(alias, "crime");
      McpServer.applyArgAliases("search_catalog", a);
      assertEquals("crime", a.path("query").asText(), "alias not honoured: " + alias);
    }
  }

  @Test @DisplayName("passing BOTH leaves the alias in place, so the call is still rejected")
  void doesNotSilentlyDiscardEither() {
    // Quietly dropping one of two conflicting values is the behaviour that caused the
    // original bug. The caller must be told, not guessed at.
    ObjectNode a = args("query", "real", "keyword", "other");
    McpServer.applyArgAliases("search_catalog", a);
    assertEquals("real", a.path("query").asText());
    assertTrue(a.has("keyword"), "the conflicting alias must remain for validation to reject");
  }

  @Test @DisplayName("aliases apply only to the tool that declares them")
  void doesNotLeakToOtherTools() {
    ObjectNode a = args("keyword", "x");
    McpServer.applyArgAliases("query", a);
    assertTrue(a.has("keyword"), "query() has no such alias");
    assertFalse(a.has("query"), "and nothing should have been invented");
  }
}
