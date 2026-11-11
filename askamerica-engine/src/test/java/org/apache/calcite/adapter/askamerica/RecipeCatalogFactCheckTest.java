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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The find_recipe description tells the model to look up 'fact-check news article' before
 * checking an article. That lookup, and the plain words a model would use, must return the
 * article fact-check recipe first.
 */
@Tag("unit")
class RecipeCatalogFactCheckTest {

  private static String topHit(String topic) throws Exception {
    JsonNode out = new ObjectMapper().readTree(RecipeCatalog.find(topic, 5));
    return out.path("recipes").path(0).path("id").asText("");
  }

  @Test void theTopicTheToolDescriptionNamesFindsTheRecipe() throws Exception {
    assertEquals("fact-check-a-news-article", topHit("fact-check news article"));
  }

  @Test void plainFactCheckWordingFindsTheRecipe() throws Exception {
    assertEquals("fact-check-a-news-article", topHit("fact-check this article"));
    assertEquals("fact-check-a-news-article", topHit("pinocchio news report"));
  }
}
