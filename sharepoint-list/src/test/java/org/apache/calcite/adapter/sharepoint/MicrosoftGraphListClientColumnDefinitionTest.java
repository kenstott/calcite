/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.sharepoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Graph column definition written for each column type of a new list.
 */
@Tag("unit")
class MicrosoftGraphListClientColumnDefinitionTest {
  private static ObjectNode definitionOf(String type) {
    return MicrosoftGraphListClient.columnDefinition(new ObjectMapper(),
        new SharePointColumn("Priority", "Priority", type, false));
  }

  @Test void anIntegerColumnIsANumberWithoutDecimalPlaces() {
    ObjectNode definition = definitionOf("integer");
    // Created as text, the column answered HTTP 500 to the number written to it
    assertFalse(definition.has("text"));
    assertEquals("none", definition.get("number").get("decimalPlaces").asText());
  }

  @Test void eachSupportedTypeHasItsOwnFacet() {
    assertTrue(definitionOf("text").has("text"));
    assertTrue(definitionOf("number").has("number"));
    assertFalse(definitionOf("number").get("number").has("decimalPlaces"));
    assertTrue(definitionOf("boolean").has("boolean"));
    assertTrue(definitionOf("dateTime").has("dateTime"));
    assertTrue(definitionOf("choice").get("choice").has("choices"));
  }

  @Test void theNameDisplayNameAndRequiredFlagAreCarried() {
    ObjectNode definition =
        MicrosoftGraphListClient.columnDefinition(new ObjectMapper(),
            new SharePointColumn("TextTitle", "Text Title", "text", true));
    assertEquals("TextTitle", definition.get("name").asText());
    assertEquals("Text Title", definition.get("displayName").asText());
    assertTrue(definition.get("required").asBoolean());
  }

  @Test void aTypeWithNoDefinitionIsRefused() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> definitionOf("geolocation"));
    assertTrue(e.getMessage().contains("geolocation"), e.getMessage());
  }
}
