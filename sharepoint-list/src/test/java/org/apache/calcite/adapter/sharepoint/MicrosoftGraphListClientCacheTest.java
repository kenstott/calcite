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

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A list created or dropped through the client is seen by the next discovery, although the
 * lists of a site are cached and the cache is shared by every client of that site.
 */
@Tag("unit")
class MicrosoftGraphListClientCacheTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A Graph endpoint holding lists in memory; no call leaves the JVM. */
  private static class FakeGraph extends MicrosoftGraphListClient {
    private final Map<String, String> displayNameById;
    /** The id of a list Graph still lists although it has been dropped. */
    String droppedButListed = "none";

    FakeGraph(String siteUrl, Map<String, String> displayNameById) {
      super(siteUrl, new SharePointAuth() {
        @Override public String getAccessToken() {
          return "token";
        }
      });
      this.displayNameById = displayNameById;
    }

    private ObjectNode list(String id) {
      ObjectNode list = MAPPER.createObjectNode();
      list.put("id", id);
      list.put("displayName", displayNameById.get(id));
      list.put("name", displayNameById.get(id));
      return list;
    }

    @Override public JsonNode executeGraphCall(String method, String url, JsonNode requestBody)
        throws GraphApiException {
      ObjectNode response = MAPPER.createObjectNode();
      if (url.contains("/columns")) {
        if (url.contains("/" + droppedButListed + "/")) {
          throw new GraphApiException(404, "The specified list was not found");
        }
        response.putArray("value");
      } else if (url.endsWith("/lists") && "POST".equals(method)) {
        String id = "id-" + (displayNameById.size() + 1);
        displayNameById.put(id, requestBody.get("displayName").asText());
        response.put("id", id);
      } else if (url.endsWith("/lists")) {
        ArrayNode value = response.putArray("value");
        for (String id : displayNameById.keySet()) {
          value.add(list(id));
        }
      } else if (url.contains("/lists/") && "DELETE".equals(method)) {
        displayNameById.remove(url.substring(url.lastIndexOf('/') + 1));
      } else if (url.contains("/lists/")) {
        return list(url.substring(url.lastIndexOf('/') + 1));
      } else {
        response.put("id", "site-id");
      }
      return response;
    }
  }

  @Test void aCreatedListIsDiscoveredAndADroppedOneIsNot() throws Exception {
    Map<String, String> lists = new LinkedHashMap<>();
    lists.put("id-1", "Existing");
    String siteUrl = "https://cache-test.example/sites/created-and-dropped";
    MicrosoftGraphListClient writer = new FakeGraph(siteUrl, lists);
    MicrosoftGraphListClient reader = new FakeGraph(siteUrl, lists);

    assertEquals(1, reader.getAvailableLists().size());

    SharePointListMetadata created =
        writer.createList("new_list", Collections.<SharePointColumn>emptyList());
    assertTrue(reader.getAvailableLists().containsKey(created.getListName()),
        "another client of the site sees the list just created");

    writer.deleteList(created.getListId());
    assertFalse(reader.getAvailableLists().containsKey(created.getListName()),
        "another client of the site no longer sees the list just dropped");
    assertEquals(1, reader.getAvailableLists().size());
  }

  @Test void aListDroppedWhileTheListsAreReadIsLeftOut() throws Exception {
    Map<String, String> lists = new LinkedHashMap<>();
    lists.put("id-1", "Existing");
    lists.put("id-2", "Just Dropped");
    FakeGraph client =
        new FakeGraph("https://cache-test.example/sites/dropped-while-read", lists);
    client.droppedButListed = "id-2";

    assertEquals(Collections.singleton("existing"), client.getAvailableLists().keySet());
  }
}
