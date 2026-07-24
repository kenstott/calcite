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
package org.apache.calcite.adapter.govdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Reads the govdata metadata catalog — schema / table / column names + {@code comment:}
 * descriptions — straight from the {@code *-schema.yaml} resources already baked into
 * this driver jar. No separate catalog artifact and no live schema connection: the
 * catalog is authored in the YAMLs and shipped in the jar, so callers (e.g. the MCP
 * server's discovery tools) get complete metadata instantly.
 *
 * <p>Includes {@code partitionedTables}, {@code tables} and {@code views} — unlike
 * {@link GovDataUtils#loadTableDefinitions} which merges only the first two.
 */
public final class GovDataCatalog {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private GovDataCatalog() {
  }

  /**
   * Classpath resource path for a schema's YAML. Matches the driver convention
   * {@code /<source>/<source>-schema.yaml} with the handful of known exceptions
   * whose file lives beside a sibling schema.
   */
  public static String resourcePath(String schema) {
    switch (schema) {
      case "cyber_threat":
        return "/cyber/cyber-threat-schema.yaml";
      case "cyber_vuln":
        return "/cyber/cyber-vuln-schema.yaml";
      case "econ_reference":
        return "/econ/econ-reference-schema.yaml";
      default:
        return "/" + schema + "/" + schema + "-schema.yaml";
    }
  }

  /**
   * Build the catalog for the given schemas as
   * {@code [{schema, comment?, tables:[{name, type, comment?, columns:[{name, type, nullable, comment?}]}]}]}.
   * Schemas whose YAML resource is missing are skipped.
   */
  public static ArrayNode build(Collection<String> schemas) {
    ArrayNode out = MAPPER.createArrayNode();
    for (String schema : schemas) {
      JsonNode root = read(resourcePath(schema));
      if (root == null) {
        continue;
      }
      ObjectNode sc = MAPPER.createObjectNode();
      sc.put("schema", schema);
      putText(sc, "comment", root.get("comment"));
      ArrayNode tables = MAPPER.createArrayNode();
      Set<String> seen = new HashSet<>();
      addTables(tables, seen, root.get("partitionedTables"), "table");
      addTables(tables, seen, root.get("tables"), "table");
      addTables(tables, seen, root.get("views"), "view");
      sc.set("tables", tables);
      out.add(sc);
    }
    return out;
  }

  private static JsonNode read(String path) {
    try (InputStream is = GovDataCatalog.class.getResourceAsStream(path)) {
      if (is == null) {
        return null;
      }
      return YamlUtils.parseYamlOrJson(is, path);
    } catch (Exception e) {
      return null;
    }
  }

  private static void addTables(ArrayNode out, Set<String> seen, JsonNode arr, String type) {
    if (arr == null || !arr.isArray()) {
      return;
    }
    for (JsonNode t : arr) {
      String name = text(t.get("name"));
      if (name.isEmpty() || !seen.add(name)) {
        continue;
      }
      ObjectNode to = MAPPER.createObjectNode();
      to.put("name", name);
      to.put("type", type);
      putText(to, "comment", t.get("comment"));
      ArrayNode cols = MAPPER.createArrayNode();
      JsonNode columns = t.get("columns");
      if (columns != null && columns.isArray()) {
        for (JsonNode c : columns) {
          ObjectNode co = MAPPER.createObjectNode();
          co.put("name", text(c.get("name")));
          co.put("type", text(c.get("type")));
          co.put("nullable", c.path("nullable").asBoolean(true));
          putText(co, "comment", c.get("comment"));
          cols.add(co);
        }
      }
      to.set("columns", cols);
      out.add(to);
    }
  }

  private static String text(JsonNode n) {
    return (n == null || n.isNull()) ? "" : n.asText("");
  }

  /** Put a whitespace-collapsed comment (YAML block scalars carry newlines) if non-empty. */
  private static void putText(ObjectNode o, String field, JsonNode n) {
    if (n != null && !n.isNull()) {
      String s = n.asText("").replaceAll("\\s+", " ").trim();
      if (!s.isEmpty()) {
        o.put(field, s);
      }
    }
  }
}
