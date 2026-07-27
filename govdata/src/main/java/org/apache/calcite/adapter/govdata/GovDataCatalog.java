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
      // The schema-level lag is the ceiling for any partitioned table that does not
      // declare a year range of its own — the same value GovDataUtils.getEndYear reads.
      JsonNode schemaLag = root.get("dataLagYears");
      addTables(tables, seen, root.get("partitionedTables"), "table", schemaLag);
      addTables(tables, seen, root.get("tables"), "table", null);
      addTables(tables, seen, root.get("views"), "view", null);
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

  private static void addTables(ArrayNode out, Set<String> seen, JsonNode arr, String type,
      JsonNode schemaLag) {
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
      putCoverage(to, t, schemaLag);
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

  /**
   * Copy the table's declared {@code year} bounds as a {@code coverage} node.
   *
   * <p>Emitted verbatim from the YAML — {@code start} may still hold an unresolved
   * {@code ${VAR:default}} reference, and {@code end} may be the literal
   * {@code "current"}. Callers resolve those against the running environment; keeping
   * this layer declarative means the catalog states what the schema author wrote, not
   * what one particular JVM computed.
   *
   * <p>The schemas declare a year three different ways, and all three are real coverage:
   * a {@code yearRange} dimension, an explicit list of years, or nothing but a
   * {@code year} hive partition column — the last meaning the table rides the global
   * start/end with the schema's own {@code dataLagYears} as its ceiling. A table with no
   * year in any of those positions gets no coverage node, which is the honest answer for
   * the ~83 partitioned by something other than time.
   */
  private static void putCoverage(ObjectNode to, JsonNode t, JsonNode schemaLag) {
    JsonNode year = t.path("dimensions").path("year");

    if (year.isObject() && "yearRange".equals(text(year.get("type")))) {
      ObjectNode cov = MAPPER.createObjectNode();
      cov.put("column", "year");
      cov.put("form", "yearRange");
      putText(cov, "start", year.get("start"));
      putText(cov, "end", year.get("end"));
      putInt(cov, "minYear", year.get("minYear"));
      putInt(cov, "maxYear", year.get("maxYear"));
      // A dimension without its own lag still inherits the schema's.
      putInt(cov, "dataLag", year.hasNonNull("dataLag") ? year.get("dataLag") : schemaLag);
      to.set("coverage", cov);
      return;
    }

    // An explicit list of years is the tightest declaration there is — both bounds are
    // stated outright, so take the ends verbatim and let the caller resolve them.
    if (year.isArray() && year.size() > 0) {
      ObjectNode cov = MAPPER.createObjectNode();
      cov.put("column", "year");
      cov.put("form", "list");
      putText(cov, "start", year.get(0));
      putText(cov, "end", year.get(year.size() - 1));
      to.set("coverage", cov);
      return;
    }

    if (hasYearPartitionColumn(t)) {
      ObjectNode cov = MAPPER.createObjectNode();
      cov.put("column", "year");
      cov.put("form", "partitionColumn");
      // No floor is declared anywhere for these — the global start governs, and the
      // caller omits first_year rather than inventing one. The ceiling is real.
      putInt(cov, "dataLag", schemaLag);
      to.set("coverage", cov);
    }
  }

  private static boolean hasYearPartitionColumn(JsonNode t) {
    for (JsonNode c : t.path("partitions").path("columnDefinitions")) {
      if ("year".equalsIgnoreCase(text(c.get("name")))) {
        return true;
      }
    }
    return false;
  }

  private static void putInt(ObjectNode o, String field, JsonNode n) {
    if (n != null && n.isNumber()) {
      o.put(field, n.intValue());
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
