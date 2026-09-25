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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.govdata.GovDataUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Consistency of the law schema's {@code constraints:} block: every key and foreign key column
 * exists, every foreign key targets a declared key, cross-schema targets resolve to real schema
 * names, and the normalized committee_id expression turns a Congress.gov committee code into the
 * form officials.committees uses.
 */
@Tag("unit")
class LawConstraintsTest {

  private static final String RESOURCE = "/law/law-schema.yaml";

  @SuppressWarnings("unchecked")
  private static Map<String, Object> schema() throws Exception {
    try (InputStream in = LawConstraintsTest.class.getResourceAsStream(RESOURCE)) {
      // SnakeYAML, not Jackson: the schema declares columns with YAML merge keys
      // (- <<: *partition_congress), which SnakeYAML expands and Jackson does not.
      // The schema reuses column and partition anchors far more than SnakeYAML's default alias
      // limit (50), so raise it.
      LoaderOptions options = new LoaderOptions();
      options.setMaxAliasesForCollections(100000);
      return new Yaml(new SafeConstructor(options)).load(in);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, List<String>> tableColumns(Map<String, Object> schema) {
    Map<String, List<String>> out = new HashMap<String, List<String>>();
    for (Object t : (List<Object>) schema.get("partitionedTables")) {
      Map<String, Object> table = (Map<String, Object>) t;
      List<String> cols = new java.util.ArrayList<String>();
      for (Object c : (List<Object>) table.get("columns")) {
        cols.add((String) ((Map<String, Object>) c).get("name"));
      }
      out.put((String) table.get("name"), cols);
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  @Test void testEveryKeyAndForeignKeyIsConsistent() throws Exception {
    Map<String, List<String>> tables = tableColumns(schema());
    Map<String, Map<String, Object>> constraints =
        GovDataUtils.loadTableConstraints(LawSchemaFactory.class, RESOURCE);
    assertEquals(tables.keySet(), constraints.keySet(), "every table has a constraints entry");

    Set<String> externalSchemas = new HashSet<String>(Arrays.asList("officials", "geo", "ref"));
    int foreignKeys = 0;
    for (Map.Entry<String, Map<String, Object>> e : constraints.entrySet()) {
      String table = e.getKey();
      List<String> pk = (List<String>) e.getValue().get("primaryKey");
      if (pk != null) {
        assertTrue(tables.get(table).containsAll(pk), table + " primaryKey columns exist");
      }
      List<Map<String, Object>> fks = (List<Map<String, Object>>) e.getValue().get("foreignKeys");
      if (fks == null) {
        continue;
      }
      for (Map<String, Object> fk : fks) {
        foreignKeys++;
        List<String> cols = (List<String>) fk.get("columns");
        List<String> targetCols = (List<String>) fk.get("targetColumns");
        assertTrue(tables.get(table).containsAll(cols), table + " fk columns exist: " + cols);
        assertEquals(cols.size(), targetCols.size(), table + " fk arity");
        String targetSchema = (String) fk.get("targetSchema");
        if (targetSchema == null) {
          String target = (String) fk.get("targetTable");
          assertTrue(tables.containsKey(target), table + " targets unknown table " + target);
          List<String> targetPk = (List<String>) constraints.get(target).get("primaryKey");
          assertEquals(targetCols, targetPk, table + " fk must target the primary key of " + target);
        } else {
          assertFalse(targetSchema.contains("${"), "variable resolved: " + targetSchema);
          assertTrue(externalSchemas.contains(targetSchema), "known schema: " + targetSchema);
        }
      }
    }
    assertTrue(foreignKeys > 60, "expected the full set of foreign keys, found " + foreignKeys);
  }

  @SuppressWarnings("unchecked")
  private static String committeeExpression() throws Exception {
    for (Object t : (List<Object>) schema().get("partitionedTables")) {
      Map<String, Object> table = (Map<String, Object>) t;
      if ("bill_committees".equals(table.get("name"))) {
        for (Object c : (List<Object>) table.get("columns")) {
          Map<String, Object> col = (Map<String, Object>) c;
          if ("committee_id".equals(col.get("name"))) {
            return (String) col.get("expression");
          }
        }
      }
    }
    throw new AssertionError("bill_committees.committee_id has no expression");
  }

  private static String eval(String expression, String code) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      String source = code == null ? "NULL" : "'" + code + "'";
      try (ResultSet rs = st.executeQuery("SELECT " + expression + " FROM (SELECT " + source
          + " AS committee_system_code) src")) {
        rs.next();
        return rs.getString(1);
      }
    }
  }

  @Test void testCommitteeIdExpressionMatchesTheOfficialsForm() throws Exception {
    String expr = committeeExpression();
    assertEquals("HSJU", eval(expr, "hsju00"));
    assertEquals("SSFI", eval(expr, "ssfi00"));
    assertEquals("HSIF14", eval(expr, "hsif14"));
    assertEquals("SSAP22", eval(expr, "ssap22"));
    assertNull(eval(expr, null));
  }
}
