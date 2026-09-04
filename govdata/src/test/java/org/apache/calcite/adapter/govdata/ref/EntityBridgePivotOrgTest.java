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
package org.apache.calcite.adapter.govdata.ref;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@code canonical_org_entity}'s grain: one row per source mention, per
 * entity-resolution-plan.md. Folding an org's several source keys into one row makes the ones
 * not chosen unreachable — the failure this covers.
 */
@Tag("unit")
class EntityBridgePivotOrgTest {

  /** Reflectively builds the private OrgSource registry entry; only canonicalColumn matters here. */
  private static Object orgSource(String canonicalColumn) throws Exception {
    Class<?> cls = Class.forName(
        "org.apache.calcite.adapter.govdata.ref.EntityBridgeListener$OrgSource");
    Constructor<?> c = cls.getDeclaredConstructors()[0];
    c.setAccessible(true);
    return c.newInstance("s", "t", null, "n", "k", null, "col", null, null, canonicalColumn);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> pivot(Connection conn, String... canonicalColumns)
      throws Exception {
    List<Object> sources = new ArrayList<Object>();
    for (String c : canonicalColumns) {
      sources.add(orgSource(c));
    }
    Class<?> cls = Class.forName(
        "org.apache.calcite.adapter.govdata.ref.EntityBridgeListener$PivotOrgIterator");
    Constructor<?> ctor = cls.getDeclaredConstructors()[0];
    ctor.setAccessible(true);
    Object it = ctor.newInstance(conn, sources);
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    Iterator<Map<String, Object>> iter = (Iterator<Map<String, Object>>) it;
    while (iter.hasNext()) {
      out.add(iter.next());
    }
    return out;
  }

  private static Connection mentions(String... valuesRows) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE all_org_mentions ("
          + "canonical_entity_id VARCHAR, canonical_column VARCHAR, source_key VARCHAR, "
          + "match_confidence VARCHAR, lei VARCHAR, sec_cik VARCHAR, gleif_legal_name VARCHAR, "
          + "name_raw VARCHAR, support_count BIGINT)");
      for (String row : valuesRows) {
        st.execute("INSERT INTO all_org_mentions VALUES " + row);
      }
    }
    return conn;
  }

  /**
   * The core case: one company, three patent assignee_ids. All three must survive, each on its
   * own row, sharing the org's identity — not collapsed to whichever has the most patents.
   */
  @Test void emitsOneRowPerSourceMention() throws Exception {
    try (Connection conn = mentions(
        "('E1','patents_assignee_id','A-1','high','LEI1',NULL,'Schlumberger','Schlumberger',10227)",
        "('E1','patents_assignee_id','A-2','high','LEI1',NULL,'Schlumberger','Schlumberger',1)",
        "('E1','patents_assignee_id','A-3','high','LEI1',NULL,'Schlumberger','Schlumberger',44)")) {
      List<Map<String, Object>> rows = pivot(conn, "patents_assignee_id");

      assertEquals(3, rows.size(), "three assignee_ids must yield three rows, not one");
      Set<String> ids = new HashSet<String>();
      for (Map<String, Object> r : rows) {
        ids.add((String) r.get("patents_assignee_id"));
        assertEquals("E1", r.get("canonical_entity_id"), "identity repeats on every row");
        assertEquals("LEI1", r.get("lei"));
        assertEquals("Schlumberger", r.get("canonical_name"));
      }
      assertTrue(ids.contains("A-1") && ids.contains("A-2") && ids.contains("A-3"),
          "every assignee_id must be reachable, got " + ids);
    }
  }

  /**
   * A mention row carries the org's resolved identity alongside its own source key, so a
   * cross-source predicate (sec_cik IS NOT NULL AND patents_assignee_id IS NOT NULL) still
   * matches on one row under this grain.
   */
  @Test void mentionRowCarriesResolvedIdentityAlongsideItsOwnKey() throws Exception {
    // sec_cik is an identity column resolved across the org, never a per-source canonical
    // column (the registry carries sec_manager_cik / sec_reporting_person_cik, not bare
    // sec_cik), so it is supplied on the mention rows rather than declared as a source.
    try (Connection conn = mentions(
        "('E2','fec_committee_id','C-1','high',NULL,'0000320193','Apple','Apple',5)",
        "('E2','patents_assignee_id','A-9','high',NULL,'0000320193','Apple','Apple',7)")) {
      List<Map<String, Object>> rows = pivot(conn, "patents_assignee_id", "fec_committee_id");

      assertEquals(2, rows.size());
      int matches = 0;
      for (Map<String, Object> r : rows) {
        if (r.get("sec_cik") != null && r.get("patents_assignee_id") != null) {
          matches++;
        }
      }
      assertEquals(1, matches,
          "the patents mention row must carry sec_cik too, so the AND predicate still matches");
    }
  }

  /** The same source key seen twice is one mention — it must not multiply rows. */
  @Test void deduplicatesARepeatedSourceKey() throws Exception {
    try (Connection conn = mentions(
        "('E3','patents_assignee_id','A-1','low','LEI3',NULL,'Acme','Acme',1)",
        "('E3','patents_assignee_id','A-1','high','LEI3',NULL,'Acme','Acme',99)")) {
      List<Map<String, Object>> rows = pivot(conn, "patents_assignee_id");

      assertEquals(1, rows.size(), "a repeated key is one mention");
      assertEquals("A-1", rows.get(0).get("patents_assignee_id"));
      assertEquals("high", rows.get(0).get("patents_assignee_id_confidence"),
          "the higher-support duplicate wins");
    }
  }

  /** An org with no source-keyed mention still gets exactly one identity row. */
  @Test void emitsAnIdentityRowWhenNoSourceKeyIsPresent() throws Exception {
    try (Connection conn = mentions(
        "('E4',NULL,NULL,NULL,'LEI4',NULL,'Ghost Ltd','Ghost Ltd',0)")) {
      List<Map<String, Object>> rows = pivot(conn, "patents_assignee_id");

      assertEquals(1, rows.size());
      assertEquals("LEI4", rows.get(0).get("lei"));
      assertNull(rows.get(0).get("patents_assignee_id"));
    }
  }

  /** Two orgs stay distinct, and each keeps all of its own mentions. */
  @Test void keepsGroupsSeparate() throws Exception {
    try (Connection conn = mentions(
        "('E5','patents_assignee_id','A-1','high','LEI5',NULL,'Alpha','Alpha',2)",
        "('E5','patents_assignee_id','A-2','high','LEI5',NULL,'Alpha','Alpha',3)",
        "('E6','patents_assignee_id','B-1','high','LEI6',NULL,'Beta','Beta',4)")) {
      List<Map<String, Object>> rows = pivot(conn, "patents_assignee_id");

      assertEquals(3, rows.size());
      int e5 = 0;
      for (Map<String, Object> r : rows) {
        if ("E5".equals(r.get("canonical_entity_id"))) {
          e5++;
        }
      }
      assertEquals(2, e5, "E5 keeps both of its assignee_ids");
    }
  }
}
