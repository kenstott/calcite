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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DatasetRegistryTest {
  @Test void expandsFromReferenceAsLeadingCte() {
    DatasetRegistry r = new DatasetRegistry();
    r.define("panel", "SELECT a, b FROM econ.t;");
    assertEquals("WITH panel AS (SELECT a, b FROM econ.t) SELECT * FROM panel",
        r.expand("SELECT * FROM panel"));
  }

  @Test void mergesIntoCallersWithClause() {
    DatasetRegistry r = new DatasetRegistry();
    r.define("panel", "SELECT 1 AS a");
    assertEquals("WITH panel AS (SELECT 1 AS a), x AS (SELECT * FROM panel) SELECT * FROM x",
        r.expand("WITH x AS (SELECT * FROM panel) SELECT * FROM x"));
  }

  @Test void leavesUnrelatedSqlAndColumnsAlone() {
    DatasetRegistry r = new DatasetRegistry();
    r.define("panel", "SELECT 1 AS a");
    String sql = "SELECT panel FROM econ.t WHERE note = 'FROM panel' AND s.panel = 1";
    assertEquals(sql, r.expand(sql));
  }

  @Test void commaJoinAndNestedDatasets() {
    DatasetRegistry r = new DatasetRegistry();
    r.define("a", "SELECT 1 AS k");
    r.define("b", "SELECT k FROM a");
    assertEquals("WITH a AS (SELECT 1 AS k), b AS (WITH a AS (SELECT 1 AS k) SELECT k FROM a) "
        + "SELECT * FROM a, b", r.expand("SELECT * FROM a, b"));
  }

  @Test void callerCteOfSameNameWins() {
    DatasetRegistry r = new DatasetRegistry();
    r.define("panel", "SELECT 1 AS a");
    String sql = "WITH panel AS (SELECT 2 AS a) SELECT * FROM panel";
    assertEquals(sql, r.expand(sql));
  }

  @Test void rejectsBadNameAndNonSelect() {
    DatasetRegistry r = new DatasetRegistry();
    assertThrows(IllegalArgumentException.class, () -> r.define("1bad", "SELECT 1"));
    assertThrows(IllegalArgumentException.class, () -> r.define("ok", "DROP TABLE x"));
  }
}
