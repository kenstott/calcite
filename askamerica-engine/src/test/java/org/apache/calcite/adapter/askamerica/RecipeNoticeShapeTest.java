/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecipeNoticeShapeTest {
  @Test void singleSchemaRankingDoesNotFire() {
    assertFalse(McpServer.isMultiStepShape(
        "SELECT u.unit_name, SUM(v.recreation_visits) s FROM lands.nps_visitation v "
            + "JOIN lands.nps_units u ON v.unit_code = u.unit_code "
            + "GROUP BY u.unit_name ORDER BY s DESC LIMIT 10"));
  }

  @Test void singleTableDescriptiveAggregateDoesNotFire() {
    assertFalse(McpServer.isMultiStepShape(
        "SELECT river, COUNT(*), MIN(lock_year), AVG(chamber_length), MAX(chamber_width) "
            + "FROM transport.usace_locks GROUP BY river"));
  }

  @Test void crossSchemaJoinFires() {
    assertTrue(McpServer.isMultiStepShape(
        "SELECT a.x FROM econ.t a JOIN census.u b ON a.k = b.k"));
  }

  @Test void ratioFires() {
    assertTrue(McpServer.isMultiStepShape(
        "SELECT SUM(a) / SUM(b) FROM econ.t"));
  }

  @Test void correlationFires() {
    assertTrue(McpServer.isMultiStepShape("SELECT CORR(a, b) FROM econ.t"));
  }

  @Test void groupByYearFires() {
    assertTrue(McpServer.isMultiStepShape(
        "SELECT fire_year, COUNT(*) FROM disasters.t GROUP BY fire_year ORDER BY fire_year"));
  }

  @Test void plainSelectDoesNotFire() {
    assertFalse(McpServer.isMultiStepShape("SELECT * FROM econ.t WHERE k = 1"));
  }
}
