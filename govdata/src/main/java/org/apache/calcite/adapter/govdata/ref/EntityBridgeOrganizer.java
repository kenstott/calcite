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
package org.apache.calcite.adapter.govdata.ref;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Orchestrates cross-schema entity resolution as a standalone post-ETL job.
 *
 * <p>Runs AFTER all daily ETL (every source already materialized), BEFORE vss-local.sh
 * (embedding stage). Builds entity bridges linking free-text org/individual names across 9
 * AskAmerica schemas to GLEIF LEI / SEC EIN hubs. See entity-resolution-plan.md (repo root)
 * for the full source registry and matching algorithm.
 *
 * <p>All four entity tables ({@code entity_org_bridge}, {@code entity_person_bridge},
 * {@code canonical_org_entity}, {@code canonical_person_entity}) are built in a single pass,
 * with gleif_entities and gleif_cik_mapping (pre-materialized as ref schema tables) as input.
 *
 * <p><b>Not wired into any schema's {@code hooks.tableLifecycleListener} -- this is a standalone
 * job, invoked only via {@link #main}/{@link #sweep} by {@code x-schema.sh} on its own schedule.
 * </b> Per the cross-schema separation-of-concerns principle (schema ETL runs operate only on
 * self-contained elements; cross-schema derivations run in one separate job after daily ETL).
 */
public class EntityBridgeOrganizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityBridgeOrganizer.class);

  public static void main(String[] args) throws Exception {
    String jdbcUrl = System.getenv("CALCITE_TRACKER_PG_URL");
    if (jdbcUrl == null) {
      throw new IllegalStateException("CALCITE_TRACKER_PG_URL not set");
    }
    String user = System.getenv("CALCITE_TRACKER_PG_USER");
    String password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
    String parquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (parquetDir == null) {
      parquetDir = "s3://govdata-parquet-v1";
    }

    try (Connection pgConn = openPgConnection(jdbcUrl, user, password)) {
      sweep(pgConn, parquetDir);
    }
  }

  /**
   * Builds entity bridges and canonical entities by delegating to EntityBridgeListener.
   * The listener's logic runs standalone here rather than as a schema lifecycle hook.
   */
  public static void sweep(Connection pgConn, String parquetDir) throws Exception {
    LOGGER.info("[entity-bridge] sweeping entity resolution across all schemas");
    EntityBridgeListener listener = new EntityBridgeListener();
    listener.buildBridges(pgConn, parquetDir);
    LOGGER.info("[entity-bridge] entity resolution complete");
  }

  private static Connection openPgConnection(String jdbcUrl, String user, String password)
      throws Exception {
    Class.forName("org.postgresql.Driver");
    if (user != null && password != null) {
      return DriverManager.getConnection(jdbcUrl, user, password);
    }
    return DriverManager.getConnection(jdbcUrl);
  }
}
