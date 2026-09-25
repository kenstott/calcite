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

import org.apache.calcite.adapter.file.partition.PGPipelineTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * One-shot repair for {@code vc_staging} rows written before {@link ChunkOrganizer} populated the
 * per-source FK columns: fills them from {@code stringified_fk} (see {@link
 * ChunkOrganizer#backfillForeignKeys}). The set of sources and their FK columns comes from
 * ChunkOrganizer's registry, so this stays in sync with it.
 *
 * <p>Usage: {@code ChunkFkBackfill [--apply] [--source-table <table>] [--batch-size <n>]}.
 * Without {@code --apply} it only reports what would change. Targets the Postgres namespace
 * derived from {@code GOVDATA_PARQUET_DIR}, the same way {@link ChunkOrganizer#main} does, and
 * creates any missing FK columns first (the idempotent {@link ChunkOrganizer#ensureVcSchema}).
 */
public class ChunkFkBackfill {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkFkBackfill.class);

  private static final int DEFAULT_BATCH_SIZE = 50_000;

  public static void main(String[] args) throws Exception {
    boolean apply = false;
    String sourceTable = null;
    int batchSize = DEFAULT_BATCH_SIZE;
    for (int i = 0; i < args.length; i++) {
      if ("--apply".equals(args[i])) {
        apply = true;
      } else if ("--source-table".equals(args[i]) && i + 1 < args.length) {
        sourceTable = args[++i];
      } else if ("--batch-size".equals(args[i]) && i + 1 < args.length) {
        batchSize = Integer.parseInt(args[++i]);
      } else {
        throw new IllegalArgumentException("unknown or incomplete argument '" + args[i]
            + "'; usage: ChunkFkBackfill [--apply] [--source-table <table>] [--batch-size <n>]");
      }
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("--batch-size must be positive, got " + batchSize);
    }
    String jdbcUrl = System.getenv("CALCITE_TRACKER_PG_URL");
    if (jdbcUrl == null) {
      throw new IllegalStateException("CALCITE_TRACKER_PG_URL not set");
    }
    String user = System.getenv("CALCITE_TRACKER_PG_USER");
    String password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
    String base = System.getenv("GOVDATA_PARQUET_DIR");
    if (base == null) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR not set -- it selects the vc_staging "
          + "namespace, and an unset value must not silently mean production");
    }
    String ns = PGPipelineTracker.sanitizeNamespace(base);
    if (ns == null) {
      throw new IllegalStateException("cannot derive a PG namespace from '" + base + "'");
    }
    LOGGER.info("ChunkFkBackfill: namespace={}, mode={}, source-table={}, batch-size={}", ns,
        apply ? "APPLY" : "dry-run", sourceTable == null ? "(all)" : sourceTable, batchSize);
    try (Connection pg = user != null ? DriverManager.getConnection(jdbcUrl, user, password)
        : DriverManager.getConnection(jdbcUrl)) {
      pg.setAutoCommit(false);
      try (Statement stmt = pg.createStatement()) {
        stmt.execute("SET search_path TO \"" + ns + "\"");
      }
      ChunkOrganizer.ensureVcSchema(pg);
      pg.commit();
      long rows = ChunkOrganizer.backfillForeignKeys(pg, sourceTable, apply, batchSize);
      LOGGER.info("ChunkFkBackfill: {} {} row(s)", apply ? "updated" : "would update", rows);
    }
  }
}
