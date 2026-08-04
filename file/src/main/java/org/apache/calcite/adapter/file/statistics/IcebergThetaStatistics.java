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
package org.apache.calcite.adapter.file.statistics;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;

import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Per-column distinct-value sketches published as Iceberg Puffin statistics.
 *
 * <p>Iceberg manifests already carry bounds, value counts and null counts — {@code
 * iceberg_column_stats} reads them straight from metadata in milliseconds. The one thing they do
 * not carry is cardinality, and Iceberg's slot for that is a Puffin blob referenced from table
 * metadata as a {@link StatisticsFile}. Writing it there means the statistics travel with the
 * table, version with the snapshot that produced them, and are readable by anything that speaks
 * Iceberg — no side database, no second service to reach, no credentials beyond the data.
 *
 * <p>Sketches are accumulated while rows are written rather than by scanning afterwards. The
 * values are already materialized in the writer at that point, so updating a sketch is O(1) per
 * value and costs no additional I/O; a post-hoc pass would re-read the whole table. It also means
 * the sketch describes exactly the rows that were committed.
 *
 * <p>Sketches are built from real values. An NDV computed elsewhere cannot be turned back into a
 * valid theta sketch, and emitting a blob typed {@code apache-datasketches-theta-v1} whose payload
 * is not one would fail in another engine at read time — long after the snapshot committed —
 * rather than here. The blob also carries the spec's {@code ndv} property, so a reader that only
 * wants the number never has to deserialize the sketch.
 *
 * <p>Advisory: statistics never gate a commit. Every failure path logs and leaves the table
 * without a statistics file, which puts the planner back on its default estimates.
 */
public final class IcebergThetaStatistics {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergThetaStatistics.class);

  /** Spec property name a reader consults for the cardinality without parsing the payload. */
  private static final String NDV_PROPERTY = "ndv";

  private final Map<String, UpdateSketch> sketches = new LinkedHashMap<String, UpdateSketch>();

  /**
   * Folds one row's values into the per-column sketches.
   *
   * <p>Nulls are skipped: a null is the absence of a value, not a distinct one, and Iceberg
   * already reports null counts separately from the manifests.
   */
  public void addRow(Map<String, Object> row) {
    if (row == null) {
      return;
    }
    for (Map.Entry<String, Object> e : row.entrySet()) {
      Object v = e.getValue();
      if (v == null) {
        continue;
      }
      UpdateSketch s = sketches.get(e.getKey());
      if (s == null) {
        s = UpdateSketch.builder().build();
        sketches.put(e.getKey(), s);
      }
      // Hash the rendered value so every column type funnels through one update path. Distinctness
      // is judged on that rendering, which is what a cardinality estimate for a column means here.
      s.update(v.toString());
    }
  }

  public boolean isEmpty() {
    return sketches.isEmpty();
  }

  /** Cardinality estimate per column, for logging or direct use. */
  public Map<String, Long> estimates() {
    Map<String, Long> out = new LinkedHashMap<String, Long>();
    for (Map.Entry<String, UpdateSketch> e : sketches.entrySet()) {
      out.put(e.getKey(), (long) e.getValue().getEstimate());
    }
    return out;
  }

  /**
   * Serializes the accumulated sketches into a Puffin file and registers it against the table's
   * current snapshot.
   *
   * <p>Field IDs are resolved from the table schema; a column the schema does not know is skipped
   * rather than guessed, since a blob pointing at the wrong field would misinform every reader.
   */
  public void commit(Table table, String statisticsPath) {
    if (sketches.isEmpty() || table == null) {
      return;
    }
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return;   // nothing committed to describe
    }
    try {
      OutputFile out = table.io().newOutputFile(statisticsPath);
      List<BlobMetadata> written = new ArrayList<BlobMetadata>();
      long fileSize;
      long footerSize;
      try (PuffinWriter writer = Puffin.write(out)
          .createdBy("calcite-file-adapter")
          .build()) {
        for (Map.Entry<String, UpdateSketch> e : sketches.entrySet()) {
          org.apache.iceberg.types.Types.NestedField field =
              table.schema().findField(e.getKey().toLowerCase(Locale.ROOT));
          if (field == null) {
            continue;
          }
          CompactSketch compact = e.getValue().compact();
          Map<String, String> props = new LinkedHashMap<String, String>();
          props.put(NDV_PROPERTY, Long.toString((long) compact.getEstimate()));
          writer.add(
              new Blob(
                  StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                  Collections.singletonList(field.fieldId()),
                  snapshot.snapshotId(),
                  snapshot.sequenceNumber(),
                  ByteBuffer.wrap(compact.toByteArray()),
                  null,
                  props));
        }
        writer.finish();
        for (org.apache.iceberg.puffin.BlobMetadata pm : writer.writtenBlobsMetadata()) {
          written.add(GenericBlobMetadata.from(pm));
        }
        fileSize = writer.fileSize();
        footerSize = writer.footerSize();
      }
      if (written.isEmpty()) {
        return;
      }
      StatisticsFile statsFile =
          new GenericStatisticsFile(snapshot.snapshotId(), statisticsPath, fileSize, footerSize,
              written);
      table.updateStatistics().setStatistics(snapshot.snapshotId(), statsFile).commit();
      LOGGER.info("Published Iceberg column statistics: {} sketches for snapshot {} at {}",
          written.size(), snapshot.snapshotId(), statisticsPath);
    } catch (Exception e) {
      // Never fail the data commit for statistics.
      LOGGER.warn("Could not publish Iceberg column statistics: {}", e.toString());
    }
  }

  /**
   * Reads published cardinalities for a table's current snapshot.
   *
   * <p>Reads only the {@code ndv} property from blob metadata — the Puffin footer — so it never
   * downloads or deserializes a sketch payload. Returns an empty map when the table has no
   * statistics file, which is the normal state for a table written before statistics existed.
   */
  public static Map<String, Long> readNdv(Table table) {
    Map<String, Long> out = new LinkedHashMap<String, Long>();
    if (table == null || table.currentSnapshot() == null) {
      return out;
    }
    long snapshotId = table.currentSnapshot().snapshotId();
    try {
      for (StatisticsFile sf : table.statisticsFiles()) {
        if (sf.snapshotId() != snapshotId) {
          // Statistics describe the snapshot they were computed from. Reading an older one would
          // report cardinalities for data the query will not see.
          continue;
        }
        for (BlobMetadata blob : sf.blobMetadata()) {
          if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())) {
            continue;
          }
          String ndv = blob.properties() == null ? null : blob.properties().get(NDV_PROPERTY);
          if (ndv == null || blob.fields().size() != 1) {
            continue;
          }
          org.apache.iceberg.types.Types.NestedField field =
              table.schema().findField(blob.fields().get(0));
          if (field != null) {
            out.put(field.name().toLowerCase(Locale.ROOT), Long.parseLong(ndv));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Could not read Iceberg column statistics: {}", e.toString());
      out.clear();
    }
    return out;
  }
}
