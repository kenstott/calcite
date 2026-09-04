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

  /** Partition key -> column -> sketch of the values this run wrote into that partition. */
  private final Map<String, Map<String, UpdateSketch>> byPartition =
      new LinkedHashMap<String, Map<String, UpdateSketch>>();

  /** Property naming the partition a blob describes, so untouched ones can be carried forward. */
  private static final String PARTITION_PROPERTY = "calcite.partition";

  /**
   * Folds one row's values into the per-column sketches.
   *
   * <p>Nulls are skipped: a null is the absence of a value, not a distinct one, and Iceberg
   * already reports null counts separately from the manifests.
   */
  public void addRow(String partitionKey, Map<String, Object> row) {
    if (row == null) {
      return;
    }
    String pk = partitionKey == null ? "" : partitionKey;
    Map<String, UpdateSketch> sketches = byPartition.get(pk);
    if (sketches == null) {
      sketches = new LinkedHashMap<String, UpdateSketch>();
      byPartition.put(pk, sketches);
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
    return byPartition.isEmpty();
  }

  /** Cardinality estimate per column, for logging or direct use. */
  public Map<String, Long> estimates() {
    Map<String, org.apache.datasketches.theta.Union> unions =
        new LinkedHashMap<String, org.apache.datasketches.theta.Union>();
    for (Map<String, UpdateSketch> perColumn : byPartition.values()) {
      for (Map.Entry<String, UpdateSketch> e : perColumn.entrySet()) {
        org.apache.datasketches.theta.Union u = unions.get(e.getKey());
        if (u == null) {
          u = org.apache.datasketches.theta.SetOperation.builder().buildUnion();
          unions.put(e.getKey(), u);
        }
        u.union(e.getValue().compact());
      }
    }
    Map<String, Long> out = new LinkedHashMap<String, Long>();
    for (Map.Entry<String, org.apache.datasketches.theta.Union> e : unions.entrySet()) {
      out.put(e.getKey(), (long) e.getValue().getResult().getEstimate());
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
    if (byPartition.isEmpty() || table == null) {
      return;
    }
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return;
    }
    try {
      // Carry forward sketches for partitions this run did not touch. A run normally writes a
      // slice, so most of the table's cardinality lives in blobs written by earlier runs; dropping
      // them would report the slice as the whole table. Partitions this run DID write are
      // superseded rather than merged — overwritePartitions replaces the data, and a theta union
      // cannot subtract the values that went away.
      List<Carried> carried = readCarryForward(table, byPartition.keySet());

      OutputFile out = table.io().newOutputFile(statisticsPath);
      List<BlobMetadata> written = new ArrayList<BlobMetadata>();
      long fileSize;
      long footerSize;
      try (PuffinWriter writer = Puffin.write(out).createdBy("calcite-file-adapter").build()) {
        for (Carried c : carried) {
          writer.add(new Blob(c.type, c.fields, snapshot.snapshotId(), snapshot.sequenceNumber(),
              c.payload, null, c.properties));
        }
        for (Map.Entry<String, Map<String, UpdateSketch>> part : byPartition.entrySet()) {
          for (Map.Entry<String, UpdateSketch> e : part.getValue().entrySet()) {
            org.apache.iceberg.types.Types.NestedField field =
                table.schema().findField(e.getKey().toLowerCase(Locale.ROOT));
            if (field == null) {
              continue;
            }
            CompactSketch compact = e.getValue().compact();
            Map<String, String> props = new LinkedHashMap<String, String>();
            props.put(NDV_PROPERTY, Long.toString((long) compact.getEstimate()));
            props.put(PARTITION_PROPERTY, part.getKey());
            writer.add(
                new Blob(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    Collections.singletonList(field.fieldId()),
                    snapshot.snapshotId(), snapshot.sequenceNumber(),
                    ByteBuffer.wrap(compact.toByteArray()), null, props));
          }
        }
        // Roll the per-partition sketches up and publish the answer as a plain number, so the
        // READ path never has to deserialize a sketch. Reading otherwise pulls in
        // datasketches-memory, whose static initializer rejects any JDK past 21 with an
        // ExceptionInInitializerError — an Error, not an Exception, so it would escape the
        // read-path guards and surface during query planning on a client we do not control. The
        // sketches stay on disk purely so the NEXT write can merge with them; queries only need
        // the total.
        Map<Integer, org.apache.datasketches.theta.Union> rollup =
            new LinkedHashMap<Integer, org.apache.datasketches.theta.Union>();
        for (Carried c : carried) {
          if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(c.type)
              || c.fields.size() != 1 || c.payload.remaining() == 0) {
            continue;
          }
          byte[] bytes = new byte[c.payload.remaining()];
          c.payload.duplicate().get(bytes);
          unionInto(rollup, c.fields.get(0), bytes);
        }
        for (Map.Entry<String, Map<String, UpdateSketch>> part : byPartition.entrySet()) {
          for (Map.Entry<String, UpdateSketch> e : part.getValue().entrySet()) {
            org.apache.iceberg.types.Types.NestedField field =
                table.schema().findField(e.getKey().toLowerCase(Locale.ROOT));
            if (field != null) {
              unionInto(rollup, field.fieldId(), e.getValue().compact().toByteArray());
            }
          }
        }
        for (Map.Entry<Integer, org.apache.datasketches.theta.Union> e : rollup.entrySet()) {
          Map<String, String> props = new LinkedHashMap<String, String>();
          props.put(NDV_PROPERTY, Long.toString((long) e.getValue().getResult().getEstimate()));
          writer.add(
              new Blob(NDV_BLOB_TYPE, Collections.singletonList(e.getKey()),
                  snapshot.snapshotId(), snapshot.sequenceNumber(),
                  ByteBuffer.wrap(new byte[0]), null, props));
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
      // The new file already carries forward every blob worth keeping (see above), so every
      // older statistics file is now a pure redundant subset. Removing them keeps
      // table.statisticsFiles() at size 1 - otherwise it grows one entry per commit forever,
      // and readCarryForward's next run re-reads all of them, compounding without bound.
      org.apache.iceberg.UpdateStatistics update =
          table.updateStatistics().setStatistics(snapshot.snapshotId(), statsFile);
      for (StatisticsFile old : table.statisticsFiles()) {
        if (old.snapshotId() != snapshot.snapshotId()) {
          update.removeStatistics(old.snapshotId());
        }
      }
      // A statistics commit moves the catalog's version-hint pointer like any other metadata
      // commit, and this one runs right after compaction/expire-snapshots — exactly the
      // interleaving CrossProcessCommitLock exists to prevent.
      org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock.runExclusive(
          table.location(), update::commit);
      LOGGER.info("Published column statistics: {} blobs ({} carried forward, {} partitions "
          + "written) for snapshot {}", written.size(), carried.size(), byPartition.size(),
          snapshot.snapshotId());
    } catch (Exception e) {
      LOGGER.warn("Could not publish Iceberg column statistics", e);
    }
  }

  private static void unionInto(Map<Integer, org.apache.datasketches.theta.Union> unions,
      int fieldId, byte[] sketchBytes) {
    org.apache.datasketches.theta.Union u = unions.get(fieldId);
    if (u == null) {
      u = org.apache.datasketches.theta.SetOperation.builder().buildUnion();
      unions.put(fieldId, u);
    }
    u.union(org.apache.datasketches.theta.Sketches.wrapCompactSketch(
        org.apache.datasketches.memory.Memory.wrap(sketchBytes)));
  }

  /** A blob copied unchanged from the previous statistics file. */
  private static final class Carried {
    String type;
    List<Integer> fields;
    ByteBuffer payload;
    Map<String, String> properties;
  }

  /**
   * Reads back blobs describing partitions this run did not write.
   *
   * <p>Payloads must be physically re-read: a Puffin file is immutable, so the new one has to
   * contain every blob that should survive.
   */
  private static List<Carried> readCarryForward(Table table, java.util.Set<String> superseded) {
    List<Carried> out = new ArrayList<Carried>();
    if (table.currentSnapshot() == null) {
      return out;
    }
    // Every statistics file this class writes is already a complete carry-forward of every file
    // before it (see commit() below), so only the most recent one can hold anything not already
    // superseded. Reading every historical file here re-reads that same carried-forward data once
    // per file it was ever copied into - a table with N statistics files re-reads O(N) redundant
    // copies of the same blobs, which is what exhausted the heap on a table with many commits.
    StatisticsFile latest = null;
    long latestSeq = Long.MIN_VALUE;
    for (StatisticsFile sf : table.statisticsFiles()) {
      Snapshot sn = table.snapshot(sf.snapshotId());
      long seq = sn != null ? sn.sequenceNumber() : Long.MIN_VALUE;
      if (latest == null || seq > latestSeq) {
        latest = sf;
        latestSeq = seq;
      }
    }
    if (latest == null) {
      return out;
    }
    try (org.apache.iceberg.puffin.PuffinReader reader =
             Puffin.read(table.io().newInputFile(latest.path()))
                 .withFileSize(latest.fileSizeInBytes())
                 .withFooterSize(latest.fileFooterSizeInBytes())
                 .build()) {
      List<org.apache.iceberg.puffin.BlobMetadata> wanted =
          new ArrayList<org.apache.iceberg.puffin.BlobMetadata>();
      for (org.apache.iceberg.puffin.BlobMetadata bm : reader.fileMetadata().blobs()) {
        String pk = bm.properties() == null ? null : bm.properties().get(PARTITION_PROPERTY);
        // A blob with no partition property describes the whole table (the bootstrap) and cannot
        // be reconciled with per-partition ones; drop it rather than double-count.
        if (pk != null && !superseded.contains(pk)) {
          wanted.add(bm);
        }
      }
      if (wanted.isEmpty()) {
        return out;
      }
      for (org.apache.iceberg.util.Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> pair
          : reader.readAll(wanted)) {
        Carried c = new Carried();
        c.type = pair.first().type();
        c.fields = pair.first().inputFields();
        c.properties = pair.first().properties();
        ByteBuffer src = pair.second();
        byte[] copy = new byte[src.remaining()];
        src.duplicate().get(copy);
        c.payload = ByteBuffer.wrap(copy);
        out.add(c);
      }
    } catch (Exception e) {
      LOGGER.warn("Could not carry forward statistics from {}", latest.path(), e);
    }
    return out;
  }


  /**
   * Blob type for a cardinality measured over the committed table rather than accumulated while
   * writing.
   *
   * <p>Deliberately not {@code apache-datasketches-theta-v1}: that type promises a serialized
   * theta sketch as the payload, and a cardinality computed by the engine is a number — a sketch
   * cannot be reconstructed from one. Writing a number under the theta type would fail in another
   * engine at read time, long after the snapshot committed. This carries the same {@code ndv}
   * property in a standard Puffin container under a name that describes what is actually there.
   */
  private static final String NDV_BLOB_TYPE = "calcite-ndv-v1";

  /**
   * Measures cardinality over the whole committed table and publishes it.
   *
   * <p>Preferred over sketches accumulated during a write, which only ever describe the rows that
   * run happened to write. Most runs write a slice — {@code overwritePartitions} replaces just the
   * partitions touched — so an accumulated sketch would report one partition's cardinality as the
   * table's, and would do so again on every run. Measuring after the commit describes the table as
   * it now stands, whatever the run did to it.
   *
   * <p>One pass covers every column: the engine maintains all the estimators concurrently while
   * scanning once, so this costs a scan rather than a scan per column — measured at 0.6s for 10
   * columns over 22.8M rows. Against an ETL run of minutes to hours that is not a cost worth
   * optimizing away.
   *
   * @param conn         connection able to read {@code fromClause}
   * @param fromClause   scan expression for the committed table
   * @param columns      columns to measure
   */
  public static void publishMeasured(Table table, String statisticsPath,
      java.sql.Connection conn, String fromClause, List<String> columns) {
    if (table == null || table.currentSnapshot() == null || columns == null || columns.isEmpty()) {
      return;
    }
    Map<String, Long> ndv = computeNdv(conn, fromClause, columns);
    if (ndv.isEmpty()) {
      return;
    }
    Snapshot snapshot = table.currentSnapshot();
    try {
      OutputFile out = table.io().newOutputFile(statisticsPath);
      List<BlobMetadata> written = new ArrayList<BlobMetadata>();
      long fileSize;
      long footerSize;
      try (PuffinWriter writer = Puffin.write(out).createdBy("calcite-file-adapter").build()) {
        for (Map.Entry<String, Long> e : ndv.entrySet()) {
          org.apache.iceberg.types.Types.NestedField field =
              table.schema().findField(e.getKey().toLowerCase(Locale.ROOT));
          if (field == null) {
            continue;
          }
          Map<String, String> props = new LinkedHashMap<String, String>();
          props.put(NDV_PROPERTY, Long.toString(e.getValue()));
          writer.add(
              new Blob(NDV_BLOB_TYPE, Collections.singletonList(field.fieldId()),
                  snapshot.snapshotId(), snapshot.sequenceNumber(),
                  ByteBuffer.wrap(new byte[0]), null, props));
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
      org.apache.iceberg.UpdateStatistics measured =
          table.updateStatistics().setStatistics(snapshot.snapshotId(), statsFile);
      org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock.runExclusive(
          table.location(), measured::commit);
      LOGGER.info("Published measured column statistics: {} columns for snapshot {}",
          written.size(), snapshot.snapshotId());
    } catch (Exception e) {
      LOGGER.warn("Could not publish measured column statistics: {}", e.toString());
    }
  }

  /**
   * Builds per-partition sketches for a table that has none, by scanning it once.
   *
   * <p>This is the bootstrap, and it has to produce PER-PARTITION sketches rather than a
   * whole-table number. A later run rewrites some partitions and carries the rest forward, which
   * requires every partition to be represented separately; a single whole-table figure has no
   * partition to attribute it to and cannot be reconciled with the ones a run replaces. Emitting
   * one would leave the incremental path permanently unreachable — every run would find nothing to
   * carry forward and rescan the whole table again.
   *
   * <p>Values are read rather than counted by the engine because a theta sketch cannot be rebuilt
   * from a cardinality: {@code approx_count_distinct} returns a number, and the sketch is what
   * makes later runs mergeable. So this pays a real scan — once per table, after which every run
   * is incremental.
   *
   * <p>Partition keys are derived from the table's own partition spec and formatted to match the
   * writer's {@code buildPartitionKey} — sorted {@code name=value} joined by {@code |} — because a
   * key that does not match is a key that never gets superseded, leaving a stale partition sketch
   * unioned in forever.
   */
  public static void bootstrapFromScan(Table table, String statisticsPath) {
    if (table == null || table.currentSnapshot() == null) {
      return;
    }
    List<String> partitionColumns = new ArrayList<String>();
    for (org.apache.iceberg.PartitionField pf : table.spec().fields()) {
      org.apache.iceberg.types.Types.NestedField src = table.schema().findField(pf.sourceId());
      if (src != null) {
        partitionColumns.add(src.name());
      }
    }
    java.util.Collections.sort(partitionColumns);

    IcebergThetaStatistics stats = new IcebergThetaStatistics();
    long rows = 0;
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> records =
             org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
      for (org.apache.iceberg.data.Record r : records) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (org.apache.iceberg.types.Types.NestedField f : table.schema().columns()) {
          row.put(f.name(), r.getField(f.name()));
        }
        StringBuilder key = new StringBuilder();
        for (String pc : partitionColumns) {
          if (key.length() > 0) {
            key.append('|');
          }
          key.append(pc).append('=').append(String.valueOf(row.get(pc)));
        }
        stats.addRow(key.toString(), row);
        rows++;
      }
    } catch (Exception e) {
      LOGGER.warn("Could not scan {} to bootstrap column statistics", table.name(), e);
      return;
    }
    if (stats.isEmpty()) {
      return;
    }
    LOGGER.info("Bootstrapping column statistics from {} rows across {} partitions",
        rows, stats.writtenPartitions().size());
    stats.commit(table, statisticsPath);
  }

  /** Partition keys this instance holds sketches for. */
  public java.util.Set<String> writtenPartitions() {
    return byPartition.keySet();
  }

  /**
   * True when the table holds per-partition sketches a later run can build on.
   *
   * <p>Deliberately NOT scoped to the current snapshot. Every write creates a new snapshot while
   * statistics are keyed to the snapshot they were computed from, so a current-snapshot test is
   * false immediately after any write — which sent every run down the full-rescan bootstrap and
   * left the per-partition path unreachable, discarding the whole point of making the sketches
   * mergeable. What matters is whether there is prior work to carry forward, not whether it is
   * labelled with the snapshot this run just created.
   */
  public static boolean hasCarryForwardStatistics(Table table) {
    if (table == null) {
      return false;
    }
    for (StatisticsFile sf : table.statisticsFiles()) {
      for (org.apache.iceberg.BlobMetadata bm : sf.blobMetadata()) {
        // Only per-partition sketches can be carried forward. A whole-table measurement is a
        // scalar with no partition to attribute it to, so it cannot be reconciled with the
        // partitions a later run rewrites — a table holding only that must be re-measured.
        if (StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(bm.type())
            && bm.properties() != null
            && bm.properties().get(PARTITION_PROPERTY) != null) {
          return true;
        }
      }
    }
    return false;
  }

  /** Measures distinct values for every column in one pass. */
  private static Map<String, Long> computeNdv(java.sql.Connection conn, String fromClause,
      List<String> columns) {
    Map<String, Long> out = new LinkedHashMap<String, Long>();
    StringBuilder sql = new StringBuilder("SELECT ");
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("approx_count_distinct(\"").append(columns.get(i)).append("\")");
    }
    sql.append(" FROM ").append(fromClause);
    try (java.sql.Statement st = conn.createStatement();
         java.sql.ResultSet rs = st.executeQuery(sql.toString())) {
      if (rs.next()) {
        for (int i = 0; i < columns.size(); i++) {
          out.put(columns.get(i), rs.getLong(i + 1));
        }
      }
    } catch (Exception e) {
      // One unhashable column type fails the whole SELECT. Skipping the table leaves the planner
      // on its defaults, which is the correct outcome for "could not measure".
      LOGGER.warn("Could not measure column cardinality: {}", e.getMessage());
      out.clear();
    }
    return out;
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
    // Deliberately reads ONLY blob metadata — the Puffin footer — and never a sketch payload.
    // Deserializing one would pull in datasketches-memory, whose static initializer rejects any
    // JDK past 21 and fails with an ExceptionInInitializerError. That is an Error, not an
    // Exception, so it would bypass the guards here and surface during query planning on a client
    // whose JDK we do not choose. The write side already rolls the per-partition sketches up and
    // publishes the total, so a query only needs to read a number.
    try {
      for (StatisticsFile sf : table.statisticsFiles()) {
        if (sf.snapshotId() != snapshotId) {
          // Statistics describe the snapshot they were computed from; an older one would report
          // cardinalities for data this query will not read.
          continue;
        }
        for (org.apache.iceberg.BlobMetadata bm : sf.blobMetadata()) {
          if (!NDV_BLOB_TYPE.equals(bm.type()) || bm.fields().size() != 1) {
            continue;
          }
          String ndv = bm.properties() == null ? null : bm.properties().get(NDV_PROPERTY);
          if (ndv == null) {
            continue;
          }
          org.apache.iceberg.types.Types.NestedField f = table.schema().findField(bm.fields().get(0));
          if (f != null) {
            out.put(f.name().toLowerCase(Locale.ROOT), Long.parseLong(ndv));
          }
        }
      }
    // Throwable, not Exception: a missing or incompatible dependency raises an Error, and a
    // statistic that cannot be read must degrade to "unknown" rather than fail the query that
    // happened to touch this table.
    } catch (Throwable t) {
      LOGGER.debug("Could not read Iceberg column statistics: {}", t.toString());
      out.clear();
    }
    return out;
  }
}
