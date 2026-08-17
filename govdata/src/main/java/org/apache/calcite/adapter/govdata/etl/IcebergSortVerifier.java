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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies that a heal actually did what it claims: that the table's current metadata points at
 * rewritten files, that no rows were lost doing it, and that the resulting per-file bounds are
 * narrow enough to prune.
 *
 * <p>This exists because "the heal ran without error" is not evidence of any of that. A rewrite
 * commits a whole new set of data files and a new manifest; if it silently dropped rows, or
 * produced files whose ranges still span the column, the table reads fine and every query is
 * still slow. The failure is invisible until someone measures it — which is the state that
 * produced the 177s lookup in the first place.
 *
 * <p>Run it against MinIO after {@code heal-sort-order.sh} and BEFORE publishing to R2, because
 * the publish deletes the pre-heal files and with them the ability to go back.
 *
 * <p>Checks, in the order they matter:
 * <ol>
 *   <li><b>Row preservation.</b> Compares the current snapshot's {@code total-records} against
 *       its parent's. A rewrite that loses rows is catastrophic and otherwise silent.</li>
 *   <li><b>Sort order recorded.</b> The {@code aperio.sorted-by} property, which is also what
 *       makes a re-run a no-op — if it is absent the heal did not complete.</li>
 *   <li><b>Bound overlap.</b> The payoff metric. For the leading sort column, every data file
 *       carries a lower and upper bound in the manifest. Sorted, those ranges tile the domain and
 *       a probe touches one or two files; unsorted, every file spans everything and a probe
 *       touches all of them. Reported as the worst-case and mean number of files a point lookup
 *       must open.</li>
 * </ol>
 */
public final class IcebergSortVerifier {

  private IcebergSortVerifier() {
  }

  public static void main(String[] args) {
    String warehouse = null;
    String tableName = null;
    String sortColumn = null;
    String accessKey = null;
    String secretKey = null;
    String endpoint = null;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--warehouse": warehouse = args[++i]; break;
      case "--table": tableName = args[++i]; break;
      case "--sort-column": sortColumn = args[++i]; break;
      case "--s3-access-key": accessKey = args[++i]; break;
      case "--s3-secret-key": secretKey = args[++i]; break;
      case "--s3-endpoint": endpoint = args[++i]; break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        usage();
        System.exit(1);
      }
    }
    if (warehouse == null || tableName == null) {
      usage();
      System.exit(1);
    }

    try {
      Table table = IcebergCatalogManager.loadTable(
          buildCatalogConfig(warehouse, accessKey, secretKey, endpoint), tableName);
      System.exit(verify(table, sortColumn) ? 0 : 1);
    } catch (Exception e) {
      System.err.println("Fatal error: " + e.getMessage());
      e.printStackTrace();
      System.exit(2);
    }
  }

  /**
   * Builds the catalog config, mirroring IcebergMaintenanceRunner exactly.
   *
   * <p>Extracted so the key names can be asserted in a test. They are not interchangeable and
   * getting them wrong fails silently: an earlier version of this class used
   * {@code catalogType} / {@code warehousePath} / {@code s3Endpoint}, which the loader ignores,
   * so the endpoint never reached the SDK and every call tried to resolve a real AWS host —
   * surfacing as UnknownHostException against MinIO. A local-warehouse unit test cannot catch
   * that, because it exercises no S3 path at all, which is precisely why this is asserted
   * structurally instead.
   */
  static Map<String, Object> buildCatalogConfig(String warehouse, String accessKey,
      String secretKey, String endpoint) {
    Map<String, String> hadoopConfig = new HashMap<>();
    if (accessKey != null) {
      hadoopConfig.put("fs.s3a.access.key", accessKey);
      hadoopConfig.put("fs.s3a.secret.key", secretKey);
      if (endpoint != null) {
        hadoopConfig.put("fs.s3a.endpoint", endpoint);
        hadoopConfig.put("fs.s3a.path.style.access", "true");
      }
    }
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalog", "hadoop");
    catalogConfig.put("warehouse", warehouse);
    catalogConfig.put("hadoopConfig", hadoopConfig);
    return catalogConfig;
  }

  /** @return true when every check passed. */
  static boolean verify(Table table, String sortColumn) throws Exception {
    boolean ok = true;
    System.out.println("============================================================");
    System.out.println("Sort verification: " + table.name());
    System.out.println("============================================================");

    // ── 1. sorted-by property ────────────────────────────────────────────────
    String sortedBy = table.properties().get("aperio.sorted-by");
    if (sortedBy == null || sortedBy.isEmpty()) {
      System.out.println("  [FAIL] aperio.sorted-by is not set — the heal did not complete");
      ok = false;
    } else {
      System.out.println("  [ok]   aperio.sorted-by = " + sortedBy);
      if (sortColumn == null) {
        sortColumn = sortedBy.split(",")[0].trim();
      }
    }
    if (sortColumn == null) {
      System.out.println("  [FAIL] no sort column known; pass --sort-column");
      return false;
    }
    System.out.println("         verifying bounds on leading column: " + sortColumn);

    // ── 2. row preservation across the rewrite ───────────────────────────────
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      System.out.println("  [FAIL] table has no snapshot");
      return false;
    }
    String curRecords = current.summary().get("total-records");
    Snapshot parent = current.parentId() == null ? null : table.snapshot(current.parentId());
    String parentRecords = parent == null ? null : parent.summary().get("total-records");
    if (curRecords == null) {
      System.out.println("  [warn] current snapshot reports no total-records; cannot compare");
    } else if (parentRecords == null) {
      System.out.println("  [warn] no parent snapshot to compare against (records="
          + curRecords + ")");
    } else if (curRecords.equals(parentRecords)) {
      System.out.println("  [ok]   rows preserved across rewrite: " + curRecords);
    } else {
      // A rewrite must not change the row count. Anything else means data was lost or
      // duplicated, and the pre-heal files are still on R2 — do not publish.
      System.out.println("  [FAIL] ROW COUNT CHANGED: " + parentRecords + " -> " + curRecords
          + "  — DO NOT PUBLISH; the pre-heal files on R2 are still the good copy");
      ok = false;
    }

    // ── 3. bound overlap on the leading sort column ──────────────────────────
    Types.NestedField field = table.schema().findField(sortColumn);
    if (field == null) {
      System.out.println("  [FAIL] column " + sortColumn + " is not in the schema");
      return false;
    }
    int fieldId = field.fieldId();
    Type type = field.type();

    List<Object[]> ranges = new ArrayList<>();
    int filesWithoutBounds = 0;
    int totalFiles = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        totalFiles++;
        DataFile f = task.file();
        ByteBuffer lo = f.lowerBounds() == null ? null : f.lowerBounds().get(fieldId);
        ByteBuffer hi = f.upperBounds() == null ? null : f.upperBounds().get(fieldId);
        if (lo == null || hi == null) {
          filesWithoutBounds++;
          continue;
        }
        ranges.add(new Object[] {
            Conversions.fromByteBuffer(type, lo), Conversions.fromByteBuffer(type, hi)});
      }
    }
    System.out.println("  [info] data files: " + totalFiles
        + (filesWithoutBounds > 0 ? " (" + filesWithoutBounds + " without bounds)" : ""));
    if (ranges.isEmpty()) {
      System.out.println("  [FAIL] no per-file bounds available for " + sortColumn);
      return false;
    }
    if (ranges.size() < 2) {
      System.out.println("  [ok]   single file — nothing to overlap");
      return ok;
    }

    // Worst case: the most files whose [lo,hi] contain any single probe value. Probing at every
    // file's lower bound is sufficient — an overlap maximum always occurs at some interval start.
    @SuppressWarnings("unchecked")
    Comparator<Object> cmp = (Comparator<Object>) Comparators.forType(type.asPrimitiveType());
    int worst = 0;
    long sum = 0;
    for (Object[] probe : ranges) {
      int hits = 0;
      for (Object[] r : ranges) {
        if (cmp.compare(probe[0], r[1]) <= 0 && cmp.compare(probe[0], r[0]) >= 0) {
          hits++;
        }
      }
      worst = Math.max(worst, hits);
      sum += hits;
    }
    double mean = (double) sum / ranges.size();
    double worstPct = 100.0 * worst / ranges.size();

    System.out.println("  [info] a point lookup on " + sortColumn + " must open:");
    System.out.println("           worst case " + worst + " of " + ranges.size()
        + " files (" + String.format("%.1f", worstPct) + "%)");
    System.out.println("           mean       " + String.format("%.1f", mean) + " files");

    // Unsorted data puts every file's range across the whole domain, so a probe hits nearly all
    // of them. This is the symptom that made an equality lookup cost 177s.
    if (worstPct > 80.0) {
      System.out.println("  [FAIL] bounds still overlap almost completely — the data is NOT"
          + " clustered on " + sortColumn + "; pruning will not improve");
      ok = false;
    } else if (worstPct > 25.0) {
      System.out.println("  [warn] bounds overlap more than expected for sorted data");
    } else {
      System.out.println("  [ok]   bounds are narrow — the reader can prune on " + sortColumn);
    }

    System.out.println("============================================================");
    System.out.println(ok ? "  RESULT: PASS" : "  RESULT: FAIL");
    System.out.println("============================================================");
    return ok;
  }

  private static void usage() {
    System.err.println("Usage: IcebergSortVerifier [options]");
    System.err.println("  --warehouse PATH     Iceberg warehouse (e.g. s3a://bucket/schema)");
    System.err.println("  --table NAME         Table name");
    System.err.println("  --sort-column COL    Leading sort column (default: from"
        + " aperio.sorted-by)");
    System.err.println("  --s3-access-key KEY  S3/MinIO access key");
    System.err.println("  --s3-secret-key KEY  S3/MinIO secret key");
    System.err.println("  --s3-endpoint URL    S3/MinIO endpoint");
    System.err.println();
    System.err.println("Exit: 0 pass, 1 verification failed, 2 error");
  }
}
