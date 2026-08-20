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
package org.apache.calcite.adapter.govdata;
// storage-provider-guard:ignore-file - audited: all filesystem operations here target the
// genuinely-local operating directory (~/.govdata) — the pre-built DuckDB catalog and the
// per-schema .conversions.json tracker — not object-store URIs.

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Seeds the shared DuckDB catalog and its {@code .conversions.json} trackers from a pre-built
 * artifact bundled in the shadow JAR, accelerating time-to-first-query on a cold operating
 * directory.
 *
 * <p>The govdata build produces {@code /duckdb/seed/govdata-seed.zip} (via the
 * {@code bundleGovdataSeed} Gradle task) containing, relative to the operating-directory base
 * ({@code ~/.govdata} by default):
 * <ul>
 *   <li>{@code .duckdb/govdata.duckdb} — the single shared catalog (view DDL only, no data),
 *       built against {@code s3://} URIs so every {@code iceberg_scan}/{@code parquet_scan} view
 *       is machine-independent;</li>
 *   <li>{@code .aperio/&lt;schema&gt;/.conversions.json} — the per-schema conversion trackers,
 *       whose Iceberg records are namespaced by the S3 warehouse root and carry {@code s3://}
 *       paths only.</li>
 * </ul>
 *
 * <p>On the first connection per JVM, {@link #ensureSeeded(String)} compares a SHA-256 fingerprint
 * of the bundled {@code govdata-seed.zip} against the on-disk marker
 * ({@code &lt;base&gt;/.duckdb/govdata.duckdb.version}) AND confirms the catalog file itself
 * ({@code &lt;base&gt;/.duckdb/govdata.duckdb}) still exists. Both must hold for extraction to be
 * skipped (fast path); either the fingerprint changing (a JAR upgrade with new/changed data) or the
 * catalog file being absent despite a matching marker (deleted, corrupted, or replaced out from
 * under the marker) forces re-extraction. A content fingerprint is used rather than the project
 * version because the project version stays constant across many SNAPSHOT rebuilds while the
 * bundled catalog's actual content changes — a version-only gate would then treat a rebuilt seed as
 * already installed and never extract it. Re-seeding is always safe: the runtime creates any
 * missing view from {@code s3://} on demand and heals column-count drift, so a replaced catalog
 * reconciles itself against live data.
 *
 * <p>Seeding is a pure accelerator: it must run <em>before</em> the DuckDB catalog is opened (so
 * the file is not overwritten while DuckDB holds its single-writer lock), and a missing or
 * unreadable seed must never fail the connection — the cold path (live Iceberg discovery) still
 * produces a correct catalog. This mirrors {@link DuckDbExtensionInstaller}, which likewise treats
 * an absent bundled resource as a no-op.
 */
public final class GovDataSeedInstaller {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSeedInstaller.class);

  private static final String SEED_ZIP_RESOURCE = "/duckdb/seed/govdata-seed.zip";
  private static final String SEED_VERSION_RESOURCE = "/duckdb/seed/govdata-seed.version";
  private static final String MARKER_RELATIVE = ".duckdb/govdata.duckdb.version";
  private static final String CATALOG_RELATIVE = ".duckdb/govdata.duckdb";
  private static final String SCHEMA_CACHE_RESOURCE = "/duckdb/seed/iceberg-schema-cache.json";

  /** Seed check is a once-per-JVM operation; connect() is called for every connection. */
  private static volatile boolean checkedThisJvm;

  private GovDataSeedInstaller() {
  }

  /** Test-only: clears the once-per-JVM gate so {@link #ensureSeeded(String)} runs again. */
  static void resetForTesting() {
    checkedThisJvm = false;
  }

  /**
   * Extracts the bundled seed into the operating directory on first use if the on-disk version
   * marker is absent or does not match the bundled seed version. Idempotent and safe to call on
   * every connection; the check runs at most once per JVM.
   *
   * @param operatingBase absolute path of the operating-directory base (e.g. {@code ~/.govdata});
   *                      a null/empty value is a no-op
   */
  public static synchronized void ensureSeeded(String operatingBase) {
    if (checkedThisJvm) {
      return;
    }
    checkedThisJvm = true;

    if (operatingBase == null || operatingBase.isEmpty()) {
      return;
    }

    installBundledSchemaCache();

    // A JAR built without running bundleGovdataSeed has no seed resource: nothing to do.
    byte[] zipBytes = readResourceBytes(SEED_ZIP_RESOURCE);
    if (zipBytes == null) {
      LOGGER.debug("No bundled govdata seed ({}); skipping seed (cold start)", SEED_ZIP_RESOURCE);
      return;
    }
    String bundledFingerprint = sha256Hex(zipBytes);
    // Logged only, for a human comparing a bundled seed to its build — the fingerprint above,
    // not this string, is what gates extraction.
    String bundledVersion = readResourceText(SEED_VERSION_RESOURCE);

    File base = new File(operatingBase);
    File marker = new File(base, MARKER_RELATIVE);
    File catalogFile = new File(base, CATALOG_RELATIVE);

    // A catalog rebuilt at runtime (DuckDBCatalogMaintenance.rebuildPendingViews — e.g. an
    // operator "update schema" command) is newer than whatever seed this jar was built with.
    // Never clobber that with the jar's bundled copy, fingerprint match or not: the datetime
    // alone settles which one is authoritative, so a rebuilt catalog survives a restart.
    File jarFile = runningJarFile();
    if (jarFile != null && catalogFile.isFile()
        && catalogFile.lastModified() > jarFile.lastModified()) {
      LOGGER.info("govdata catalog {} ({}) is newer than the running jar ({}); leaving it in place",
          catalogFile.getAbsolutePath(),
          java.time.Instant.ofEpochMilli(catalogFile.lastModified()),
          java.time.Instant.ofEpochMilli(jarFile.lastModified()));
      return;
    }

    String onDiskFingerprint = marker.isFile() ? readFileText(marker) : null;
    if (bundledFingerprint.equals(onDiskFingerprint) && catalogFile.isFile()) {
      LOGGER.debug("govdata seed up to date (fingerprint {}, version {}); skipping extraction",
          bundledFingerprint, bundledVersion);
      return;
    }
    if (bundledFingerprint.equals(onDiskFingerprint)) {
      LOGGER.info("govdata seed marker matches but catalog file {} is missing; re-extracting seed",
          catalogFile.getAbsolutePath());
    }

    try {
      int entries = extractInto(new java.io.ByteArrayInputStream(zipBytes), base);
      writeFileText(marker, bundledFingerprint);
      LOGGER.info("Seeded govdata catalog: extracted {} entr{} into {} (fingerprint {}, version {})",
          entries, entries == 1 ? "y" : "ies", base.getAbsolutePath(), bundledFingerprint,
          bundledVersion);
    } catch (IOException e) {
      // A failed seed is recoverable: the runtime rebuilds views/trackers from s3:// on demand.
      LOGGER.warn("Failed to seed govdata catalog into {}: {}", base.getAbsolutePath(),
          e.getMessage(), e);
    }
  }

  /**
   * Absolute path this class was loaded from, or {@code null} if not running from an actual jar
   * file (e.g. an exploded classes directory in a test/IDE run) — there's no meaningful build
   * time to compare a catalog's freshness against in that case.
   */
  private static File runningJarFile() {
    try {
      java.security.CodeSource codeSource =
          GovDataSeedInstaller.class.getProtectionDomain().getCodeSource();
      if (codeSource == null) {
        return null;
      }
      File jarFile = new File(codeSource.getLocation().toURI());
      return jarFile.isFile() ? jarFile : null;
    } catch (java.net.URISyntaxException e) {
      return null;
    }
  }

  /**
   * Installs the bundled Iceberg schema cache into the Iceberg cache directory.
   *
   * <p>Deliberately outside the catalog's version gate, because the two artifacts are validated
   * differently. The catalog is derived from the driver's own model, so a version mismatch means
   * it would answer wrongly and it must be regenerated. The schema cache is derived from the
   * warehouse and validated by digest against the published copy, so a mismatch only means it
   * gets re-downloaded — and a cache miss falls through to the live read regardless. It therefore
   * needs no version gate, only a place to land before the first connection reads it.
   */
  private static void installBundledSchemaCache() {
    try (InputStream in =
             GovDataSeedInstaller.class.getResourceAsStream(SCHEMA_CACHE_RESOURCE)) {
      if (in == null) {
        LOGGER.debug("No bundled Iceberg schema cache ({}); schemas resolve live or by download",
            SCHEMA_CACHE_RESOURCE);
        return;
      }
      org.apache.calcite.adapter.file.iceberg.IcebergSchemaCache.installBundled(in);
    } catch (IOException e) {
      // Purely an accelerator: without it the cache is downloaded, or schemas are read live.
      LOGGER.warn("Could not install bundled Iceberg schema cache: {}", e.getMessage());
    }
  }

  /**
   * Extracts every zip entry beneath {@code base}, creating parent directories and replacing any
   * existing file. Guards against zip-slip: an entry that resolves outside {@code base} is
   * rejected.
   *
   * <p>Replacing a {@code .duckdb} catalog also discards the write-ahead log sitting beside it.
   * The seed ships a catalog but never a WAL, so without this the new catalog inherits the old
   * one's {@code .wal} — a log written against a database that no longer exists. DuckDB then
   * reconciles the two on open, which is both semantically wrong (the log describes different
   * content) and pathologically slow.
   *
   * @return number of file entries written
   */
  private static int extractInto(InputStream zipIn, File base) throws IOException {
    String baseCanonical = base.getCanonicalPath();
    int written = 0;
    ZipInputStream zis = new ZipInputStream(zipIn);
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      File target = new File(base, entry.getName());
      String targetCanonical = target.getCanonicalPath();
      if (!targetCanonical.equals(baseCanonical)
          && !targetCanonical.startsWith(baseCanonical + File.separator)) {
        throw new IOException("Zip entry escapes operating directory: " + entry.getName());
      }
      if (entry.isDirectory()) {
        Files.createDirectories(target.toPath());
        zis.closeEntry();
        continue;
      }
      File parent = target.getParentFile();
      if (parent != null) {
        Files.createDirectories(parent.toPath());
      }
      Files.copy(zis, target.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      discardOrphanedWal(target);
      written++;
      zis.closeEntry();
    }
    return written;
  }

  /**
   * Deletes the write-ahead log beside a freshly-extracted DuckDB catalog.
   *
   * <p>Called for every extracted entry; a no-op unless the entry is a {@code .duckdb} file that
   * actually had a {@code .wal} next to it. The WAL belongs to the catalog just overwritten, so
   * once that file is gone the log describes nothing and only costs time on open — measured at
   * 377s versus 13s for the same seed installed without it.
   *
   * <p>Discarding it loses no durable state: the seed is a pre-built accelerator that the runtime
   * reconciles against live Iceberg data anyway, so anything the WAL held is rebuilt on demand.
   */
  private static void discardOrphanedWal(File extracted) {
    if (!extracted.getName().endsWith(".duckdb")) {
      return;
    }
    File wal = new File(extracted.getParentFile(), extracted.getName() + ".wal");
    if (!wal.isFile()) {
      return;
    }
    if (wal.delete()) {
      LOGGER.info("Discarded orphaned WAL {} left by the replaced catalog", wal.getAbsolutePath());
    } else {
      // Not fatal, but the slow-open cost above is now unavoidable, so say so plainly.
      LOGGER.warn("Could not delete orphaned WAL {}; the next catalog open will be slow", wal);
    }
  }

  /** Reads a classpath resource as a trimmed UTF-8 string, or null if the resource is absent. */
  private static String readResourceText(String resource) {
    byte[] bytes = readResourceBytes(resource);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8).trim();
  }

  /** Reads a classpath resource fully into memory, or null if the resource is absent. */
  private static byte[] readResourceBytes(String resource) {
    try (InputStream is = GovDataSeedInstaller.class.getResourceAsStream(resource)) {
      if (is == null) {
        return null;
      }
      return readAll(is);
      // fallback-guard: allow documented 'or null if resource is absent' optional classpath-resource loader used only for seed installation bookkeeping
    } catch (IOException e) {
      LOGGER.warn("Could not read seed resource {}: {}", resource, e.getMessage());
      return null;
    }
  }

  /** SHA-256 of {@code bytes}, as lowercase hex — the content fingerprint gating extraction. */
  private static String sha256Hex(byte[] bytes) {
    try {
      java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(bytes);
      StringBuilder sb = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      // SHA-256 is a JLS-mandated algorithm, guaranteed present on every JVM.
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  /** Reads a file as a trimmed UTF-8 string, or null on error. */
  private static String readFileText(File file) {
    try {
      return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).trim();
      // fallback-guard: allow reads an idempotency marker file, documented 'or null on error'; unreadable marker only risks a safe redundant reinstall
    } catch (IOException e) {
      LOGGER.debug("Could not read seed marker {}: {}", file.getAbsolutePath(), e.getMessage());
      return null;
    }
  }

  /** Writes {@code content} to {@code file}, creating parent directories as needed. */
  private static void writeFileText(File file, String content) throws IOException {
    File parent = file.getParentFile();
    if (parent != null) {
      Files.createDirectories(parent.toPath());
    }
    Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] readAll(InputStream is) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = is.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }
}
