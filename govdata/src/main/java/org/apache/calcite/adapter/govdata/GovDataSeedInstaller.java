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
 * <p>On the first connection per JVM, {@link #ensureSeeded(String)} compares the bundled seed
 * version ({@code /duckdb/seed/govdata-seed.version}) against the on-disk marker
 * ({@code &lt;base&gt;/.duckdb/govdata.duckdb.version}). When they match, nothing is extracted
 * (fast path). When the marker is absent (fresh machine) or differs (JAR upgrade), the zip is
 * extracted into the operating base and the marker is rewritten. Re-seeding on a version change is
 * safe: the runtime creates any missing view from {@code s3://} on demand and heals column-count
 * drift, so a replaced catalog reconciles itself against live data.
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

  /** Seed check is a once-per-JVM operation; connect() is called for every connection. */
  private static volatile boolean checkedThisJvm;

  private GovDataSeedInstaller() {
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

    // A JAR built without running bundleGovdataSeed has no seed resource: nothing to do.
    String bundledVersion = readResourceText(SEED_VERSION_RESOURCE);
    if (bundledVersion == null) {
      LOGGER.debug("No bundled govdata seed version ({}); skipping seed (cold start)",
          SEED_VERSION_RESOURCE);
      return;
    }

    File base = new File(operatingBase);
    File marker = new File(base, MARKER_RELATIVE);
    String onDiskVersion = marker.isFile() ? readFileText(marker) : null;
    if (bundledVersion.equals(onDiskVersion)) {
      LOGGER.debug("govdata seed up to date (version {}); skipping extraction", bundledVersion);
      return;
    }

    try (InputStream zipIn = GovDataSeedInstaller.class.getResourceAsStream(SEED_ZIP_RESOURCE)) {
      if (zipIn == null) {
        // Version resource present but zip missing: a malformed build. Do not fail the
        // connection — the cold path still builds a correct catalog.
        LOGGER.warn("govdata seed version present but zip resource {} is missing; skipping seed",
            SEED_ZIP_RESOURCE);
        return;
      }
      int entries = extractInto(zipIn, base);
      writeFileText(marker, bundledVersion);
      LOGGER.info("Seeded govdata catalog: extracted {} entr{} into {} (version {})",
          entries, entries == 1 ? "y" : "ies", base.getAbsolutePath(), bundledVersion);
    } catch (IOException e) {
      // A failed seed is recoverable: the runtime rebuilds views/trackers from s3:// on demand.
      LOGGER.warn("Failed to seed govdata catalog into {}: {}", base.getAbsolutePath(),
          e.getMessage(), e);
    }
  }

  /**
   * Extracts every zip entry beneath {@code base}, creating parent directories and replacing any
   * existing file. Guards against zip-slip: an entry that resolves outside {@code base} is
   * rejected.
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
      written++;
      zis.closeEntry();
    }
    return written;
  }

  /** Reads a classpath resource as a trimmed UTF-8 string, or null if the resource is absent. */
  private static String readResourceText(String resource) {
    try (InputStream is = GovDataSeedInstaller.class.getResourceAsStream(resource)) {
      if (is == null) {
        return null;
      }
      byte[] bytes = readAll(is);
      return new String(bytes, StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      LOGGER.warn("Could not read seed resource {}: {}", resource, e.getMessage());
      return null;
    }
  }

  /** Reads a file as a trimmed UTF-8 string, or null on error. */
  private static String readFileText(File file) {
    try {
      return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).trim();
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
