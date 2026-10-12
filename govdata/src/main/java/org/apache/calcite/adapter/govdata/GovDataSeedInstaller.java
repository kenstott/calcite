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
import java.sql.SQLException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Puts the official govdata catalog in place: the seed packaged in this jar.
 *
 * <p>The govdata build produces {@code /duckdb/seed/govdata-seed.zip} (the
 * {@code bundleGovdataSeed} Gradle task) containing, relative to the operating-directory base:
 * <ul>
 *   <li>{@code .duckdb/govdata.duckdb} — the catalog (view DDL only, no data), built against
 *       {@code s3://} URIs so every view is machine-independent;</li>
 *   <li>{@code .aperio/<schema>/.conversions.json} — the per-schema conversion trackers.</li>
 * </ul>
 * and, beside it, {@code /duckdb/seed/govdata-seed.schema}: the schema the seed declares (see
 * {@link GovDataSeedSchema}).
 *
 * <p>The seed in the jar is the official catalog. At run time a catalog is never built by
 * discovery — not on a first start, not when the installed catalog is missing, not when it
 * differs. On the first use per JVM, {@link #ensureSeeded(String)} compares the schema the
 * installed catalog declares with the schema the jar's seed declares:
 * <ul>
 *   <li>the same schema: nothing is done (row counts, statistics, timestamps and file bytes
 *       may differ; they do not make a catalog different);</li>
 *   <li>no installed catalog, an unreadable one, or a different schema: the installed catalog
 *       is replaced by the jar's seed. It is never repaired or rebuilt;</li>
 *   <li>the installed catalog is held open by another process: it is left alone. DuckDB locks
 *       a catalog file against other processes, so this process opens a copy of its own, which
 *       {@code DuckDBJdbcSchemaFactory} takes from this jar's seed.</li>
 * </ul>
 * A jar that carries no seed, a seed without its schema listing, or a seed that cannot be
 * extracted stops the start with {@link SeedException}, which names what is missing. There is
 * no fallback to building a catalog. Building one belongs to the release path
 * ({@code govdata/scripts/build-seed.sh}).
 *
 * <p>Must run before any DuckDB connection to the catalog is opened by this process.
 */
public final class GovDataSeedInstaller {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSeedInstaller.class);

  private static final String SEED_ZIP_RESOURCE = "/duckdb/seed/govdata-seed.zip";
  private static final String SEED_VERSION_RESOURCE = "/duckdb/seed/govdata-seed.version";
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

  /** The start cannot proceed because the official seed is missing or unusable. */
  public static final class SeedException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    SeedException(String message) {
      super(message);
    }

    SeedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Puts this jar's seed in place under {@code operatingBase} unless the catalog installed
   * there already declares the same schema. Safe to call on every connection; the check runs
   * once per JVM.
   *
   * @throws SeedException if the jar carries no seed or no schema listing for it, or the seed
   *     cannot be extracted
   */
  public static synchronized void ensureSeeded(String operatingBase) {
    if (checkedThisJvm) {
      return;
    }
    if (operatingBase == null || operatingBase.isEmpty()) {
      throw new SeedException("No operating directory was given for the govdata catalog seed");
    }
    File base = new File(operatingBase);
    install(base, new File(base, CATALOG_RELATIVE));
  }

  /**
   * Puts this jar's seed catalog in place at {@code catalogFile} unless the catalog installed
   * there already declares the same schema. This is the start of a server, which names its
   * catalog file itself (its model's {@code database_filename}) and connects through the
   * schema factory, never through {@link GovDataDriver}: without this call nothing put the
   * seed in place for it, and it built its whole catalog by discovery at every first start.
   * Only the catalog is written; the seed's other entries belong to an operating directory,
   * which a serving connection does not use. Safe to call for every schema of the model; the
   * check runs once per JVM.
   *
   * @throws SeedException if the jar carries no seed or no schema listing for it, or the seed
   *     cannot be extracted
   */
  public static synchronized void ensureCatalog(File catalogFile) {
    if (checkedThisJvm) {
      return;
    }
    if (catalogFile == null) {
      throw new SeedException("No catalog file was given for the govdata catalog seed");
    }
    install(null, catalogFile.getAbsoluteFile());
  }

  /** {@code base} is the operating directory the whole seed is extracted into, or null to
   *  write only the catalog, to {@code catalogFile}. */
  private static void install(File base, File catalogFile) {
    installBundledSchemaCache();
    byte[] zipBytes = readResourceBytes(SEED_ZIP_RESOURCE);
    if (zipBytes == null) {
      throw new SeedException("This jar carries no govdata catalog seed (" + SEED_ZIP_RESOURCE
          + "). The catalog is never built at run time; use a jar built with its seed. (The "
          + "release path that makes the seed, govdata/scripts/build-seed.sh, is the only "
          + "caller that builds one, under -D" + GovDataDriver.SEED_BUILD_PROPERTY + "; it is "
          + "not a way to start a server.)");
    }
    String seedListingText = readResourceText(GovDataSeedSchema.RESOURCE);
    if (seedListingText == null) {
      throw new SeedException("This jar's govdata catalog seed has no schema listing ("
          + GovDataSeedSchema.RESOURCE + "); the seed was packaged without it.");
    }
    List<String> seedListing = GovDataSeedSchema.parse(seedListingText);
    if (seedListing.isEmpty()) {
      throw new SeedException("This jar's govdata catalog seed declares no schema ("
          + GovDataSeedSchema.RESOURCE + " is empty).");
    }

    String reason = reasonToReplace(catalogFile, seedListing);
    if (reason == null) {
      LOGGER.info("govdata catalog {}: the installed catalog declares the seed's schema; "
          + "nothing to put in place", catalogFile.getAbsolutePath());
    } else {
      try {
        if (base != null) {
          int entries = extractInto(new java.io.ByteArrayInputStream(zipBytes), base);
          LOGGER.info("Put the jar's govdata catalog seed in place ({}): {} entr{} into {} "
              + "(seed version {})", reason, entries, entries == 1 ? "y" : "ies",
              base.getAbsolutePath(), readResourceText(SEED_VERSION_RESOURCE));
        } else {
          extractCatalogTo(new java.io.ByteArrayInputStream(zipBytes), catalogFile);
          LOGGER.info("Put the jar's govdata catalog seed in place ({}): {} (seed version {})",
              reason, catalogFile.getAbsolutePath(), readResourceText(SEED_VERSION_RESOURCE));
        }
      } catch (IOException e) {
        throw new SeedException("Could not put the govdata catalog seed in place at "
            + catalogFile.getAbsolutePath() + ": " + e.getMessage(), e);
      }
    }
    checkedThisJvm = true;
  }

  /** Writes the seed's catalog entry, and nothing else of the seed, to {@code catalogFile}. */
  private static void extractCatalogTo(InputStream zipIn, File catalogFile) throws IOException {
    ZipInputStream zis = new ZipInputStream(zipIn);
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (!entry.isDirectory() && CATALOG_RELATIVE.equals(entry.getName())) {
        File parent = catalogFile.getParentFile();
        if (parent != null) {
          Files.createDirectories(parent.toPath());
        }
        Files.copy(zis, catalogFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        discardOrphanedWal(catalogFile);
        return;
      }
      zis.closeEntry();
    }
    throw new IOException("the seed holds no " + CATALOG_RELATIVE);
  }

  /**
   * Why the installed catalog must be replaced by the seed, or null when it is left as it is.
   */
  private static String reasonToReplace(File catalogFile, List<String> seedListing) {
    if (!catalogFile.isFile()) {
      return "no catalog installed";
    }
    List<String> installed;
    try {
      installed = GovDataSeedSchema.listing(catalogFile);
    } catch (SQLException e) {
      if (isHeldByAnotherProcess(e)) {
        LOGGER.info("govdata catalog {} is held open by another process; leaving it in place. "
            + "This process opens its own copy, taken from this jar's seed.",
            catalogFile.getAbsolutePath());
        return null;
      }
      return "the installed catalog cannot be read: " + e.getMessage();
    }
    if (installed.equals(seedListing)) {
      LOGGER.debug("govdata catalog {} declares the seed's schema; leaving it in place",
          catalogFile.getAbsolutePath());
      return null;
    }
    return "the installed catalog declares a different schema (" + installed.size()
        + " columns, the seed " + seedListing.size() + ")";
  }

  /** DuckDB's refusal to open a catalog file another process holds, in its several wordings. */
  private static boolean isHeldByAnotherProcess(SQLException e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      String message = t.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(java.util.Locale.ROOT);
        if (lower.contains("could not set lock") || lower.contains("conflicting lock")
            || lower.contains("being used by another process")
            || lower.contains("sharing violation")
            || (lower.contains("cannot open file") && lower.contains("already open in"))) {
          return true;
        }
      }
    }
    return false;
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
