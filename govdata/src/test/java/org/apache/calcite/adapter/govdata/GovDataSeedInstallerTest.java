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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage: the seed gate used to compare only a version marker, so a catalog file
 * deleted or replaced out from under a matching marker was never re-extracted — the MCP server's
 * first run then silently fell through to a full cold rebuild while a stale marker sat right next
 * to the missing file. {@link GovDataSeedInstaller#ensureSeeded} now also requires the catalog
 * file to exist, and gates on a content fingerprint of the bundled zip rather than the project
 * version (which stays constant across many SNAPSHOT rebuilds).
 *
 * <p>Requires the real {@code duckdb/seed/govdata-seed.zip} built into this module's resources
 * (via {@code bundleGovdataSeed}); skips itself if that resource is absent from the classpath.
 */
@Tag("unit")
class GovDataSeedInstallerTest {

  @BeforeEach
  void resetGate() {
    GovDataSeedInstaller.resetForTesting();
  }

  private static List<String> seedListing() throws Exception {
    java.io.InputStream in =
        GovDataSeedInstaller.class.getResourceAsStream(GovDataSeedSchema.RESOURCE);
    assertNotNull(in, "the seed's schema listing is on the classpath beside the seed zip");
    try {
      return GovDataSeedSchema.parse(
          new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
    } finally {
      in.close();
    }
  }

  private static File catalogIn(Path base) {
    return new File(base.toFile(), ".duckdb/govdata.duckdb");
  }

  @Test void theJarCarriesASeedAndItsSchemaListing() throws Exception {
    assertNotNull(GovDataSeedInstaller.class.getResourceAsStream("/duckdb/seed/govdata-seed.zip"),
        "the official seed is on the classpath");
    assertTrue(!seedListing().isEmpty(), "the listing declares columns");
  }

  @Test void putsTheSeedInPlaceInAFreshOperatingDir(@TempDir Path tmpDir) throws Exception {
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    assertTrue(catalogIn(tmpDir).isFile(), "the seed's catalog is in place");
    assertEquals(seedListing(), GovDataSeedSchema.listing(catalogIn(tmpDir)),
        "the installed catalog declares exactly the schema the seed's listing states");
  }

  @Test void leavesACatalogThatDeclaresTheSeedsSchemaInPlace(@TempDir Path tmpDir)
      throws Exception {
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());
    File catalog = catalogIn(tmpDir);
    long longAgo = 1_000_000_000_000L;
    assertTrue(catalog.setLastModified(longAgo), "timestamp set");

    GovDataSeedInstaller.resetForTesting();
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    assertEquals(longAgo, catalog.lastModified(),
        "same schema: the catalog is not rewritten, whatever its timestamp");
  }

  @Test void replacesACatalogThatDeclaresADifferentSchema(@TempDir Path tmpDir)
      throws Exception {
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());
    File catalog = catalogIn(tmpDir);
    try (Connection connection =
             DriverManager.getConnection("jdbc:duckdb:" + catalog.getAbsolutePath());
         Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA built_at_run_time");
      statement.execute("CREATE TABLE built_at_run_time.t (x INTEGER)");
      statement.execute("CHECKPOINT");
    }
    assertTrue(!seedListing().equals(GovDataSeedSchema.listing(catalog)),
        "precondition: the catalog now declares a schema the seed does not");

    GovDataSeedInstaller.resetForTesting();
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    assertEquals(seedListing(), GovDataSeedSchema.listing(catalog),
        "a catalog with a different schema is replaced by the seed, even though it is newer");
  }

  @Test void replacesACatalogThatCannotBeRead(@TempDir Path tmpDir) throws Exception {
    File catalog = catalogIn(tmpDir);
    Files.createDirectories(catalog.getParentFile().toPath());
    Files.write(catalog.toPath(), new byte[] {1, 2, 3});

    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    assertEquals(seedListing(), GovDataSeedSchema.listing(catalog),
        "an unreadable catalog is replaced by the seed");
  }

  /**
   * A WAL belongs to the catalog it was written against. Replacing the catalog while leaving
   * the old log beside it made DuckDB spend minutes reconciling the two on open — 377s versus
   * 13s for the same seed measured without it.
   */
  @Test void discardsTheWalLeftBesideAReplacedCatalog(@TempDir Path tmpDir) throws Exception {
    File catalog = catalogIn(tmpDir);
    Files.createDirectories(catalog.getParentFile().toPath());
    File wal = new File(catalog.getParentFile(), "govdata.duckdb.wal");
    Files.write(catalog.toPath(), new byte[] {1, 2, 3});
    Files.write(wal.toPath(), new byte[] {4, 5, 6});

    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());

    assertTrue(catalog.isFile(), "precondition: the placeholder catalog was replaced");
    assertTrue(!wal.exists(),
        "the WAL of the replaced catalog must be discarded, not inherited by the new one");
  }

  @Test void theCommittedListingIsTheListingOfTheCommittedSeed(@TempDir Path tmpDir)
      throws Exception {
    // The listing is how an installed catalog is compared with the seed. One that was not
    // regenerated when the seed was rebuilt would make every start replace its catalog.
    GovDataSeedInstaller.ensureSeeded(tmpDir.toString());
    assertEquals(seedListing(), GovDataSeedSchema.listing(catalogIn(tmpDir)),
        "govdata-seed.schema is the schema of govdata-seed.zip; regenerate it with "
            + "./gradlew :govdata:writeGovdataSeedSchema when the seed changes");
  }

  @Test void aServerIsGivenTheSeedAtTheCatalogFileItNames(@TempDir Path tmpDir)
      throws Exception {
    File catalog = new File(tmpDir.toFile(), "state/govdata-catalog.duckdb");
    GovDataSeedInstaller.ensureCatalog(catalog);
    assertTrue(catalog.isFile(), "the seed's catalog is where the server will open it");
    assertEquals(seedListing(), GovDataSeedSchema.listing(catalog));
    assertTrue(!new File(tmpDir.toFile(), "state/.aperio").exists(),
        "only the catalog is written for a server");
  }

  @Test void aServersCatalogThatDeclaresTheSeedsSchemaIsLeftInPlace(@TempDir Path tmpDir)
      throws Exception {
    File catalog = new File(tmpDir.toFile(), "govdata-catalog.duckdb");
    GovDataSeedInstaller.ensureCatalog(catalog);
    long written = catalog.lastModified();
    assertTrue(catalog.setLastModified(written - 60_000L));
    GovDataSeedInstaller.resetForTesting();
    GovDataSeedInstaller.ensureCatalog(catalog);
    assertEquals(written - 60_000L, catalog.lastModified(), "a matching catalog is not rewritten");
  }

  @Test void whichConnectionsAreServed(@TempDir Path tmpDir) {
    // An absolute path on whatever platform runs the test: "/state/..." is not one on Windows.
    File absolute = tmpDir.resolve("state").resolve("govdata.duckdb").toFile();
    java.util.Map<String, Object> served = new java.util.HashMap<>();
    served.put("database_filename", absolute.getPath());
    served.put("executionEngine", "duckdb");
    served.put("autoDownload", Boolean.FALSE);
    served.put("directory", "s3://bucket");
    assertEquals(absolute, GovDataSchemaFactory.servedCatalogFile(served));

    java.util.Map<String, Object> ingest = new java.util.HashMap<>(served);
    ingest.put("autoDownload", Boolean.TRUE);
    assertEquals(null, GovDataSchemaFactory.servedCatalogFile(ingest), "an ingest builds");

    java.util.Map<String, Object> local = new java.util.HashMap<>(served);
    local.put("directory", "/tmp/parquet");
    assertEquals(null, GovDataSchemaFactory.servedCatalogFile(local), "local files: not a server");

    java.util.Map<String, Object> noCatalog = new java.util.HashMap<>(served);
    noCatalog.remove("database_filename");
    assertEquals(null, GovDataSchemaFactory.servedCatalogFile(noCatalog));

    java.util.Map<String, Object> relative = new java.util.HashMap<>(served);
    relative.put("database_filename", "govdata-catalog.duckdb");
    assertEquals(
        new File(System.getProperty("user.dir"), ".aperio/.duckdb/govdata-catalog.duckdb"),
        GovDataSchemaFactory.servedCatalogFile(relative),
        "a relative name is resolved as the file adapter resolves it");
  }

  @Test void noOperatingDirectoryIsAnErrorNotASkip() {
    assertThrows(GovDataSeedInstaller.SeedException.class,
        () -> GovDataSeedInstaller.ensureSeeded(null));
  }
}
