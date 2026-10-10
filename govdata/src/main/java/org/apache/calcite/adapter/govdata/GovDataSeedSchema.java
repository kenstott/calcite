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
// storage-provider-guard:ignore-file - audited: reads a DuckDB catalog file in the local
// operating directory and the seed zip of the local build; no object-store URIs.

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * The schema a govdata catalog declares, as text: one line per column,
 * {@code schema.table.column:type}, sorted. This listing is the only measure of whether two
 * catalogs differ and of whether a seed covers a model. Row counts, statistics, timestamps,
 * version strings and file bytes are not part of it.
 *
 * <p>The listing of the official seed is written at build time by {@link #main} (the
 * {@code writeGovdataSeedSchema} Gradle task) and shipped beside the seed zip; the listing of
 * an installed catalog is read at start by {@link #listing(File)}. Both use this one query, so
 * the two are computed the same way.
 */
public final class GovDataSeedSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSeedSchema.class);

  /** Classpath resource holding the official seed's listing. */
  static final String RESOURCE = "/duckdb/seed/govdata-seed.schema";

  private static final String CATALOG_ENTRY = ".duckdb/govdata.duckdb";

  private static final String QUERY =
      "SELECT schema_name, table_name, column_name, data_type FROM duckdb_columns() "
      + "WHERE NOT internal AND database_name = current_database()";

  private GovDataSeedSchema() {
  }

  /**
   * Reads the listing of the catalog at {@code catalogFile}, opened read-only.
   *
   * @throws SQLException if the catalog cannot be opened or read; a catalog another process
   *     holds open fails here with DuckDB's lock error
   */
  public static List<String> listing(File catalogFile) throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("duckdb.read_only", "true");
    TreeSet<String> lines = new TreeSet<String>();
    try (Connection connection =
             DriverManager.getConnection("jdbc:duckdb:" + catalogFile.getAbsolutePath(),
                 properties);
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(QUERY)) {
      while (rs.next()) {
        lines.add(rs.getString(1) + "." + rs.getString(2) + "." + rs.getString(3) + ":"
            + rs.getString(4));
      }
    }
    return new ArrayList<String>(lines);
  }

  /** Parses listing text (one line per column) into its lines, blank lines dropped. */
  static List<String> parse(String text) {
    List<String> lines = new ArrayList<String>();
    for (String line : text.split("\n")) {
      String trimmed = line.trim();
      if (!trimmed.isEmpty()) {
        lines.add(trimmed);
      }
    }
    Collections.sort(lines);
    return lines;
  }

  /** The schema names a listing declares. */
  static TreeSet<String> schemas(List<String> listing) {
    TreeSet<String> names = new TreeSet<String>();
    for (String line : listing) {
      int dot = line.indexOf('.');
      if (dot > 0) {
        names.add(line.substring(0, dot));
      }
    }
    return names;
  }

  /**
   * Build step: writes the listing of the catalog inside a seed zip.
   * Arguments: the seed zip, the listing file to write.
   */
  public static void main(String[] args) throws IOException, SQLException {
    if (args.length != 2) {
      throw new IllegalArgumentException("usage: GovDataSeedSchema <seed.zip> <listing-out>");
    }
    File zip = new File(args[0]);
    File out = new File(args[1]);
    Path work = Files.createTempDirectory("govdata-seed-schema");
    File catalog = new File(work.toFile(), "govdata.duckdb");
    try {
      boolean found = false;
      try (InputStream in = Files.newInputStream(zip.toPath());
           ZipInputStream zis = new ZipInputStream(in)) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          if (CATALOG_ENTRY.equals(entry.getName())) {
            Files.copy(zis, catalog.toPath(), StandardCopyOption.REPLACE_EXISTING);
            found = true;
            break;
          }
        }
      }
      if (!found) {
        throw new IOException("Seed zip " + zip + " holds no " + CATALOG_ENTRY);
      }
      List<String> lines = listing(catalog);
      if (lines.isEmpty()) {
        throw new IOException("The catalog in " + zip + " declares no columns; refusing to "
            + "write an empty schema listing");
      }
      StringBuilder text = new StringBuilder();
      for (String line : lines) {
        text.append(line).append('\n');
      }
      Files.write(out.toPath(), text.toString().getBytes(StandardCharsets.UTF_8));
      LOGGER.info("Wrote seed schema listing: {} columns in {} schemas -> {}",
          lines.size(), schemas(lines).size(), out);
    } finally {
      Files.deleteIfExists(catalog.toPath());
      Files.deleteIfExists(new File(work.toFile(), "govdata.duckdb.wal").toPath());
      Files.deleteIfExists(work);
    }
  }
}
