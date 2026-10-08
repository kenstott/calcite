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
package org.apache.calcite.adapter.file.duckdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The SQL views of a schema whose DuckDB catalog is ephemeral.
 *
 * <p>A schema that keeps its cache under {@code /tmp} gets a catalog that lasts as long as the
 * connection, and such a catalog has no path to defer a view under. Where the system temporary
 * directory is {@code /tmp}, as on Linux, that is every schema with an ephemeral cache.
 */
@Tag("unit")
public class DuckDBEphemeralCatalogViewTest {
  private static final Path TMP = Paths.get("/tmp");

  private Path base;

  @BeforeEach void createBaseDirectory() throws IOException {
    assumeTrue(Files.isDirectory(TMP) && Files.isWritable(TMP), "/tmp is not a writable directory");
    base = Files.createTempDirectory(TMP, "calcite-ephemeral-catalog-");
    Files.createDirectory(base.resolve("data"));
  }

  @AfterEach void deleteBaseDirectory() throws IOException {
    if (base == null) {
      return;
    }
    try (Stream<Path> files = Files.walk(base)) {
      for (Path file : (Iterable<Path>) files.sorted(Comparator.reverseOrder())::iterator) {
        Files.delete(file);
      }
    }
  }

  private Connection connect() throws Exception {
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"executionEngine\": \"duckdb\",\n"
        + "        \"baseDirectory\": \"" + base + "\",\n"
        + "        \"directory\": \"" + base.resolve("data") + "\",\n"
        + "        \"views\": [\n"
        + "          {\"name\": \"doubled\","
        + " \"sql\": \"SELECT answer * 2 AS doubled FROM \\\"TEST\\\".\\\"answer\\\"\"},\n"
        + "          {\"name\": \"answer\", \"sql\": \"SELECT 42 AS answer\"},\n"
        + "          {\"name\": \"over_missing\", \"sql\": \"SELECT * FROM not_ingested_yet\"}\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  @Test void viewsAreListedAndAnswerQueries() throws Exception {
    try (Connection connection = connect()) {
      Set<String> listed = new HashSet<>();
      try (ResultSet tables = connection.getMetaData().getTables(null, "TEST", "%", null)) {
        while (tables.next()) {
          listed.add(tables.getString("TABLE_NAME").toLowerCase(Locale.ROOT));
        }
      }
      assertTrue(listed.contains("answer"), "answer is listed among " + listed);
      assertTrue(listed.contains("doubled"),
          "a view declared before the view it reads is listed among " + listed);
      assertFalse(listed.contains("over_missing"),
          "a view over a missing table is not listed among " + listed);

      try (Statement statement = connection.createStatement()) {
        try (ResultSet rs = statement.executeQuery("SELECT answer FROM answer")) {
          assertTrue(rs.next());
          assertEquals(42, rs.getInt(1));
        }
        try (ResultSet rs = statement.executeQuery("SELECT doubled FROM doubled")) {
          assertTrue(rs.next());
          assertEquals(84, rs.getInt(1));
        }
      }
    }
  }
}
