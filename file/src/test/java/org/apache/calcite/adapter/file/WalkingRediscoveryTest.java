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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FILE-027 — a walking source re-discovers its table-set: a freshly built schema reflects files
 * added to / removed from the directory since the last build. (The live refresh-driven incremental
 * re-walk of a single partitioned table is a separate concern; see FILE-135.)
 */
@Tag("unit")
public class WalkingRediscoveryTest {

  @Test @Tag("FILE-027") void reWalkReflectsAddedAndRemovedFiles(@TempDir Path dir) throws Exception {
    write(dir, "a.csv");
    assertEquals(set("a"), tables(dir), "first walk sees a");

    write(dir, "b.csv");
    assertEquals(set("a", "b"), tables(dir), "re-walk picks up the added file");

    Files.delete(dir.resolve("a.csv"));
    assertEquals(set("b"), tables(dir), "re-walk drops the removed file");
  }

  private static void write(Path dir, String name) throws Exception {
    Files.write(dir.resolve(name), "id,v\n1,x\n".getBytes(StandardCharsets.UTF_8));
  }

  private static TreeSet<String> set(String... names) {
    TreeSet<String> s = new TreeSet<>();
    for (String n : names) {
      s.add(n);
    }
    return s;
  }

  /** Opens a fresh schema over the directory and returns the file-derived table names. */
  private static TreeSet<String> tables(Path dir) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(dir));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    TreeSet<String> names = new TreeSet<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
      while (rs.next()) {
        String schema = rs.getString("TABLE_SCHEM");
        if (schema != null && schema.equalsIgnoreCase("s")) {
          names.add(rs.getString("TABLE_NAME").toLowerCase());
        }
      }
    }
    return names;
  }

  private static String model(Path dir) {
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + dir.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"ephemeralCache\": true,\n"
        + "      \"executionEngine\": \"linq4j\"\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }
}
