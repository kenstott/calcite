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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-110 / FILE-111 / FILE-095 — catalog-discovery and error-surface goldens for the file
 * adapter, plus {@link Disabled @Disabled} targets documenting two resolved-but-pending bugs.
 *
 * <p>These drive the real discovery/error path end-to-end: a {@link FileSchemaFactory}-backed
 * schema is built over a temp directory via a model, and the catalog is inspected through the JDBC
 * {@code DatabaseMetaData} or queried via SQL (mirrors {@code WalkingDiscoveryRequirementsTest}).
 * All tests are hermetic: every schema is rooted at a {@link TempDir} and uses
 * {@code ephemeralCache: true} for isolation.
 *
 * <p>FILE-110 — recursion is enabled ONLY by a real boolean {@code true}. {@code FileSchemaFactory}
 * reads it as {@code operand.get("recursive") == Boolean.TRUE} (FileSchemaFactory.java:464), so a
 * JSON STRING {@code "true"} does NOT enable recursion, and the default (absent) is FALSE.
 *
 * <p>FILE-111 — {@code FileSchema} skips any file whose basename startsWith {@code "."}
 * (FileSchema.java:4730 isFileTypeSupported, and 4843 listFilesRecursively): dotfiles such as
 * {@code .hidden.csv} / {@code .aperio} never register as tables.
 *
 * <p>FILE-095 — an unknown table over a FileSchema JDBC connection raises a clear "not found"
 * error, never a silent empty result.
 */
@Tag("unit")
public class CatalogErrorRequirementsTest {

  // ----------------------------------------------------------------- helpers ------------------

  private static void write(Path file, String content) throws Exception {
    Files.createDirectories(file.getParent());
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
  }

  private static void csv(Path file) throws Exception {
    write(file, "id,v\n1,x\n2,y\n");
  }

  private static TreeSet<String> set(String... names) {
    TreeSet<String> s = new TreeSet<>();
    for (String n : names) {
      s.add(n);
    }
    return s;
  }

  /**
   * Builds a model whose {@code recursive} operand is emitted verbatim. Pass {@code "true"}
   * (boolean literal) or {@code "\"true\""} (a JSON string) to exercise the type discrimination at
   * FileSchemaFactory.java:464. When {@code recursiveLiteral} is {@code null} the operand is
   * omitted entirely (exercises the default).
   */
  private static String model(Path dir, String recursiveLiteral) {
    StringBuilder operand = new StringBuilder();
    operand.append("      \"directory\": \"").append(dir.toString().replace("\\", "\\\\")).append("\",\n");
    operand.append("      \"ephemeralCache\": true,\n");
    operand.append("      \"executionEngine\": \"linq4j\",\n");
    operand.append("      \"tableNameCasing\": \"LOWER\",\n");
    operand.append("      \"columnNameCasing\": \"LOWER\"");
    if (recursiveLiteral != null) {
      operand.append(",\n      \"recursive\": ").append(recursiveLiteral);
    }
    operand.append("\n");
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + operand
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  private static Connection connect(String model) throws SQLException {
    Properties info = new Properties();
    info.put("model", "inline:" + model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  /** Lowercased table names discovered in schema {@code s} for the given model. */
  private static TreeSet<String> tables(String model) throws Exception {
    TreeSet<String> names = new TreeSet<>();
    try (Connection conn = connect(model);
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

  // ----------------------------------------------------------------- FILE-110 -----------------

  /**
   * A boolean {@code recursive: true} discovers a nested-subdir file as a table; a STRING
   * {@code recursive: "true"} does NOT (the nested file is absent because the operand is not
   * {@code Boolean.TRUE}); the default (operand absent) is non-recursive. The depth-0 file is
   * always present, isolating recursion as the only variable.
   */
  @Test @Tag("FILE-110") void recursiveBooleanEnablesStringDoesNot(@TempDir Path dir)
      throws Exception {
    csv(dir.resolve("top.csv"));
    csv(dir.resolve("sub/nested.csv"));

    // boolean true -> recursion ON: the nested file registers.
    TreeSet<String> bool = tables(model(dir, "true"));
    assertTrue(bool.contains("top"), "depth-0 file always present");
    assertTrue(bool.contains("sub__nested"),
        "boolean recursive=true discovers the nested file (got " + bool + ")");

    // string "true" -> recursion OFF: == Boolean.TRUE is false for a String, so no recursion.
    TreeSet<String> str = tables(model(dir, "\"true\""));
    assertTrue(str.contains("top"), "depth-0 file always present");
    assertFalse(str.contains("sub__nested"),
        "string recursive=\"true\" does NOT enable recursion (got " + str + ")");

    // default (operand absent) -> recursion OFF.
    TreeSet<String> def = tables(model(dir, null));
    assertTrue(def.contains("top"), "depth-0 file always present");
    assertFalse(def.contains("sub__nested"),
        "default recursive is FALSE (got " + def + ")");
  }

  /**
   * C-01 (pending fix): the INTENDED contract is that recursion should default to true AND that a
   * JSON string {@code "true"} should also enable recursion. This target asserts the intended
   * behavior (string "true" enables recursion -> nested file present); it is {@link Disabled} and
   * will FAIL against the current {@code == Boolean.TRUE} read until the fix lands. It does NOT
   * assert the current (wrong) behavior as passing.
   */
  @Disabled("C-01: default recursive=true AND accept string 'true' — pending code fix")
  @Test @Tag("FILE-110") void recursiveStringTrueShouldEnableRecursion(@TempDir Path dir)
      throws Exception {
    csv(dir.resolve("top.csv"));
    csv(dir.resolve("sub/nested.csv"));

    TreeSet<String> str = tables(model(dir, "\"true\""));
    assertTrue(str.contains("sub__nested"),
        "INTENDED: string recursive=\"true\" should enable recursion and discover the nested file");
  }

  // ----------------------------------------------------------------- FILE-111 -----------------

  /**
   * A directory containing {@code .hidden.csv} and {@code visible.csv} registers only
   * {@code visible}: the dotfile is skipped because its basename startsWith {@code "."}
   * (FileSchema.isFileTypeSupported, FileSchema.java:4730). The default non-recursive walk is used
   * since both files live at depth 0.
   */
  @Test @Tag("FILE-111") void dotfilesAreSkipped(@TempDir Path dir) throws Exception {
    csv(dir.resolve(".hidden.csv"));
    csv(dir.resolve("visible.csv"));

    TreeSet<String> names = tables(model(dir, null));
    assertTrue(names.contains("visible"), "non-dot file registers as a table");
    assertFalse(names.contains("hidden"),
        "dotfile basename startsWith '.' is skipped (got " + names + ")");
    assertFalse(names.contains(".hidden"), "no table is registered for the dotfile");
    // NOTE: isGlobPattern(String) is a private INSTANCE method (FileSchema.java:2402), not static;
    // invoking it requires a fully constructed FileSchema (heavy: needs an ExecutionEngineConfig +
    // baseDirectory wiring), which cannot be built hermetically with only the APIs read here.
    // Its http(s):// -> false contract (lines 2409-2411) is therefore OMITTED rather than guessed.
  }

  // ----------------------------------------------------------------- FILE-095 -----------------

  /**
   * Querying a non-existent table over a FileSchema JDBC connection throws a {@link SQLException}
   * whose message indicates the object is not found — never a silent empty result. A real table is
   * present so the schema itself is valid; only the queried name is unknown.
   */
  @Test @Tag("FILE-095") void unknownTableRaisesClearError(@TempDir Path dir) throws Exception {
    csv(dir.resolve("present.csv"));

    SQLException ex;
    try (Connection conn = connect(model(dir, null));
         Statement st = conn.createStatement()) {
      ex = assertThrows(SQLException.class, () -> {
        try (ResultSet rs = st.executeQuery("select * from s.does_not_exist")) {
          rs.next();
        }
      }, "querying an unknown table must raise SQLException, not return silently");
    }
    String msg = ex.getMessage();
    assertNotNull(msg, "the not-found error must carry a message");
    String lower = msg.toLowerCase();
    assertTrue(lower.contains("not found") || lower.contains("object '"),
        "message must indicate the object is not found (got: " + msg + ")");
  }

  /**
   * C-11 (pending fix): the INTENDED contract is that a table-not-found surfaces a
   * {@link SQLException} carrying SQLState {@code 42S02} ("base table or view not found"). This
   * target asserts that intended SQLState contract; it is {@link Disabled} and will FAIL until the
   * error layer is fixed to set SQLState 42S02 (today the message says "not found" but the SQLState
   * is not the standardized 42S02). It does NOT assert the current (wrong) behavior as passing.
   */
  @Disabled("C-11: FileReaderException should be SQLException carrying SQLState 42S02 "
      + "(table not found) etc. — pending code fix")
  @Test @Tag("FILE-095") void unknownTableShouldCarrySqlState42S02(@TempDir Path dir)
      throws Exception {
    csv(dir.resolve("present.csv"));

    SQLException ex;
    try (Connection conn = connect(model(dir, null));
         Statement st = conn.createStatement()) {
      ex = assertThrows(SQLException.class, () -> {
        try (ResultSet rs = st.executeQuery("select * from s.does_not_exist")) {
          rs.next();
        }
      });
    }
    assertTrue("42S02".equals(ex.getSQLState()),
        "INTENDED: table-not-found must carry SQLState 42S02 (got SQLState: " + ex.getSQLState() + ")");
  }
}
