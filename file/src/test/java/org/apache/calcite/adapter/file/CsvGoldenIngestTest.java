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

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reference pattern for file-adapter ingestion tests.
 *
 * <p>This is the "solid, repeatable pattern" the file/govdata suites should be rebuilt on. Unlike
 * the legacy {@code CsvTypeInferenceTest} — which asserts {@code idType == INTEGER || idType ==
 * BIGINT}, {@code count > 0}, and never checks a single row value — every test here pins the
 * <i>exact</i> contract:
 *
 * <ol>
 *   <li><b>Golden round-trip</b> — ingest a known fixture, {@code SELECT *}, and assert the whole
 *       table equals a committed expected matrix, cell for cell, with SQL NULL distinguished from
 *       the empty string.</li>
 *   <li><b>Exact type inference</b> — assert the precise {@link SqlTypeName} per column (no OR-ing
 *       across "acceptable" types), via the engine-independent inferrer.</li>
 *   <li><b>Null-sentinel fidelity</b> — pin exactly which textual sentinels (NULL, NA, N/A, NONE,
 *       nil, …) collapse to SQL NULL and which survive as data.</li>
 *   <li><b>Idempotent re-ingestion</b> — ingest the same bytes twice into the same persistent
 *       cache and assert the materialized table is identical. This is the invariant the legacy
 *       suite never tested and the one whose absence produced the resume/re-write bugs.</li>
 * </ol>
 *
 * <p>The execution engine is pinned to {@code linq4j} (the reference row-by-row engine) so the
 * golden values are deterministic and independent of which optional engines are on the classpath.
 * Temporal expectations are canonical JDBC string renderings, never locale-formatted display text.
 */
@Tag("unit")
public class CsvGoldenIngestTest {

  /** Sentinel printed in golden matrices for a SQL NULL, so it is never confused with "". */
  private static final String NULL = "␀"; // ␀

  private static final String FIXTURE_DIR =
      new File(CsvGoldenIngestTest.class.getResource("/csv-type-inference").getFile())
          .getAbsolutePath();

  // ---------------------------------------------------------------------------------------------
  // 1. Golden round-trip: the whole table, value for value.
  // ---------------------------------------------------------------------------------------------

  @Test @Tag("FILE-002") void mixedTypes_fullTableMatchesGolden(@TempDir Path cache) throws Exception {
    String[][] expected = {
        {"1", "John Doe", "30", "75000.5", "2020-03-15", "true", "4.5", "09:30:00", "Started in sales"},
        {"2", "Jane Smith", "28", "82000.0", "2021-06-01", "true", "4.8", "08:45:30", "Senior developer"},
        {"3", "Bob Johnson", "45", "95000.75", "2015-11-20", "false", "3.9", "10:15:00", "On sabbatical"},
        {"4", "Alice Brown", "32", "68500.25", "2019-08-10", "true", "4.2", "09:00:00", "Team lead"},
        {"5", "Charlie Wilson", NULL, "72000.0", "2022-01-05", "true", "4.6", "08:30:00", "New hire"},
        // NOTE: row 6's empty notes is preserved as "" (blank in a VARCHAR column), whereas row 9's
        // literal "NULL" collapses to SQL NULL. The golden pins this distinction exactly — the kind
        // of contract the legacy contains()-based tests could never see.
        {"6", "Diana Martinez", "38", "88000.5", "2018-04-22", "true", "4.9", "09:45:00", ""},
        {"7", "Edward Lee", "41", "91000.0", "2016-12-15", "false", "3.7", "11:00:00", "Remote worker"},
        {"8", "Fiona Taylor", "29", "77500.75", "2020-09-30", "true", "4.4", "08:00:00", "Product manager"},
        {"9", "George Harris", "35", "83000.0", "2019-02-14", "true", "4.7", "09:30:00", NULL},
        {"10", "Helen White", "31", "79000.5", "2021-03-01", "true", "4.3", "10:00:00", "Marketing specialist"},
    };

    List<String[]> actual = query(cache, "SELECT * FROM g.mixed_types ORDER BY id");

    assertMatrix(expected, actual);
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Exact type inference (engine-independent, no OR-ing).
  // ---------------------------------------------------------------------------------------------

  @Test @Tag("FILE-001") void mixedTypes_inferredTypesAreExact() throws Exception {
    File csv = new File(FIXTURE_DIR, "mixed-types.csv");
    CsvTypeInferrer.TypeInferenceConfig config =
        new CsvTypeInferrer.TypeInferenceConfig(true, 1.0, 100, 0.95, true, true, true, true, 0.0);

    List<CsvTypeInferrer.ColumnTypeInfo> types =
        CsvTypeInferrer.inferTypes(Sources.of(csv), config, "UNCHANGED");

    SqlTypeName[] expected = {
        SqlTypeName.INTEGER,  // id
        SqlTypeName.VARCHAR,  // name
        SqlTypeName.INTEGER,  // age
        SqlTypeName.DOUBLE,   // salary
        SqlTypeName.DATE,     // hire_date
        SqlTypeName.BOOLEAN,  // is_active
        SqlTypeName.DOUBLE,   // rating
        SqlTypeName.TIME,     // login_time
        SqlTypeName.VARCHAR,  // notes
    };

    assertEquals(expected.length, types.size(), "column count");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], types.get(i).inferredType,
          "column " + i + " inferred type");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 3. Null-sentinel fidelity: exactly which cells collapse to SQL NULL.
  // ---------------------------------------------------------------------------------------------

  @Test @Tag("FILE-003") void nullsAndEmpty_sentinelsCollapseToNull(@TempDir Path cache) throws Exception {
    String[][] expected = {
        {"1", "100", "200.5", "true", "2024-01-01"},
        {"2", NULL, "250.75", "false", "2024-01-02"},
        {"3", "300", NULL, "true", NULL},
        {"4", "400", "350.25", NULL, "2024-01-04"},
        {"5", NULL, NULL, NULL, NULL},
        {"6", NULL, NULL, NULL, NULL},
        {"7", NULL, NULL, NULL, NULL},
        {"8", "500", "450.5", "true", "2024-01-08"},
        {"9", NULL, NULL, NULL, NULL},
        {"10", "600", "550.75", "false", "2024-01-10"},
    };

    List<String[]> actual = query(cache, "SELECT * FROM g.nulls_and_empty ORDER BY id");

    assertMatrix(expected, actual);
  }

  // ---------------------------------------------------------------------------------------------
  // 4. Idempotent re-ingestion: same bytes twice into the same cache → identical table.
  // ---------------------------------------------------------------------------------------------

  @Test @Tag("FILE-004") void mixedTypes_reingestionIsIdempotent(@TempDir Path cache) throws Exception {
    String sql = "SELECT * FROM g.mixed_types ORDER BY id";
    List<String[]> firstRun = query(cache, sql);
    List<String[]> secondRun = query(cache, sql); // reuses the persistent cache from the first run

    assertMatrix(firstRun.toArray(new String[0][]), secondRun);
  }

  // ---------------------------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------------------------

  /** Runs a query against a freshly opened connection and captures the result as a text matrix. */
  private static List<String[]> query(Path cache, String sql) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(cache));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    List<String[]> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.createStatement().executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      while (rs.next()) {
        String[] row = new String[cols];
        for (int i = 1; i <= cols; i++) {
          Object v = rs.getObject(i);
          row[i - 1] = rs.wasNull() || v == null ? NULL : v.toString();
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /** Model with a persistent cache, type inference on, engine pinned for deterministic goldens. */
  private static String model(Path cache) {
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"g\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"g\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + FIXTURE_DIR.replace("\\", "\\\\") + "\",\n"
        + "      \"baseDirectory\": \"" + cache.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"ephemeralCache\": false,\n"
        + "      \"executionEngine\": \"linq4j\",\n"
        + "      \"csvTypeInference\": {\n"
        + "        \"enabled\": true,\n"
        + "        \"samplingRate\": 1.0,\n"
        + "        \"maxSampleRows\": 100,\n"
        + "        \"confidenceThreshold\": 0.9,\n"
        + "        \"makeAllNullable\": true,\n"
        + "        \"inferDates\": true,\n"
        + "        \"inferTimes\": true,\n"
        + "        \"inferTimestamps\": true\n"
        + "      }\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  /** Asserts two text matrices are identical, dumping the full actual matrix on any mismatch. */
  private static void assertMatrix(String[][] expected, List<String[]> actual) {
    String exp = render(Arrays.asList(expected));
    String act = render(actual);
    assertEquals(exp, act); // single diff shows the whole table — copy actual to lock a new golden
  }

  private static String render(List<String[]> rows) {
    StringBuilder sb = new StringBuilder();
    for (String[] row : rows) {
      sb.append(String.join(" | ", row)).append('\n');
    }
    return sb.toString();
  }
}
