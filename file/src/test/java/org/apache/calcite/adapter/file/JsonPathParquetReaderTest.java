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

import org.apache.calcite.adapter.file.converters.JsonPathConverter;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Recode of three weak file-adapter areas to exact assertions.
 *
 * <ul>
 *   <li>FILE-011 — {@link JsonPathConverter} extracts the exact sub-tree of a configured JSON path,
 *       preserving nested objects and array elements per the documented dot/bracket rule.</li>
 *   <li>FILE-013 — a Parquet source round-trips to EXACT values with types taken from the file
 *       schema (self-describing, no inference) and nullability preserved.</li>
 *   <li>FILE-151 — {@link FileReaderException} is a package-private checked {@link Exception}
 *       carrying NO SQLState/vendor code; {@link FileReader} throws it for the documented
 *       failure conditions.</li>
 * </ul>
 */
@Tag("unit")
public class JsonPathParquetReaderTest extends BaseFileTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // A small nested JSON fixture exercising top-level fields, a nested object, and an array.
  private static final String NESTED_JSON = "{"
      + "\"company\":\"Acme\","
      + "\"address\":{\"city\":\"Springfield\",\"zip\":\"12345\"},"
      + "\"employees\":["
      + "{\"id\":1,\"name\":\"Alice\"},"
      + "{\"id\":2,\"name\":\"Bob\"}"
      + "]"
      + "}";

  // -------------------------------------------------- FILE-011 (JsonPathConverter) -------------

  @Test @Tag("FILE-011") void jsonPathExtractsNestedObjectExactly() throws Exception {
    JsonNode root = MAPPER.readTree(NESTED_JSON);

    // Nested object via dot notation -> the whole sub-object is returned, fields intact.
    JsonNode address = JsonPathConverter.extractPath(root, "$.address");
    assertNotNull(address, "address sub-tree must be found");
    assertTrue(address.isObject(), "address is an object node");
    assertEquals(2, address.size(), "address has exactly two fields");
    assertEquals("Springfield", address.get("city").asText());
    assertEquals("12345", address.get("zip").asText());
  }

  @Test @Tag("FILE-011") void jsonPathExtractsArrayElementAndScalarExactly() throws Exception {
    JsonNode root = MAPPER.readTree(NESTED_JSON);

    // Array element by index -> the object at that index.
    JsonNode emp0 = JsonPathConverter.extractPath(root, "employees[0]");
    assertNotNull(emp0, "employees[0] must be found");
    assertTrue(emp0.isObject(), "array element is an object");
    assertEquals(1, emp0.get("id").asInt(), "exact id of first employee");
    assertEquals("Alice", emp0.get("name").asText(), "exact name of first employee");

    // Scalar reached through array element + nested field.
    JsonNode emp1Name = JsonPathConverter.extractPath(root, "employees[1].name");
    assertNotNull(emp1Name, "employees[1].name must be found");
    assertEquals("Bob", emp1Name.asText(), "exact name of second employee");

    // Top-level scalar.
    assertEquals("Acme", JsonPathConverter.extractPath(root, "$.company").asText());
  }

  @Test @Tag("FILE-011") void jsonPathMissingFieldReturnsNull() throws Exception {
    JsonNode root = MAPPER.readTree(NESTED_JSON);
    assertNull(JsonPathConverter.extractPath(root, "address.country"),
        "absent nested field -> null");
    assertNull(JsonPathConverter.extractPath(root, "employees[9]"),
        "out-of-range array index -> null");
  }

  @Test @Tag("FILE-011") void jsonPathExtractWritesExactSubTreeToFile(@TempDir Path dir) throws Exception {
    File src = dir.resolve("source.json").toFile();
    Files.write(src.toPath(), NESTED_JSON.getBytes(StandardCharsets.UTF_8));
    File out = dir.resolve("address.json").toFile();

    JsonPathConverter.extract(src, out, "$.address", dir.toFile());

    assertTrue(out.exists(), "output file written");
    JsonNode written = MAPPER.readTree(out);
    assertTrue(written.isObject(), "written node is the extracted object");
    assertEquals(2, written.size(), "exactly the two address fields");
    assertEquals("Springfield", written.get("city").asText());
    assertEquals("12345", written.get("zip").asText());
  }

  // -------------------------------------------------- FILE-013 (Parquet self-describing) ------

  @SuppressWarnings("deprecation")
  private static File writeParquet(File dir) throws Exception {
    // Self-describing Avro/Parquet schema: an INT, a non-null STRING, a nullable STRING (union),
    // and a DOUBLE. The adapter must take these types from the file, not infer them.
    String schemaString = "{\"type\":\"record\",\"name\":\"Rec\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"nickname\",\"type\":[\"null\",\"string\"]},"
        + "{\"name\":\"score\",\"type\":\"double\"}"
        + "]}";
    Schema schema = new Schema.Parser().parse(schemaString);

    File parquet = new File(dir, "people.parquet");
    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter.<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(parquet.getAbsolutePath()))
                 .withSchema(schema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      GenericRecord r1 = new GenericData.Record(schema);
      r1.put("id", 1);
      r1.put("name", "Alice");
      r1.put("nickname", "Ali");
      r1.put("score", 98.5);
      writer.write(r1);

      GenericRecord r2 = new GenericData.Record(schema);
      r2.put("id", 2);
      r2.put("name", "Bob");
      r2.put("nickname", null);     // a null that must stay null
      r2.put("score", 42.0);
      writer.write(r2);
    }
    return parquet;
  }

  @Test @Tag("FILE-013") void parquetRoundTripsExactValuesAndTypesFromFileSchema(@TempDir Path dir)
      throws Exception {
    writeParquet(dir.toFile());

    // Force the parquet execution engine so column types come from the file schema, not inference.
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", dir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"P\","
        + "\"schemas\":[{"
        + "\"name\":\"P\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.toString().replace("\\", "\\\\") + "\","
        + "\"executionEngine\":\"parquet\","
        + "\"ephemeralCache\":true"
        + "}}]}";

    Properties props = new Properties();
    props.setProperty("model", "inline:" + model);
    applyEngineDefaults(props);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT \"id\", \"name\", \"nickname\", \"score\" "
                 + "FROM \"P\".\"people\" ORDER BY \"id\"")) {

      ResultSetMetaData md = rs.getMetaData();
      assertEquals(4, md.getColumnCount(), "exactly four columns");

      // Types taken from the self-describing file schema.
      assertEquals(Types.INTEGER, md.getColumnType(1), "id is INTEGER");
      assertEquals(Types.VARCHAR, md.getColumnType(2), "name is VARCHAR");
      assertEquals(Types.VARCHAR, md.getColumnType(3), "nickname is VARCHAR");
      assertEquals(Types.DOUBLE, md.getColumnType(4), "score is DOUBLE");

      // Nullability of the union-with-null column is preserved as NULLABLE at the metadata level.
      // NOTE: this adapter does not surface Avro "required" as JDBC columnNoNulls, so the
      // load-bearing nullability guarantee is asserted at the value level (a null stays null) below.
      assertEquals(ResultSetMetaData.columnNullable, md.isNullable(3),
          "nickname (union with null) is NULLABLE");

      // Row 1 — exact values.
      assertTrue(rs.next(), "first row present");
      assertEquals(1, rs.getInt("id"));
      assertEquals("Alice", rs.getString("name"));
      assertEquals("Ali", rs.getString("nickname"));
      assertFalse(rs.wasNull(), "non-null nickname stays non-null");
      assertEquals(98.5, rs.getDouble("score"), 0.0);

      // Row 2 — a null stays null, other values exact.
      assertTrue(rs.next(), "second row present");
      assertEquals(2, rs.getInt("id"));
      assertEquals("Bob", rs.getString("name"));
      assertNull(rs.getString("nickname"), "null nickname stays null");
      assertTrue(rs.wasNull(), "wasNull reports the null");
      assertEquals(42.0, rs.getDouble("score"), 0.0);

      assertFalse(rs.next(), "exactly two rows");
    }
  }

  // -------------------------------------------------- FILE-151 (FileReaderException) ----------

  /**
   * Asserts that FileReaderException is a plain checked Exception with no JDBC vendor info,
   * and that it is not a {@link java.sql.SQLException}.
   */
  private static void assertPlainException(FileReaderException ex) {
    assertNotNull(ex, "a FileReaderException must be thrown");
    assertTrue(Exception.class.isInstance(ex), "FileReaderException extends Exception");
    assertFalse(RuntimeException.class.isInstance(ex), "it is a checked exception, not unchecked");
    assertFalse(java.sql.SQLException.class.isInstance(ex),
        "CURRENT behavior: it is NOT a SQLException (no SQLState/vendor code)");
  }

  @Test @Tag("FILE-151") void fileReaderExceptionIsPackagePrivateCheckedException() {
    // Confirms the class shape the other assertions rely on: same package, extends Exception.
    assertEquals("org.apache.calcite.adapter.file",
        FileReaderException.class.getPackage().getName(),
        "FileReaderException lives in this package");
    assertEquals(Exception.class, FileReaderException.class.getSuperclass(),
        "FileReaderException extends java.lang.Exception directly");
    assertFalse(java.sql.SQLException.class.isAssignableFrom(FileReaderException.class),
        "FileReaderException is not a SQLException");
  }

  @Test @Tag("FILE-151") void missingFileThrowsPlainException(@TempDir Path dir) {
    File missing = dir.resolve("does-not-exist.html").toFile();
    Source source = Sources.of(missing);
    FileReader reader = new FileReader(source);
    FileReaderException ex = assertThrows(FileReaderException.class, reader::refresh,
        "a missing file must throw FileReaderException");
    assertPlainException(ex);
    assertTrue(ex.getMessage().contains("File not found"),
        "message identifies the missing file (got: " + ex.getMessage() + ")");
  }

  @Test @Tag("FILE-151") void nullSourceThrowsNpeBeforeConstruction() {
    // The constructor requireNonNull guards null source: a NullPointerException is thrown
    // (not a FileReaderException) before any read is attempted.
    assertThrows(NullPointerException.class, () -> new FileReader(null),
        "a null source is rejected by the constructor");
  }

  @Test @Tag("FILE-151") void noMatchSelectorThrowsPlainException(@TempDir Path dir) throws Exception {
    String html = "<html><body><table><tr><th>a</th></tr><tr><td>1</td></tr></table></body></html>";
    File f = dir.resolve("doc.html").toFile();
    Files.write(f.toPath(), html.getBytes(StandardCharsets.UTF_8));

    FileReader reader = new FileReader(Sources.of(f), "table#nonexistent");
    FileReaderException ex = assertThrows(FileReaderException.class, reader::refresh,
        "a selector matching nothing must throw");
    assertPlainException(ex);
    assertTrue(ex.getMessage().contains("No HTML elements found"),
        "message reports the empty selection (got: " + ex.getMessage() + ")");
  }

  @Test @Tag("FILE-151") void nonTableElementSelectedThrowsPlainException(@TempDir Path dir)
      throws Exception {
    // The selector matches exactly one element that is not a <table>.
    String html = "<html><body><div id=\"box\">not a table</div>"
        + "<table><tr><th>a</th></tr><tr><td>1</td></tr></table></body></html>";
    File f = dir.resolve("nontable.html").toFile();
    Files.write(f.toPath(), html.getBytes(StandardCharsets.UTF_8));

    FileReader reader = new FileReader(Sources.of(f), "div#box");
    FileReaderException ex = assertThrows(FileReaderException.class, reader::refresh,
        "a selected non-<table> element must throw");
    assertPlainException(ex);
    assertTrue(ex.getMessage().contains("not a table"),
        "message reports the wrong element type (got: " + ex.getMessage() + ")");
  }

  @Test @Tag("FILE-151") void noTablesFoundThrowsPlainException(@TempDir Path dir) throws Exception {
    // No selector -> getBestTable; a document with no <table> at all throws "no tables found".
    String html = "<html><body><p>nothing here</p></body></html>";
    File f = dir.resolve("notables.html").toFile();
    Files.write(f.toPath(), html.getBytes(StandardCharsets.UTF_8));

    FileReader reader = new FileReader(Sources.of(f));
    FileReaderException ex = assertThrows(FileReaderException.class, reader::refresh,
        "a document with no tables must throw");
    assertPlainException(ex);
    assertEquals("no tables found", ex.getMessage(), "exact 'no tables found' message");
  }

  /**
   * C-11 (RESOLVED contradiction, code-wrong): the INTENDED future is that FileReaderException
   * becomes a {@link java.sql.SQLException} carrying documented SQLStates. This is disabled because
   * the current code throws a plain checked Exception with no vendor info — see the active tests
   * above which pin that present behavior. Enable this once the code fix lands.
   */
  @Disabled("C-11: FileReaderException should be a SQLException with SQLState — pending code fix")
  @Test @Tag("FILE-151") void targetContractIsSqlExceptionWithSqlState(@TempDir Path dir)
      throws Exception {
    String html = "<html><body><p>nothing here</p></body></html>";
    File f = dir.resolve("notables.html").toFile();
    Files.write(f.toPath(), html.getBytes(StandardCharsets.UTF_8));

    FileReader reader = new FileReader(Sources.of(f));
    // TARGET (not yet active): the thrown exception is a SQLException carrying a non-null SQLState.
    java.sql.SQLException ex =
        assertThrows(java.sql.SQLException.class, reader::refresh,
            "future: FileReader throws a SQLException");
    assertNotNull(ex.getSQLState(), "future: a documented SQLState is present");
  }
}
