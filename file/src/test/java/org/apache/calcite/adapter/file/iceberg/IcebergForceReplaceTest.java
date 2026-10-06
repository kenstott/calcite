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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What a table-level force-reprocess relies on: the accessions carried by the staged files are
 * learned, and deleting them removes them from every year partition, so the replacement appended
 * afterwards is the only copy.
 */
@Tag("unit")
class IcebergForceReplaceTest {
  @TempDir
  Path tempDir;

  private Table table;
  private IcebergTableWriter writer;

  @BeforeEach
  void setUp() {
    Map<String, Object> catalog = new HashMap<String, Object>();
    catalog.put("catalogType", "hadoop");
    catalog.put("warehousePath", tempDir.resolve("warehouse").toString());
    Schema schema = new Schema(
        Types.NestedField.optional(1, "accession_number", Types.StringType.get()),
        Types.NestedField.optional(2, "seq", Types.IntegerType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();
    table = IcebergCatalogManager.createTable(catalog, "force_table", schema, spec);
    writer = new IcebergTableWriter(table, new LocalFileStorageProvider());
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private void put(String accession, int rows, int year) throws IOException {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    for (int i = 1; i <= rows; i++) {
      Map<String, Object> r = new LinkedHashMap<String, Object>();
      r.put("accession_number", accession);
      r.put("seq", Integer.valueOf(i));
      r.put("year", Integer.valueOf(year));
      out.add(r);
    }
    DataFile f = writer.writeRecords(out, Collections.singletonMap("year", String.valueOf(year)));
    writer.commitDataFiles(Collections.singletonList(f), null);
  }

  private Map<String, Integer> counts() throws IOException {
    table.refresh();
    Map<String, Integer> counts = new TreeMap<String, Integer>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        String key = r.getField("accession_number") + "@" + r.getField("year");
        Integer n = counts.get(key);
        counts.put(key, n == null ? 1 : n + 1);
      }
    }
    return counts;
  }

  @Test void deletingAnAccessionRemovesItFromEveryYearPartition() throws IOException {
    // A filing staged under one year directory but stored under the fiscal year before it, and a
    // second copy of it under the staging year: the forced delete must clear both.
    put("A", 2, 2024);
    put("A", 2, 2025);
    put("B", 3, 2024);
    writer.deleteRows("accession_number", Collections.singleton("A"));
    assertEquals(Collections.singletonMap("B@2024", 3), counts());
  }

  @Test void deleteThenAppendLeavesExactlyOneCopyOfAForcedAccession() throws IOException {
    put("A", 2, 2024);
    put("A", 2, 2024);   // an earlier duplicate
    put("B", 3, 2024);
    writer.deleteRows("accession_number", Collections.singleton("A"));
    put("A", 2, 2024);   // the replacement
    Map<String, Integer> expected = new TreeMap<String, Integer>();
    expected.put("A@2024", 2);
    expected.put("B@2024", 3);
    assertEquals(expected, counts());
  }

  @Test void distinctValuesInFilesReadsTheAccessionsOfEveryFile() throws Exception {
    Path one = tempDir.resolve("one.parquet");
    Path two = tempDir.resolve("two.parquet");
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("COPY (SELECT * FROM (VALUES ('A',1),('A',2),('B',1)) t(accession_number, seq)) "
          + "TO '" + one + "' (FORMAT PARQUET)");
      st.execute("COPY (SELECT * FROM (VALUES ('C',1),('A',3),(NULL,9)) t(accession_number, seq)) "
          + "TO '" + two + "' (FORMAT PARQUET)");
      Set<String> got = IcebergMaterializer.distinctValuesInFiles(c,
          Arrays.asList(one.toString(), two.toString()), "accession_number");
      assertEquals(new HashSet<String>(Arrays.asList("A", "B", "C")), got);
      assertEquals(Collections.<String>emptySet(),
          IcebergMaterializer.distinctValuesInFiles(c, Collections.<String>emptyList(),
              "accession_number"));
    }
  }

  @Test void aColumnNameThatIsNotAnIdentifierIsRejected() throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      assertThrows(IllegalArgumentException.class, () ->
          IcebergMaterializer.distinctValuesInFiles(c,
              Collections.singletonList(Files.createTempFile(tempDir, "x", ".parquet").toString()),
              "accession_number; DROP TABLE t"));
    }
  }
}
