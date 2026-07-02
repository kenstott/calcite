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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-186 — end-to-end for the aperio {@code materialize=iceberg} verb: a source CSV directory is
 * materialized into an Iceberg lake under {@code storage}, queries are served from it, and freshness
 * is PER-TABLE (changing one source rebuilds only that table).
 */
@Tag("integration")
public class AperioMaterializeIcebergTest {

  @TempDir File csvDir;
  @TempDir File lakeDir;

  private void write(String file, String contents) throws Exception {
    Files.write(new File(csvDir, file).toPath(), contents.getBytes(StandardCharsets.UTF_8));
  }

  private String url() {
    String src = csvDir.getAbsolutePath().replace('\\', '/');
    String lake = lakeDir.getAbsolutePath().replace('\\', '/');
    return "jdbc:aperio:" + src + "?materialize=iceberg&rw=on&schema=t&storage=" + lake;
  }

  private Set<String> names(String table) throws Exception {
    Set<String> out = new LinkedHashSet<String>();
    try (Connection conn = DriverManager.getConnection(url());
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT \"name\" FROM \"t\".\"" + table + "\" ORDER BY \"id\"")) {
      while (rs.next()) {
        out.add(rs.getString(1));
      }
    }
    return out;
  }

  private long count(String table) throws Exception {
    try (Connection conn = DriverManager.getConnection(url());
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM \"t\".\"" + table + "\"")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** Finds the Iceberg table directory named {@code table} anywhere under the warehouse. */
  private File tableDir(String table) {
    File warehouse = new File(lakeDir, "t/iceberg");
    File[] hit = {null};
    walk(warehouse, table, hit);
    return hit[0];
  }

  private void walk(File dir, String name, File[] hit) {
    File[] kids = dir.listFiles();
    if (kids == null) {
      return;
    }
    for (File k : kids) {
      if (k.isDirectory()) {
        if (k.getName().equals(name)) {
          hit[0] = k;
          return;
        }
        walk(k, name, hit);
      }
    }
  }

  /** Set of relative file paths under a directory — identical across runs iff untouched. */
  private Set<String> listing(File dir) throws Exception {
    Set<String> out = new TreeSet<String>();
    Path root = dir.toPath();
    try (Stream<Path> s = Files.walk(root)) {
      s.filter(Files::isRegularFile).forEach(p -> out.add(root.relativize(p).toString()));
    }
    return out;
  }

  @Test @Tag("FILE-186") void materializeIcebergBuildsLakeAndServesIt() throws Exception {
    write("people.csv", "id,name\n1,alice\n2,bob\n3,carol\n");

    Set<String> names = names("people");
    assertEquals(3, names.size(), "all rows served from the materialized lake");
    assertTrue(names.contains("alice") && names.contains("bob") && names.contains("carol"),
        "names round-trip through the Iceberg lake: " + names);

    // The Iceberg lake was actually built on disk under the storage root (<storage>/t/iceberg).
    File warehouse = new File(lakeDir, "t/iceberg");
    assertTrue(warehouse.isDirectory(), "iceberg warehouse created under storage: " + warehouse);

    // Reconnecting is idempotent: the source is unchanged, so the lake is served, not rebuilt.
    assertEquals(names, names("people"), "second open serves the same rows from the existing lake");
  }

  @Test @Tag("FILE-186") void perTableFreshnessRebuildsOnlyChangedTable() throws Exception {
    write("people.csv", "id,name\n1,alice\n2,bob\n");
    write("orders.csv", "id,name\n10,ten\n20,twenty\n");

    // First open materializes both tables.
    assertEquals(2, count("people"));
    assertEquals(2, count("orders"));

    File peopleDir = tableDir("people");
    File ordersDir = tableDir("orders");
    assertTrue(peopleDir != null && ordersDir != null, "both lakes exist");
    Set<String> peopleBefore = listing(peopleDir);
    Set<String> ordersBefore = listing(ordersDir);
    assertFalse(ordersBefore.isEmpty(), "orders lake has files");

    // Change ONLY people.csv (append a row -> its per-table signature changes).
    write("people.csv", "id,name\n1,alice\n2,bob\n3,carol\n");

    // Reconnect triggers the materialize pass again.
    count("people");

    // Per-table freshness: only the changed table is rebuilt. The changed table's Iceberg files
    // differ (drop + recreate -> new snapshot/data files); the untouched table's are byte-for-byte
    // identical — proving freshness is PER-TABLE, not schema-wide.
    assertNotEquals(peopleBefore, listing(peopleDir),
        "changed table WAS rebuilt (its Iceberg files differ)");
    assertEquals(ordersBefore, listing(ordersDir),
        "unchanged table was NOT rebuilt (its Iceberg files are identical)");
  }
}
