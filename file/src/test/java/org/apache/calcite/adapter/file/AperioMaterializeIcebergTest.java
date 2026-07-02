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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-186 — end-to-end for the aperio {@code materialize=iceberg} verb: a source CSV directory is
 * materialized into an Iceberg lake under {@code storage}, and queries are served from it.
 */
@Tag("integration")
public class AperioMaterializeIcebergTest {

  @TempDir File csvDir;
  @TempDir File lakeDir;

  private void writeCsv() throws Exception {
    File csv = new File(csvDir, "people.csv");
    Files.write(csv.toPath(),
        "id,name\n1,alice\n2,bob\n3,carol\n".getBytes(StandardCharsets.UTF_8));
  }

  private String url() {
    String src = csvDir.getAbsolutePath().replace('\\', '/');
    String lake = lakeDir.getAbsolutePath().replace('\\', '/');
    return "jdbc:aperio:" + src
        + "?materialize=iceberg&rw=on&schema=t&storage=" + lake;
  }

  private Set<String> queryNames() throws Exception {
    Set<String> names = new LinkedHashSet<String>();
    try (Connection conn = DriverManager.getConnection(url());
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT \"id\", \"name\" FROM \"t\".\"people\" ORDER BY \"id\"")) {
      while (rs.next()) {
        names.add(rs.getString("name"));
      }
    }
    return names;
  }

  @Test @Tag("FILE-186") void materializeIcebergBuildsLakeAndServesIt() throws Exception {
    writeCsv();

    Set<String> names = queryNames();
    assertEquals(3, names.size(), "all rows served from the materialized lake");
    assertTrue(names.contains("alice") && names.contains("bob") && names.contains("carol"),
        "names round-trip through the Iceberg lake: " + names);

    // The Iceberg lake was actually built on disk under the storage root (<storage>/t/iceberg).
    File warehouse = new File(lakeDir, "t/iceberg");
    assertTrue(warehouse.isDirectory(), "iceberg warehouse created under storage: " + warehouse);
    File[] contents = warehouse.listFiles();
    assertTrue(contents != null && contents.length > 0,
        "warehouse holds the materialized table metadata");

    // Reconnecting is idempotent: the lake is fresh (unchanged source), so it is served, not rebuilt.
    assertEquals(names, queryNames(), "second open serves the same rows from the existing lake");
  }
}
