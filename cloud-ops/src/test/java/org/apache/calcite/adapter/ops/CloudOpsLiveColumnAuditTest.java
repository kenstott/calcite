/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ops;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reads every table from the live clouds configured in
 * {@code src/test/resources/local-test.properties} and writes, per table and cloud provider, how
 * many rows came back and which columns were null in all of them, to
 * {@code build/reports/cloudops-column-audit.txt}. It fails only if a table cannot be read.
 */
@Tag("integration")
public class CloudOpsLiveColumnAuditTest {

  private static final String[] TABLES = {
      "compute_resources", "storage_resources", "kubernetes_clusters", "container_registries",
      "network_resources", "iam_resources", "database_resources", "compute_security_groups"};

  @Test public void auditColumns() throws SQLException, IOException {
    Properties config = new Properties();
    try (InputStream in = new FileInputStream("src/test/resources/local-test.properties")) {
      config.load(in);
    }
    Path report = Paths.get("build", "reports", "cloudops-column-audit.txt");
    Files.createDirectories(report.getParent());
    try (Connection connection = new CloudOpsDriver().connect("jdbc:cloudops:", config);
         PrintWriter out =
             new PrintWriter(Files.newBufferedWriter(report, StandardCharsets.UTF_8))) {
      for (String table : TABLES) {
        audit(connection, table, out);
      }
    }
  }

  private static void audit(Connection connection, String table, PrintWriter out)
      throws SQLException {
    // provider -> [row count, non-null count per column]
    Map<String, int[]> nonNull = new LinkedHashMap<String, int[]>();
    Map<String, Integer> rows = new LinkedHashMap<String, Integer>();
    Map<String, List<String>> samples = new LinkedHashMap<String, List<String>>();
    List<String> columns = new ArrayList<String>();
    try (Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT * FROM \"cloud\".\"" + table + "\"")) {
      ResultSetMetaData metaData = rs.getMetaData();
      int providerColumn = 0;
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columns.add(metaData.getColumnName(i));
        if ("cloud_provider".equals(metaData.getColumnName(i))) {
          providerColumn = i;
        }
      }
      while (rs.next()) {
        String provider = String.valueOf(rs.getObject(providerColumn));
        if (!nonNull.containsKey(provider)) {
          nonNull.put(provider, new int[columns.size()]);
          rows.put(provider, 0);
          samples.put(provider, new ArrayList<String>());
        }
        rows.put(provider, rows.get(provider) + 1);
        StringBuilder sample = new StringBuilder();
        for (int i = 1; i <= columns.size(); i++) {
          Object value = rs.getObject(i);
          if (value != null) {
            nonNull.get(provider)[i - 1]++;
          }
          String text = String.valueOf(value);
          sample.append(columns.get(i - 1)).append('=')
              .append(text.length() > 60 ? text.substring(0, 60) + "..." : text).append("; ");
        }
        if (samples.get(provider).size() < 8) {
          samples.get(provider).add(sample.toString());
        }
      }
    }
    out.println("== " + table);
    if (rows.isEmpty()) {
      out.println("   (no rows from any provider)");
    }
    for (Map.Entry<String, Integer> entry : rows.entrySet()) {
      List<String> allNull = new ArrayList<String>();
      for (int i = 0; i < columns.size(); i++) {
        if (nonNull.get(entry.getKey())[i] == 0) {
          allNull.add(columns.get(i));
        }
      }
      out.println("   " + entry.getKey() + ": " + entry.getValue() + " rows; always null: " + allNull);
      for (String sample : samples.get(entry.getKey())) {
        out.println("      e.g. " + sample);
      }
    }
    out.flush();
  }
}
