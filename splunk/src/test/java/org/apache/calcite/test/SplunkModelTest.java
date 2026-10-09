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
package org.apache.calcite.test;

import org.apache.calcite.jdbc.CalciteConnection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Splunk adapter using model JSON file.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
class SplunkModelTest {


  /** Writes the model a user would write, pointing at the configured server. */
  private static File modelFile(File directory) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode operand = mapper.createObjectNode();
    operand.put("url", SplunkTestSettings.url());
    operand.put("username", SplunkTestSettings.user());
    operand.put("password", SplunkTestSettings.password());
    operand.put("disableSslValidation", true);
    operand.put("disableDynamicDiscovery", false);
    ObjectNode schema = mapper.createObjectNode();
    schema.put("name", "splunk");
    schema.put("type", "custom");
    schema.put("factory", "org.apache.calcite.adapter.splunk.SplunkSchemaFactory");
    schema.set("operand", operand);
    ObjectNode model = mapper.createObjectNode();
    model.put("version", "1.0");
    model.put("defaultSchema", "splunk");
    model.putArray("schemas").add(schema);
    File file = new File(directory, "splunk-model.json");
    mapper.writeValue(file, model);
    return file;
  }

  @Test void testWithModelFile(@TempDir File directory) throws Exception {
    System.out.println("\n=== Testing with Model File ===");

    Properties info = new Properties();
    info.put("model", modelFile(directory).getAbsolutePath());

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      DatabaseMetaData metaData = calciteConn.getMetaData();

      System.out.println("\nSchemas:");
      try (ResultSet rs = metaData.getSchemas()) {
        while (rs.next()) {
          String schemaName = rs.getString("TABLE_SCHEM");
          System.out.println("  - " + schemaName);
        }
      }

      System.out.println("\nTables in 'splunk' schema:");
      int count = 0;

      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          System.out.println("  - " + tableName);
          count++;
        }
      }

      System.out.println("\nTotal tables discovered: " + count);
      assertTrue(count > 0, "Should discover some tables");
    }
  }
}
