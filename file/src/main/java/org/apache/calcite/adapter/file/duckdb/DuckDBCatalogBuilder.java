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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Set;

/**
 * Command-line tool to build a persistent DuckDB catalog from a Calcite model.json.
 *
 * <p>This tool connects to Calcite using a model.json configuration and forces all
 * schemas to initialize, creating DuckDB views for all discovered tables. The resulting
 * DuckDB catalog file contains view definitions and macros but not the actual data,
 * making it small and distributable.
 *
 * <p>Usage:
 * <pre>
 * java -cp ... org.apache.calcite.adapter.file.duckdb.DuckDBCatalogBuilder \
 *   /path/to/model.json \
 *   /path/to/output.duckdb
 * </pre>
 *
 * <p>Environment variables:
 * <ul>
 *   <li>GOVDATA_CACHE_DIR - Cache directory for raw downloads (optional)
 *   <li>GOVDATA_PARQUET_DIR - Directory for parquet files (optional)
 *   <li>AWS_ACCESS_KEY_ID - AWS credentials for S3 (optional)
 *   <li>AWS_SECRET_ACCESS_KEY - AWS credentials for S3 (optional)
 * </ul>
 *
 * <p>The catalog path is automatically set via system property before connection.
 */
public class DuckDBCatalogBuilder {

  @SuppressWarnings("deprecation")
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: DuckDBCatalogBuilder <model.json> <output.duckdb>");
      System.err.println();
      System.err.println("Example:");
      System.err.println("  DuckDBCatalogBuilder model.json catalog.duckdb");
      System.err.println();
      System.err.println("Environment variables (optional):");
      System.err.println("  GOVDATA_CACHE_DIR      - Cache directory for raw downloads");
      System.err.println("  GOVDATA_PARQUET_DIR    - Directory for parquet files");
      System.err.println("  AWS_ACCESS_KEY_ID      - AWS credentials for S3");
      System.err.println("  AWS_SECRET_ACCESS_KEY  - AWS credentials for S3");
      System.exit(1);
    }

    String modelPath = new File(args[0]).getAbsolutePath();
    String catalogPath = new File(args[1]).getAbsolutePath();

    System.out.println("=========================================");
    System.out.println("DuckDB Catalog Builder");
    System.out.println("=========================================");
    System.out.println("Model:   " + modelPath);
    System.out.println("Catalog: " + catalogPath);
    System.out.println("=========================================");
    System.out.println();

    // Validate model file exists
    File modelFile = new File(modelPath);
    if (!modelFile.exists()) {
      System.err.println("Error: Model file not found: " + modelPath);
      System.exit(1);
    }

    // Remove existing catalog if present
    File catalogFile = new File(catalogPath);
    if (catalogFile.exists()) {
      System.out.println("Removing existing catalog: " + catalogPath);
      if (!catalogFile.delete()) {
        System.err.println("Warning: Failed to delete existing catalog");
      }
    }

    // Set system property for DuckDB catalog path (checked by DuckDBJdbcSchemaFactory)
    System.setProperty("duckdb.catalog.path", catalogPath);
    System.out.println("Set catalog path: " + catalogPath);
    System.out.println();

    // Build JDBC URL
    String jdbcUrl = "jdbc:calcite:model=" + modelPath;
    System.out.println("Connecting to Calcite...");
    System.out.println("JDBC URL: " + jdbcUrl);
    System.out.println();

    try {
      // Load Calcite driver
      Class.forName("org.apache.calcite.jdbc.Driver");

      // Connect and force schema initialization
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConn.getRootSchema();

        System.out.println("Enumerating schemas from model...");
        Set<String> schemaNames = rootSchema.getSubSchemaNames();
        System.out.println("Found " + schemaNames.size() + " schema(s): " + schemaNames);
        System.out.println();

        // Force each schema to initialize by accessing its tables
        int totalTables = 0;
        for (String schemaName : schemaNames) {
          // Skip metadata schemas
          if (schemaName.equals("metadata") || schemaName.equalsIgnoreCase("INFORMATION_SCHEMA")) {
            continue;
          }

          SchemaPlus schema = rootSchema.getSubSchema(schemaName);
          if (schema != null) {
            System.out.println("Initializing schema: " + schemaName);

            // Force initialization by getting table names
            // This triggers schema factory to discover files and register DuckDB views
            Set<String> tableNames = schema.getTableNames();
            System.out.println("  Tables: " + tableNames.size());

            // Log first few tables as confirmation
            int count = 0;
            for (String tableName : tableNames) {
              if (count < 5) {
                System.out.println("    - " + tableName);
                count++;
              } else if (count == 5) {
                System.out.println("    ... and " + (tableNames.size() - 5) + " more");
                break;
              }
            }

            totalTables += tableNames.size();
            System.out.println();
          }
        }

        System.out.println("=========================================");
        System.out.println("Catalog built successfully!");
        System.out.println("=========================================");
        System.out.println("Schemas: " + schemaNames.size());
        System.out.println("Tables:  " + totalTables);
        System.out.println("File:    " + catalogPath);

        // Get file size
        if (catalogFile.exists()) {
          long bytes = catalogFile.length();
          String size;
          if (bytes < 1024) {
            size = bytes + " bytes";
          } else if (bytes < 1024 * 1024) {
            size = String.format("%.1f KB", bytes / 1024.0);
          } else {
            size = String.format("%.1f MB", bytes / (1024.0 * 1024.0));
          }
          System.out.println("Size:    " + size);
        }

        System.out.println();
        System.out.println("Test the catalog:");
        System.out.println("  duckdb " + catalogPath);
        System.out.println();
        System.out.println("Or use with Calcite:");
        System.out.println("  export DUCKDB_CATALOG_PATH=" + catalogPath);
        System.out.println("  sqlline -u '" + jdbcUrl + "'");
        System.out.println("=========================================");

      }

    } catch (Exception e) {
      System.err.println();
      System.err.println("=========================================");
      System.err.println("ERROR: Failed to build catalog");
      System.err.println("=========================================");
      e.printStackTrace();
      System.err.println("=========================================");
      System.exit(1);
    }
  }
}
