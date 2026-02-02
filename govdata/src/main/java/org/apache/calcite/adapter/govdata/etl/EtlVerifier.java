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
package org.apache.calcite.adapter.govdata.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Standalone verification tool for ETL results.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>Each table in the schema can be queried</li>
 *   <li>No columns are 100% null (data quality check)</li>
 *   <li>Tables have at least one row</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * java -cp "build/libs/*" org.apache.calcite.adapter.govdata.etl.EtlVerifier \
 *   --model path/to/model.json [--schema SCHEMA_NAME] [--verbose]
 * </pre>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>0 - All tables verified successfully</li>
 *   <li>1 - Some tables have issues (null columns, empty tables)</li>
 *   <li>2 - Critical error (connection failed, etc.)</li>
 * </ul>
 */
public class EtlVerifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(EtlVerifier.class);

  public static final int EXIT_SUCCESS = 0;
  public static final int EXIT_ISSUES = 1;
  public static final int EXIT_FAILED = 2;

  private final File modelFile;
  private final String schemaName;
  private final boolean verbose;
  private final PrintStream out;

  public EtlVerifier(File modelFile, String schemaName, boolean verbose) {
    this(modelFile, schemaName, verbose, System.out);
  }

  public EtlVerifier(File modelFile, String schemaName, boolean verbose, PrintStream out) {
    this.modelFile = modelFile;
    this.schemaName = schemaName;
    this.verbose = verbose;
    this.out = out;
  }

  /**
   * Main entry point.
   */
  public static void main(String[] args) {
    File modelFile = null;
    String schemaName = null;
    boolean verbose = false;

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      switch (arg) {
        case "--model":
          if (i + 1 >= args.length) {
            System.err.println("Error: --model requires a file path");
            System.exit(EXIT_FAILED);
          }
          modelFile = new File(args[++i]);
          break;
        case "--schema":
          if (i + 1 >= args.length) {
            System.err.println("Error: --schema requires a schema name");
            System.exit(EXIT_FAILED);
          }
          schemaName = args[++i];
          break;
        case "--verbose":
          verbose = true;
          break;
        case "--help":
        case "-h":
          printUsage();
          System.exit(0);
          break;
        default:
          if (arg.startsWith("-")) {
            System.err.println("Error: Unknown option: " + arg);
            System.exit(EXIT_FAILED);
          }
          // Positional argument - treat as model file
          if (modelFile == null) {
            modelFile = new File(arg);
          }
      }
    }

    if (modelFile == null) {
      System.err.println("Error: Model file is required");
      printUsage();
      System.exit(EXIT_FAILED);
    }

    if (!modelFile.exists()) {
      System.err.println("Error: Model file not found: " + modelFile);
      System.exit(EXIT_FAILED);
    }

    try {
      EtlVerifier verifier = new EtlVerifier(modelFile, schemaName, verbose);
      int exitCode = verifier.verify();
      System.exit(exitCode);
    } catch (Exception e) {
      System.err.println("Fatal error: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(EXIT_FAILED);
    }
  }

  private static void printUsage() {
    System.out.println("Usage: etl-verifier [OPTIONS] --model <file>");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --model <file>    Model JSON file (required)");
    System.out.println("  --schema <name>   Schema to verify (default: all schemas)");
    System.out.println("  --verbose         Show detailed column-level results");
    System.out.println("  --help, -h        Show this help message");
    System.out.println();
    System.out.println("Exit codes:");
    System.out.println("  0  SUCCESS - All tables verified, no issues");
    System.out.println("  1  ISSUES  - Some tables have null columns or are empty");
    System.out.println("  2  FAILED  - Critical error");
  }

  /**
   * Run verification.
   *
   * @return exit code
   */
  public int verify() {
    log("ETL Verifier starting");
    log("Model: " + modelFile.getAbsolutePath());

    try {
      Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    } catch (ClassNotFoundException e) {
      logError("GovData driver not found. Ensure govdata JAR is in classpath.");
      return EXIT_FAILED;
    }

    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(url)) {
      log("Connected successfully");

      List<String> schemasToVerify = getSchemasToVerify(conn);
      log("Schemas to verify: " + schemasToVerify);

      VerificationResult overallResult = new VerificationResult();

      for (String schema : schemasToVerify) {
        log("");
        log("=== Verifying schema: " + schema + " ===");
        VerificationResult schemaResult = verifySchema(conn, schema);
        overallResult.merge(schemaResult);
      }

      // Print summary
      log("");
      log("=== VERIFICATION SUMMARY ===");
      log("Tables verified: " + overallResult.tablesVerified);
      log("Tables with data: " + overallResult.tablesWithData);
      log("Empty tables: " + overallResult.emptyTables.size());
      log("Tables with null columns: " + overallResult.tablesWithNullColumns.size());
      log("Query failures: " + overallResult.queryFailures.size());

      if (!overallResult.emptyTables.isEmpty()) {
        log("");
        logWarn("Empty tables: " + String.join(", ", overallResult.emptyTables));
      }

      if (!overallResult.tablesWithNullColumns.isEmpty()) {
        log("");
        logWarn("Tables with 100% null columns:");
        for (Map.Entry<String, List<String>> entry : overallResult.tablesWithNullColumns.entrySet()) {
          logWarn("  " + entry.getKey() + ": " + String.join(", ", entry.getValue()));
        }
      }

      if (!overallResult.queryFailures.isEmpty()) {
        log("");
        logError("Query failures:");
        for (Map.Entry<String, String> entry : overallResult.queryFailures.entrySet()) {
          logError("  " + entry.getKey() + ": " + entry.getValue());
        }
      }

      // Determine exit code
      if (!overallResult.queryFailures.isEmpty()) {
        return EXIT_FAILED;
      } else if (!overallResult.emptyTables.isEmpty() || !overallResult.tablesWithNullColumns.isEmpty()) {
        return EXIT_ISSUES;
      } else {
        log("");
        log("All tables verified successfully!");
        return EXIT_SUCCESS;
      }

    } catch (SQLException e) {
      logError("Connection failed: " + e.getMessage());
      if (verbose) {
        e.printStackTrace(System.err);
      }
      return EXIT_FAILED;
    }
  }

  private List<String> getSchemasToVerify(Connection conn) throws SQLException {
    List<String> schemas = new ArrayList<>();

    if (schemaName != null) {
      schemas.add(schemaName);
      return schemas;
    }

    DatabaseMetaData meta = conn.getMetaData();
    try (ResultSet rs = meta.getSchemas()) {
      while (rs.next()) {
        String schema = rs.getString("TABLE_SCHEM");
        // Skip internal Calcite schemas
        if (schema != null && !schema.equals("metadata") && !schema.startsWith("$")) {
          schemas.add(schema);
        }
      }
    }

    return schemas;
  }

  private VerificationResult verifySchema(Connection conn, String schema) {
    VerificationResult result = new VerificationResult();

    List<String> tables = getTablesInSchema(conn, schema);
    log("Found " + tables.size() + " tables");

    for (String table : tables) {
      result.tablesVerified++;
      String fullName = schema + "." + table;

      verbose("Checking: " + fullName);

      try {
        TableCheckResult checkResult = checkTable(conn, schema, table);

        if (checkResult.rowCount == 0) {
          result.emptyTables.add(fullName);
          logWarn("  " + fullName + ": EMPTY");
        } else {
          result.tablesWithData++;
          verbose("  " + fullName + ": " + checkResult.rowCount + " rows");

          if (!checkResult.nullColumns.isEmpty()) {
            result.tablesWithNullColumns.put(fullName, checkResult.nullColumns);
            logWarn("  " + fullName + ": 100% null columns: " + checkResult.nullColumns);
          }
        }

      } catch (SQLException e) {
        result.queryFailures.put(fullName, e.getMessage());
        logError("  " + fullName + ": QUERY FAILED - " + e.getMessage());
      }
    }

    return result;
  }

  private List<String> getTablesInSchema(Connection conn, String schema) {
    List<String> tables = new ArrayList<>();

    try {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet rs = meta.getTables(null, schema, "%", new String[]{"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if (tableName != null) {
            tables.add(tableName);
          }
        }
      }
    } catch (SQLException e) {
      logError("Failed to list tables in schema " + schema + ": " + e.getMessage());
    }

    return tables;
  }

  private TableCheckResult checkTable(Connection conn, String schema, String table)
      throws SQLException {
    TableCheckResult result = new TableCheckResult();
    String fullName = "\"" + schema + "\".\"" + table + "\"";

    // Get row count
    String countSql = "SELECT COUNT(*) FROM " + fullName;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(countSql)) {
      if (rs.next()) {
        result.rowCount = rs.getLong(1);
      }
    }

    if (result.rowCount == 0) {
      return result;
    }

    // Check for null columns - sample first 1000 rows for efficiency
    String sampleSql = "SELECT * FROM " + fullName + " LIMIT 1000";
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sampleSql)) {

      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();

      // Track non-null counts per column
      long[] nonNullCounts = new long[columnCount];
      String[] columnNames = new String[columnCount];
      long rowsChecked = 0;

      for (int i = 0; i < columnCount; i++) {
        columnNames[i] = meta.getColumnName(i + 1);
      }

      while (rs.next()) {
        rowsChecked++;
        for (int i = 0; i < columnCount; i++) {
          Object value = rs.getObject(i + 1);
          if (value != null) {
            nonNullCounts[i]++;
          }
        }
      }

      // Find columns that are 100% null
      for (int i = 0; i < columnCount; i++) {
        if (nonNullCounts[i] == 0 && rowsChecked > 0) {
          result.nullColumns.add(columnNames[i]);
        }
      }
    }

    return result;
  }

  private void log(String message) {
    out.println(message);
    LOGGER.info(message);
  }

  private void logWarn(String message) {
    out.println("WARN: " + message);
    LOGGER.warn(message);
  }

  private void logError(String message) {
    out.println("ERROR: " + message);
    LOGGER.error(message);
  }

  private void verbose(String message) {
    if (verbose) {
      out.println("  " + message);
    }
    LOGGER.debug(message);
  }

  /**
   * Result of checking a single table.
   */
  private static class TableCheckResult {
    long rowCount = 0;
    List<String> nullColumns = new ArrayList<>();
  }

  /**
   * Aggregated verification results.
   */
  private static class VerificationResult {
    int tablesVerified = 0;
    int tablesWithData = 0;
    List<String> emptyTables = new ArrayList<>();
    Map<String, List<String>> tablesWithNullColumns = new HashMap<>();
    Map<String, String> queryFailures = new HashMap<>();

    void merge(VerificationResult other) {
      tablesVerified += other.tablesVerified;
      tablesWithData += other.tablesWithData;
      emptyTables.addAll(other.emptyTables);
      tablesWithNullColumns.putAll(other.tablesWithNullColumns);
      queryFailures.putAll(other.queryFailures);
    }
  }
}
