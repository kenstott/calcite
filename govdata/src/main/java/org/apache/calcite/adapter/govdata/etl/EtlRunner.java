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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.govdata.GovDataSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Standalone ETL runner for downloading historical government data.
 *
 * <p>Usage:
 * <pre>
 * java -Xmx4g -cp "build/libs/*" org.apache.calcite.adapter.govdata.etl.EtlRunner \
 *   --model src/main/resources/models/historical/sec/sec-10k-2026-all.json
 * </pre>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>0 - SUCCESS: All schemas processed successfully</li>
 *   <li>1 - PARTIAL: Some schemas failed, some succeeded</li>
 *   <li>2 - FAILED: Critical error, no schemas processed</li>
 * </ul>
 */
public class EtlRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(EtlRunner.class);

  /** Exit code: All schemas processed successfully. */
  public static final int EXIT_SUCCESS = 0;

  /** Exit code: Some schemas failed, some succeeded. */
  public static final int EXIT_PARTIAL = 1;

  /** Exit code: Critical error, no schemas processed. */
  public static final int EXIT_FAILED = 2;

  private final EtlRunConfig config;
  private final PrintStream out;

  public EtlRunner(EtlRunConfig config) {
    this(config, System.out);
  }

  public EtlRunner(EtlRunConfig config, PrintStream out) {
    this.config = config;
    this.out = out;
  }

  /**
   * Main entry point.
   */
  public static void main(String[] args) {
    int exitCode = EXIT_FAILED;

    try {
      EtlRunConfig config = EtlRunConfig.fromArgs(args);
      EtlRunner runner = new EtlRunner(config);
      exitCode = runner.run();
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      System.err.println();
      System.err.println("Use --help for usage information");
      exitCode = EXIT_FAILED;
    } catch (Exception e) {
      System.err.println("Fatal error: " + e.getMessage());
      e.printStackTrace(System.err);
      exitCode = EXIT_FAILED;
    }

    System.exit(exitCode);
  }

  /**
   * Run the ETL process.
   *
   * @return exit code
   */
  public int run() {
    log("ETL Runner starting");
    log("Model: " + config.getModelFile().getAbsolutePath());

    List<EtlRunConfig.SchemaConfig> schemas = config.getSchemas();
    log("Schemas to process: " + schemas.size());

    // Initialize manifest
    File manifestFile = config.getManifestFile();
    if (manifestFile == null) {
      // Default to runs/manifest.json relative to working directory
      manifestFile = new File("runs/manifest.json");
    }

    EtlRunManifest manifest = null;
    EtlRunManifest.RunEntry runEntry = null;

    try {
      manifest = new EtlRunManifest(manifestFile);
      runEntry = manifest.startRun(config.getModelFile().getName());
      log("Run ID: " + runEntry.runId);
    } catch (Exception e) {
      logWarn("Failed to initialize manifest: " + e.getMessage());
      // Continue without manifest
    }

    if (config.isDryRun()) {
      log("Dry run mode - validating configuration only");
      return validateDryRun(schemas);
    }

    // Process schemas sequentially
    int processed = 0;
    int succeeded = 0;
    int failed = 0;
    List<String> failedSchemas = new ArrayList<>();
    Map<String, Object> schemaDetails = new HashMap<>();

    for (EtlRunConfig.SchemaConfig schema : schemas) {
      processed++;
      log("");
      log("Processing schema " + processed + "/" + schemas.size() + ": " + schema.getName());

      long startTime = System.currentTimeMillis();
      boolean success = processSchema(schema);
      long elapsed = System.currentTimeMillis() - startTime;

      Map<String, Object> detail = new HashMap<>();
      detail.put("elapsedMs", elapsed);
      detail.put("success", success);

      if (success) {
        succeeded++;
        log("Schema " + schema.getName() + " completed successfully");
      } else {
        failed++;
        failedSchemas.add(schema.getName());
        logWarn("Schema " + schema.getName() + " failed");
      }

      schemaDetails.put(schema.getName(), detail);
    }

    // Determine final status
    EtlRunManifest.RunStatus status;
    int exitCode;

    if (failed == 0) {
      status = EtlRunManifest.RunStatus.SUCCESS;
      exitCode = EXIT_SUCCESS;
      log("");
      log("All schemas processed successfully");
    } else if (succeeded > 0) {
      status = EtlRunManifest.RunStatus.PARTIAL;
      exitCode = EXIT_PARTIAL;
      log("");
      logWarn("Partial success: " + succeeded + " succeeded, " + failed + " failed");
      logWarn("Failed schemas: " + String.join(", ", failedSchemas));
    } else {
      status = EtlRunManifest.RunStatus.FAILED;
      exitCode = EXIT_FAILED;
      log("");
      logWarn("All schemas failed");
    }

    // Update and save manifest
    if (manifest != null && runEntry != null) {
      try {
        Map<String, Object> results =
            EtlRunManifest.RunEntry.createResults(processed, succeeded, failed);
        results.put("details", schemaDetails);
        manifest.completeRun(runEntry, status, results);
        manifest.save();
        log("Manifest saved: " + manifestFile.getAbsolutePath());
      } catch (Exception e) {
        logWarn("Failed to save manifest: " + e.getMessage());
      }
    }

    return exitCode;
  }

  /**
   * Process a single schema by creating a JDBC connection (triggers ETL).
   */
  private boolean processSchema(EtlRunConfig.SchemaConfig schema) {
    String factory = schema.getFactory();

    // Verify factory is GovDataSchemaFactory
    if (factory == null) {
      logWarn("Schema " + schema.getName() + " has no factory specified");
      return false;
    }

    if (!factory.contains("GovDataSchemaFactory")) {
      logWarn("Schema " + schema.getName() + " uses non-govdata factory: " + factory);
      logWarn("ETL runner only supports GovDataSchemaFactory");
      return false;
    }

    try {
      // Load the GovData driver
      Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");

      // Create connection using the model file
      // This triggers the full ETL process via GovDataSchemaFactory
      String modelPath = config.getModelFile().getAbsolutePath();

      verbose("Creating Calcite connection with model: " + modelPath);

      // Use standard Calcite connection with our model
      String url = "jdbc:calcite:model=" + modelPath;

      try (Connection conn = DriverManager.getConnection(url)) {
        // Connection creation triggers schema creation, which triggers ETL
        // Just opening the connection is enough to run the ETL
        verbose("Connection established, ETL triggered for schema: " + schema.getName());

        // Optionally verify schema exists
        if (config.isVerbose()) {
          String catalog = conn.getCatalog();
          verbose("Connected to catalog: " + (catalog != null ? catalog : "(default)"));
        }

        return true;
      }

    } catch (ClassNotFoundException e) {
      logWarn("GovData driver not found. Ensure govdata JAR is in classpath.");
      return false;
    } catch (Exception e) {
      logWarn("Failed to process schema " + schema.getName() + ": " + e.getMessage());
      if (config.isVerbose()) {
        e.printStackTrace(System.err);
      }
      return false;
    }
  }

  /**
   * Validate configuration in dry-run mode.
   */
  private int validateDryRun(List<EtlRunConfig.SchemaConfig> schemas) {
    boolean valid = true;

    for (EtlRunConfig.SchemaConfig schema : schemas) {
      log("Validating schema: " + schema.getName());

      String factory = schema.getFactory();
      if (factory == null) {
        logWarn("  Missing factory");
        valid = false;
        continue;
      }

      if (!factory.contains("GovDataSchemaFactory")) {
        logWarn("  Unsupported factory: " + factory);
        valid = false;
        continue;
      }

      String dataSource = schema.getDataSource();
      if (dataSource == null) {
        logWarn("  Missing dataSource in operand");
        valid = false;
        continue;
      }

      // Validate dataSource
      if (!isValidDataSource(dataSource)) {
        logWarn("  Unknown dataSource: " + dataSource);
        valid = false;
        continue;
      }

      log("  Valid: dataSource=" + dataSource);
    }

    if (valid) {
      log("");
      log("Dry run validation passed");
      return EXIT_SUCCESS;
    } else {
      log("");
      logWarn("Dry run validation failed");
      return EXIT_FAILED;
    }
  }

  private boolean isValidDataSource(String dataSource) {
    switch (dataSource.toLowerCase()) {
      case "sec":
      case "edgar":
      case "geo":
      case "geographic":
      case "econ":
      case "economic":
      case "economy":
      case "econ_reference":
      case "econ_ref":
      case "census":
        return true;
      default:
        return false;
    }
  }

  private void log(String message) {
    if (!config.isCompact()) {
      out.println(message);
    }
    LOGGER.info(message);
  }

  private void logWarn(String message) {
    out.println("WARN: " + message);
    LOGGER.warn(message);
  }

  private void verbose(String message) {
    if (config.isVerbose()) {
      out.println("  " + message);
    }
    LOGGER.debug(message);
  }
}
