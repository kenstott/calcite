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
package org.apache.calcite.adapter.govdata;
// storage-provider-guard:allow-scheme - storage-dispatch layer: inspecting a URI scheme here is the legitimate job (provider dispatch / S3 path handling / endpoint SSL config), not a consumer branching local-vs-remote.

import org.apache.calcite.jdbc.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC driver for Government Data adapter.
 *
 * <p>Allows connections using "jdbc:govdata:" URL format with automatic defaults:
 * <ul>
 *   <li>lex = ORACLE (unless overridden)</li>
 *   <li>unquotedCasing = TO_LOWER (unless overridden)</li>
 *   <li>Automatic model configuration based on data source</li>
 *   <li>Support for connection parameters</li>
 * </ul>
 *
 * <p>Usage examples:
 * <pre>
 * // Single schema with downloading enabled
 * jdbc:govdata:source=sec&ciks=AAPL                    // Apple ticker → CIK 0000320193
 * jdbc:govdata:source=sec&ciks=AAPL,MSFT,GOOGL        // Multiple tickers
 * jdbc:govdata:source=sec&ciks=MAGNIFICENT7            // Predefined group → 7 CIKs
 * jdbc:govdata:source=sec&ciks=0000320193              // Raw CIK also supported
 *
 * // Multiple schemas, read-only (autoDownload=false) — for schema introspection
 * jdbc:govdata:source=sec,geo,econ&dataDirectory=/Volumes/T9/gov-data
 * jdbc:govdata:source=sec,geo,econ,health,fec&dataDirectory=/Volumes/T9/gov-data
 *
 * // Mixed identifiers and additional parameters
 * jdbc:govdata:source=sec&ciks=FAANG&startYear=2020&endYear=2023
 * jdbc:govdata:source=sec&ciks=AAPL,0001018724&dataDirectory=/Volumes/T9/gov-data
 * </pre>
 *
 * <p>Backward compatibility: Also handles legacy "jdbc:sec:" URLs by
 * automatically redirecting to "jdbc:govdata:source=sec".
 */
public class GovDataDriver extends Driver {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataDriver.class);

  static {
    new GovDataDriver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return "jdbc:govdata:";
  }

  /**
   * Returns the operating directory base for this driver.
   *
   * <p>Default: {@code GOVDATA_DATA_DIR} env var, or {@code ~/.govdata}.
   * Product drivers (e.g. AskAmericaDriver) override this to check their own env var.
   */
  protected String resolveOperatingDirBase(String home) {
    String envDir = System.getenv("GOVDATA_DATA_DIR");
    if (envDir != null && !envDir.isEmpty()) {
      return envDir;
    }
    return home + "/.govdata";
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    LOGGER.info("Connecting to government data source: {}", url);

    // Pin operating dir and httpfs cache per-process. Resolution order:
    //   1. System property already set (product driver set it before calling us)
    //   2. resolveOperatingDirBase() — checks product env var or falls back to ~/.govdata
    String home = System.getProperty("user.home");
    if (home != null && !home.isEmpty()) {
      if (System.getProperty("govdata.operating.dir.base") == null) {
        System.setProperty("govdata.operating.dir.base", resolveOperatingDirBase(home));
      }
      if (System.getProperty("duckdb.cache_httpfs.directory") == null) {
        System.setProperty("duckdb.cache_httpfs.directory",
            resolveOperatingDirBase(home) + "/.duckdb_httpfs_cache");
      }
    }

    // Apply GovData-specific defaults
    Properties govDataInfo = new Properties();
    if (info != null) {
      govDataInfo.putAll(info);
    }

    // ORACLE lex with TO_LOWER casing matches ingested lower-case table names.
    // caseSensitive=false lets TO_LOWER-folded SQL identifiers match upper-case metadata tables.
    if (!govDataInfo.containsKey("lex")) {
      govDataInfo.setProperty("lex", "ORACLE");
      LOGGER.debug("Using default lex=ORACLE");
    }
    if (!govDataInfo.containsKey("unquotedCasing")) {
      govDataInfo.setProperty("unquotedCasing", "TO_LOWER");
      LOGGER.debug("Using default unquotedCasing=TO_LOWER");
    }
    if (!govDataInfo.containsKey("caseSensitive")) {
      govDataInfo.setProperty("caseSensitive", "false");
      LOGGER.debug("Using default caseSensitive=false");
    }

    try {
      // Extract connection parameters from URL
      String paramString = url.substring(getConnectStringPrefix().length());

      // Check if dataSource is specified, default to SEC for backward compatibility
      String sourceParam = extractParameter(paramString, "source");
      if (sourceParam == null) {
        sourceParam = "sec";
        LOGGER.info("No data source specified, defaulting to 'sec'");
      }

      // Create model file — supports comma-delimited list of sources
      String modelPath = createModelFile(paramString, sourceParam);

      // Set model path in connection properties
      govDataInfo.setProperty("model", modelPath);

      // Delegate to a fresh Calcite Driver instance.
      // Cannot use super.connect() here: UnregisteredDriver.connect() calls
      // this.acceptsURL() which checks for "jdbc:govdata:", so it would return
      // null when given "jdbc:calcite:". A separate instance has the correct prefix.
      String calciteUrl = "jdbc:calcite:";
      return new Driver().connect(calciteUrl, govDataInfo);

    } catch (Exception e) {
      throw new SQLException("Failed to create government data connection: " + e.getMessage(), e);
    }
  }

  /**
   * Extract parameter value from URL parameter string.
   */
  private String extractParameter(String paramString, String paramName) {
    if (paramString == null || paramString.isEmpty()) {
      return null;
    }

    String[] params = paramString.split("&");
    for (String param : params) {
      String[] keyValue = param.split("=", 2);
      if (keyValue.length == 2 && keyValue[0].equals(paramName)) {
        return keyValue[1];
      }
    }
    return null;
  }

  /**
   * Create a Calcite model file for the given source parameter.
   *
   * <p>When {@code sourceParam} contains a single source that requires downloading (e.g. "sec"),
   * the model enables autoDownload and passes through ciks/startYear/endYear.
   *
   * <p>When {@code sourceParam} is a comma-delimited list of sources, the model includes one
   * schema entry per source with {@code autoDownload: false}. This is the read-only path
   * used for schema introspection from external tools (e.g. Python via JayDeBeAPI).
   */
  private String createModelFile(String paramString, String sourceParam) throws IOException {
    String[] sources = sourceParam.split(",");
    for (int i = 0; i < sources.length; i++) {
      sources[i] = sources[i].trim();
    }

    if (sources.length == 1) {
      return createSingleSourceModel(paramString, sources[0]);
    }
    return createMultiSourceModel(paramString, sources);
  }

  /**
   * Absolute path to the single DuckDB catalog shared by every schema in a connection. The file
   * adapter supports a shared database via the {@code database_filename} operand "for cross-schema
   * joins"; pointing all mounted schemas at one catalog (instead of a per-schema
   * {@code <schema>_db.duckdb}) lets cross-schema views resolve (e.g. an edu view that joins
   * geo.counties), and keeps a single persistent, checkpointed catalog instead of N of them.
   * An absolute path under the operating-dir base is used so all schemas resolve to the same file
   * regardless of the process working directory.
   */
  private String sharedCatalogPath() {
    String base = System.getProperty("govdata.operating.dir.base");
    if (base == null || base.isEmpty()) {
      String home = System.getProperty("user.home");
      base = resolveOperatingDirBase((home != null && !home.isEmpty())
          ? home : System.getProperty("java.io.tmpdir"));
    }
    // JSON-escape backslashes so Windows-style paths embed safely in the generated model.
    return new File(base, ".duckdb/govdata.duckdb").getAbsolutePath().replace("\\", "\\\\");
  }

  private String createSingleSourceModel(String paramString, String dataSource)
      throws IOException {
    String ciks = extractParameter(paramString, "ciks");
    String startYear = extractParameter(paramString, "startYear");
    String endYear = extractParameter(paramString, "endYear");
    String dataDirectory = resolveDataDirectory(extractParameter(paramString, "dataDirectory"));

    String schemaName = dataSource.toLowerCase();
    boolean isS3 = dataDirectory != null && dataDirectory.startsWith("s3://");
    String directoryJson = dataDirectory != null
        ? ",\n      \"directory\": \"" + dataDirectory + "\""
        : "";
    String s3ConfigJson = buildS3ConfigJson(dataDirectory);
    String engineJson = isS3 ? "      \"executionEngine\": \"duckdb\",\n" : "";
    String ciksJson = ciks != null ? "      \"ciks\": \"" + ciks + "\",\n" : "";
    String startYearJson = startYear != null ? "      \"startYear\": " + startYear + ",\n" : "";
    String endYearJson = endYear != null ? "      \"endYear\": " + endYear + ",\n" : "";

    String modelJson = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + schemaName + "\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"" + schemaName + "\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"dataSource\": \"" + dataSource.toLowerCase() + "\",\n"
        + "      \"database_filename\": \"" + sharedCatalogPath() + "\",\n"
        + ciksJson
        + startYearJson
        + endYearJson
        + engineJson
        + "      \"autoDownload\": false,\n"
        + "      \"testMode\": false,\n"
        + "      \"ephemeralCache\": false" + directoryJson + s3ConfigJson + "\n"
        + "    }\n"
        + "  }]\n"
        + "}";

    return writeTempModel("govdata-model", modelJson);
  }

  private String createMultiSourceModel(String paramString, String[] sources)
      throws IOException {
    String dataDirectory = resolveDataDirectory(extractParameter(paramString, "dataDirectory"));
    String s3ConfigJson = buildS3ConfigJson(dataDirectory);

    boolean isS3 = dataDirectory != null && dataDirectory.startsWith("s3://");
    String engineJson = isS3 ? "      \"executionEngine\": \"duckdb\",\n" : "";
    // One shared DuckDB catalog for every schema in this connection so cross-schema views resolve.
    String dbFilenameJson = "      \"database_filename\": \"" + sharedCatalogPath() + "\",\n";

    List<String> schemaEntries = new ArrayList<String>();
    for (String dataSource : sources) {
      String schemaName = dataSource.toLowerCase();
      String directoryJson = dataDirectory != null
          ? ",\n      \"directory\": \"" + dataDirectory + "\""
          : "";
      schemaEntries.add(
          "  {\n"
          + "    \"name\": \"" + schemaName + "\",\n"
          + "    \"type\": \"custom\",\n"
          + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
          + "    \"operand\": {\n"
          + "      \"dataSource\": \"" + dataSource.toLowerCase() + "\",\n"
          + dbFilenameJson
          + engineJson
          + "      \"autoDownload\": false,\n"
          + "      \"testMode\": false,\n"
          + "      \"ephemeralCache\": false" + directoryJson + s3ConfigJson + "\n"
          + "    }\n"
          + "  }");
    }

    StringBuilder schemas = new StringBuilder();
    for (int i = 0; i < schemaEntries.size(); i++) {
      if (i > 0) {
        schemas.append(",\n");
      }
      schemas.append(schemaEntries.get(i));
    }

    String modelJson = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + sources[0].toLowerCase() + "\",\n"
        + "  \"schemas\": [\n"
        + schemas.toString() + "\n"
        + "  ]\n"
        + "}";

    LOGGER.info("Creating multi-source model with {} schemas (autoDownload=false): {}",
        sources.length, java.util.Arrays.toString(sources));
    return writeTempModel("govdata-multi-model", modelJson);
  }

  /**
   * Builds an inline s3Config/storageConfig JSON fragment when the resolved directory is an
   * S3 URI.  Uses ${VAR} references so GovDataSchemaFactory.resolveS3Config() resolves them
   * at runtime — matching the model-r2.json pattern.
   * Returns "" when the directory is not S3.
   */
  private String buildS3ConfigJson(String dataDirectory) {
    if (dataDirectory == null || !dataDirectory.startsWith("s3://")) {
      return "";
    }

    String accessKeyId;
    String secretAccessKey;
    String endpoint;
    String region;

    // Honor an explicit object-store endpoint from the environment (AWS_* in .env.prod — e.g. a
    // local MinIO) over the bundled R2 defaults. This makes the configured store work with no
    // credentials file and regardless of which home directory the JVM resolves. AWS_* are
    // launch/infra flags, exempt from the model-operand rule.
    String envEndpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    String envAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String envSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (envEndpoint != null && !envEndpoint.isEmpty()
        && envAccessKey != null && !envAccessKey.isEmpty()
        && envSecretKey != null && !envSecretKey.isEmpty()) {
      accessKeyId = envAccessKey;
      secretAccessKey = envSecretKey;
      endpoint = envEndpoint;
      String envRegion = System.getenv("AWS_REGION");
      region = (envRegion != null && !envRegion.isEmpty()) ? envRegion : "auto";
      LOGGER.info("Using object-store endpoint from environment (AWS_ENDPOINT_OVERRIDE): {}", endpoint);
    } else {
      Map<String, String> creds = R2CredentialProvider.resolve();
      accessKeyId = creds.get("accessKeyId");
      secretAccessKey = creds.get("secretAccessKey");
      endpoint = creds.get("endpoint");
      region = creds.get("region");
    }

    String credBlock = "        \"accessKeyId\": \"" + accessKeyId + "\","
        + "\n        \"secretAccessKey\": \"" + secretAccessKey + "\","
        + "\n        \"endpoint\": \"" + endpoint + "\","
        + "\n        \"region\": \"" + region + "\"";

    return ",\n      \"s3Config\": {\n" + credBlock + "\n      }"
        + ",\n      \"storageConfig\": {\n" + credBlock + "\n      }";
  }

  private String resolveDataDirectory(String explicit) {
    if (explicit != null && !explicit.isEmpty()) {
      return explicit;
    }
    String envDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (envDir != null && !envDir.isEmpty()) {
      return envDir;
    }
    return "s3://govdata-parquet-v1";
  }

  private String writeTempModel(String prefix, String modelJson) throws IOException {
    // Write the generated model under the stable operating-dir base (e.g. ~/.govdata), NOT
    // java.io.tmpdir. Calcite derives the schema baseDirectory from the model file's location,
    // and that becomes the local .aperio operating/cache root. Writing to /tmp lands caches in
    // /tmp/.aperio, a hidden location that persists stale state across runs; a stable home/base
    // keeps the operating dir visible and per-user.
    String base = System.getProperty("govdata.operating.dir.base");
    File modelDir = null;
    if (base != null && !base.isEmpty()) {
      modelDir = new File(base);
      if (!modelDir.exists() && !modelDir.mkdirs() && !modelDir.exists()) {
        modelDir = null;  // could not create the base dir; fall back to default temp location
      }
    }
    File tempFile = modelDir != null
        ? File.createTempFile(prefix, ".json", modelDir)
        : File.createTempFile(prefix, ".json");
    tempFile.deleteOnExit();
    try (java.io.OutputStreamWriter writer = new java.io.OutputStreamWriter(
        new java.io.FileOutputStream(tempFile), java.nio.charset.StandardCharsets.UTF_8)) {
      writer.write(modelJson);
    }
    LOGGER.debug("Created temporary model file: {}", tempFile.getAbsolutePath());
    return tempFile.getAbsolutePath();
  }
}
