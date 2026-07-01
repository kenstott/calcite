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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Manages Iceberg catalog instances and table loading.
 *
 * <p>Extended to support write operations (createTable, dropTable) for
 * partition alternates implementation.
 */
public class IcebergCatalogManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalogManager.class);
  private static final Map<String, Catalog> CATALOG_CACHE = new HashMap<>();
  private static final String ALTERNATE_PREFIX = "_mv_";
  private static final String CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
  private static final int RANDOM_NAME_LENGTH = 32;
  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * Loads a table from the specified catalog configuration.
   *
   * @param config The configuration containing catalog details
   * @param tablePath The table path or identifier
   * @return The loaded Iceberg table
   */
  public static Table loadTable(Map<String, Object> config, String tablePath) {
    // Check if this is a direct file path (starts with / or contains file://)
    if (tablePath.startsWith("/") || tablePath.startsWith("file://") || tablePath.contains("warehouse")) {
      // Direct path loading - load table directly from filesystem
      try {
        Configuration hadoopConf = new Configuration();
        return new HadoopTables(hadoopConf).load(tablePath);
      } catch (Exception e) {
        throw new RuntimeException("Failed to load table from path: " + tablePath, e);
      }
    }

    String catalogType = (String) config.get("catalog");
    if (catalogType == null) {
      catalogType = "hadoop";
    }

    Catalog catalog = getCatalog(catalogType, config);

    // Parse table identifier
    TableIdentifier tableId = parseTableIdentifier(tablePath, config);

    return catalog.loadTable(tableId);
  }

  /**
   * Gets or creates a catalog instance for use by storage provider.
   *
   * @param catalogType The type of catalog (hadoop, hive, rest)
   * @param config The configuration
   * @return The catalog instance
   */
  public static synchronized Catalog getCatalogForProvider(String catalogType, Map<String, Object> config) {
    return getCatalog(catalogType, config);
  }

  /**
   * Gets or creates a catalog instance.
   *
   * @param catalogType The type of catalog (hadoop, hive, rest)
   * @param config The configuration
   * @return The catalog instance
   */
  private static synchronized Catalog getCatalog(String catalogType, Map<String, Object> config) {
    String cacheKey = catalogType + ":" + config.hashCode();

    if (CATALOG_CACHE.containsKey(cacheKey)) {
      return CATALOG_CACHE.get(cacheKey);
    }

    Catalog catalog;
    switch (catalogType.toLowerCase()) {
      case "hadoop":
        catalog = createHadoopCatalog(config);
        break;
      case "hive":
        throw new UnsupportedOperationException("Hive catalog support not yet implemented");
        // catalog = createHiveCatalog(config);
        // break;
      case "rest":
        catalog = createRestCatalog(config);
        break;
      default:
        throw new IllegalArgumentException("Unknown catalog type: " + catalogType);
    }

    CATALOG_CACHE.put(cacheKey, catalog);
    return catalog;
  }

  /**
   * Creates a Hadoop catalog.
   *
   * @param config The configuration
   * @return The Hadoop catalog
   */
  private static HadoopCatalog createHadoopCatalog(Map<String, Object> config) {
    String warehouse = (String) config.get("warehouse");
    if (warehouse == null) {
      warehouse = (String) config.get("warehousePath");
    }
    if (warehouse == null) {
      throw new IllegalArgumentException("Hadoop catalog requires 'warehouse' or 'warehousePath' configuration");
    }

    Configuration hadoopConf = new Configuration();

    // Apply Hadoop configuration if provided
    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    return new HadoopCatalog(hadoopConf, warehouse);
  }

  /**
   * Creates a Hive catalog.
   *
   * @param config The configuration
   * @return The Hive catalog
   */
  /*
  private static HiveCatalog createHiveCatalog(Map<String, Object> config) {
    HiveCatalog catalog = new HiveCatalog();

    // Set catalog name
    String catalogName = (String) config.getOrDefault("catalogName", "hive");
    Map<String, String> properties = new HashMap<>();
    properties.put("catalog-name", catalogName);

    // Set URI if provided
    String uri = (String) config.get("uri");
    if (uri != null) {
      properties.put("uri", uri);
    }

    // Set warehouse if provided
    String warehouse = (String) config.get("warehouse");
    if (warehouse != null) {
      properties.put("warehouse", warehouse);
    }

    // Apply additional Hive configuration
    @SuppressWarnings("unchecked")
    Map<String, String> hiveConfig = (Map<String, String>) config.get("hiveConfig");
    if (hiveConfig != null) {
      properties.putAll(hiveConfig);
    }

    Configuration hadoopConf = new Configuration();
    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    catalog.setConf(hadoopConf);
    catalog.initialize(catalogName, properties);

    return catalog;
  }

  /**
   * Creates a REST catalog.
   *
   * @param config The configuration
   * @return The REST catalog
   */
  private static RESTCatalog createRestCatalog(Map<String, Object> config) {
    RESTCatalog catalog = new RESTCatalog();

    Map<String, String> properties = new HashMap<>();

    // Set URI (required)
    String uri = (String) config.get("uri");
    if (uri == null) {
      throw new IllegalArgumentException("REST catalog requires 'uri' configuration");
    }
    properties.put("uri", uri);

    // Set warehouse if provided
    String warehouse = (String) config.get("warehouse");
    if (warehouse != null) {
      properties.put("warehouse", warehouse);
    }

    // Set authentication if provided
    String token = (String) config.get("token");
    if (token != null) {
      properties.put("token", token);
    }

    String credential = (String) config.get("credential");
    if (credential != null) {
      properties.put("credential", credential);
    }

    // Apply additional REST configuration
    @SuppressWarnings("unchecked")
    Map<String, String> restConfig = (Map<String, String>) config.get("restConfig");
    if (restConfig != null) {
      properties.putAll(restConfig);
    }

    Configuration hadoopConf = new Configuration();
    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    catalog.setConf(hadoopConf);
    catalog.initialize("rest", properties);

    return catalog;
  }

  /**
   * Parses a table identifier from a path string.
   *
   * @param tablePath The table path
   * @param config The configuration
   * @return The table identifier
   */
  private static TableIdentifier parseTableIdentifier(String tablePath, Map<String, Object> config) {
    // Check if namespace is provided in config
    String namespace = (String) config.get("namespace");

    if (tablePath.contains(".")) {
      // Path includes namespace (e.g., "namespace.table")
      String[] parts = tablePath.split("\\.", 2);
      return TableIdentifier.of(parts[0], parts[1]);
    } else if (namespace != null) {
      // Use namespace from config
      return TableIdentifier.of(namespace, tablePath);
    } else {
      // No namespace (single-level)
      return TableIdentifier.of(tablePath);
    }
  }

  /**
   * Clears the catalog cache.
   */
  public static synchronized void clearCache() {
    CATALOG_CACHE.clear();
  }

  /**
   * Creates a new Iceberg table with the given schema and partition spec.
   *
   * @param config The catalog configuration
   * @param tableId The table identifier (namespace.tableName or just tableName)
   * @param schema The Iceberg schema for the table
   * @param partitionSpec The partition specification
   * @return The created table
   */
  public static Table createTable(Map<String, Object> config, String tableId,
      Schema schema, PartitionSpec partitionSpec) {
    String catalogType = (String) config.get("catalog");
    if (catalogType == null) {
      catalogType = "hadoop";
    }

    Catalog catalog = getCatalog(catalogType, config);
    TableIdentifier identifier = parseTableIdentifier(tableId, config);

    if (catalog.tableExists(identifier)) {
      LOGGER.info("Table {} already exists, loading existing table", identifier);
      return catalog.loadTable(identifier);
    }

    LOGGER.info("Creating Iceberg table: {}", identifier);
    Table table = catalog.createTable(identifier, schema, partitionSpec);
    LOGGER.info("Created Iceberg table at location: {}", table.location());
    return table;
  }

  /**
   * Creates a new Iceberg table with schema inferred from column definitions.
   *
   * @param config The catalog configuration
   * @param tableId The table identifier
   * @param columns List of column definitions with name and type
   * @param partitionColumns List of partition column names in order
   * @return The created table
   */
  public static Table createTableFromColumns(Map<String, Object> config, String tableId,
      List<ColumnDef> columns, List<String> partitionColumns) {
    // Build Iceberg schema from column definitions
    List<Types.NestedField> fields = new ArrayList<Types.NestedField>();
    int fieldId = 1;
    for (ColumnDef col : columns) {
      Types.NestedField field;
      if (col.getDoc() != null && !col.getDoc().isEmpty()) {
        // Create field with documentation
        field =
            Types.NestedField.optional(fieldId++, col.getName(),
            mapToIcebergType(col.getType()),
            col.getDoc());
      } else {
        // Create field without documentation
        field =
            Types.NestedField.optional(fieldId++, col.getName(),
            mapToIcebergType(col.getType()));
      }
      fields.add(field);
    }
    Schema schema = new Schema(fields);

    // Build partition spec from partition column names
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    for (String partCol : partitionColumns) {
      specBuilder.identity(partCol);
    }
    PartitionSpec partitionSpec = specBuilder.build();

    return createTable(config, tableId, schema, partitionSpec);
  }

  /**
   * Maps a string type name to an Iceberg Type.
   * Supports basic types and array types (e.g., "array&lt;double&gt;").
   *
   * @param typeName The type name (e.g., "VARCHAR", "INTEGER", "array&lt;double&gt;")
   * @return The corresponding Iceberg type
   */
  private static org.apache.iceberg.types.Type mapToIcebergType(String typeName) {
    if (typeName == null) {
      return Types.StringType.get();
    }

    // Handle array types: array<element_type>
    String lowerType = typeName.toLowerCase();
    if (lowerType.startsWith("array<") && lowerType.endsWith(">")) {
      String elementType = typeName.substring(6, typeName.length() - 1).trim();
      org.apache.iceberg.types.Type elementIcebergType = mapToIcebergType(elementType);
      // Use element ID of 100 for list elements (standard convention)
      return Types.ListType.ofOptional(100, elementIcebergType);
    }

    String upperType = typeName.toUpperCase();
    switch (upperType) {
      case "INTEGER":
      case "INT":
        return Types.IntegerType.get();
      case "BIGINT":
      case "LONG":
        return Types.LongType.get();
      case "DOUBLE":
      case "FLOAT8":
        return Types.DoubleType.get();
      case "FLOAT":
      case "REAL":
        return Types.FloatType.get();
      case "BOOLEAN":
      case "BOOL":
        return Types.BooleanType.get();
      case "DATE":
        return Types.DateType.get();
      case "TIMESTAMP":
        return Types.TimestampType.withoutZone();
      case "TIMESTAMPTZ":
      case "TIMESTAMP WITH TIME ZONE":
        return Types.TimestampType.withZone();
      case "DECIMAL":
        return Types.DecimalType.of(38, 9);
      case "BINARY":
      case "BYTES":
        return Types.BinaryType.get();
      case "VARCHAR":
      case "STRING":
      case "TEXT":
      default:
        return Types.StringType.get();
    }
  }

  /**
   * Creates a new Iceberg table whose schema is INFERRED from a Parquet file rather than declared.
   *
   * <p>Iceberg is only metadata over Parquet: the file's footer already carries authoritative column
   * names and types, so a materialize with no declared {@code columns} can derive the table schema
   * directly from the staged data. The schema is read with a DuckDB
   * {@code DESCRIBE SELECT * FROM read_parquet(...)}, each DuckDB type normalized to the canonical
   * type string understood by {@link #mapToIcebergType(String)}, then delegated to
   * {@link #createTableFromColumns}. When {@code columns} ARE declared the caller should keep using
   * {@link #createTableFromColumns} — declared columns are an override layer, not replaced by this.
   *
   * @param config The catalog configuration
   * @param tableId The table identifier
   * @param parquetPath Absolute path (or read_parquet-compatible URI) of the file to introspect
   * @param partitionColumns Partition column names in order (may be empty)
   * @return The created table
   */
  public static Table createTableFromParquet(Map<String, Object> config, String tableId,
      String parquetPath, List<String> partitionColumns) {
    List<ColumnDef> columns = describeParquet(parquetPath);
    if (columns.isEmpty()) {
      throw new IllegalStateException(
          "Cannot infer Iceberg schema: DESCRIBE returned no columns for parquet: " + parquetPath);
    }
    LOGGER.info("Inferred {} columns from parquet {} for table {}",
        columns.size(), parquetPath, tableId);
    return createTableFromColumns(config, tableId, columns, partitionColumns);
  }

  /**
   * Reads the column names and types of a Parquet file via DuckDB {@code DESCRIBE} and maps each
   * DuckDB type to the canonical type string understood by {@link #mapToIcebergType(String)}.
   */
  private static List<ColumnDef> describeParquet(String parquetPath) {
    List<ColumnDef> columns = new ArrayList<ColumnDef>();
    String escaped = parquetPath.replace("'", "''");
    String sql = "DESCRIBE SELECT * FROM read_parquet('" + escaped + "')";
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        columns.add(
            new ColumnDef(rs.getString("column_name"),
            duckdbTypeToCanonical(rs.getString("column_type"))));
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to infer schema from parquet '" + parquetPath + "': " + e.getMessage(), e);
    }
    return columns;
  }

  /**
   * Maps a DuckDB {@code DESCRIBE} type name to the canonical type string understood by
   * {@link #mapToIcebergType(String)}. Fails loud on an unrecognized type rather than silently
   * defaulting to VARCHAR, so an unhandled Parquet type is surfaced and mapped deliberately.
   */
  private static String duckdbTypeToCanonical(String duckType) {
    if (duckType == null) {
      throw new IllegalStateException("DuckDB DESCRIBE returned a null column type");
    }
    String t = duckType.trim().toUpperCase(Locale.ROOT);
    // List types: "INTEGER[]" -> array<INTEGER> (mapToIcebergType recurses on the element)
    if (t.endsWith("[]")) {
      return "array<" + duckdbTypeToCanonical(t.substring(0, t.length() - 2)) + ">";
    }
    // DECIMAL(p,s) -> DECIMAL (mapToIcebergType applies fixed precision/scale)
    if (t.startsWith("DECIMAL")) {
      return "DECIMAL";
    }
    switch (t) {
      case "BOOLEAN":
        return "BOOLEAN";
      case "TINYINT":
      case "SMALLINT":
      case "INTEGER":
      case "UTINYINT":
      case "USMALLINT":
        return "INTEGER";
      case "BIGINT":
      case "UINTEGER":
        return "BIGINT";
      case "HUGEINT":
      case "UBIGINT":
        return "DECIMAL";
      case "FLOAT":
      case "REAL":
        return "FLOAT";
      case "DOUBLE":
        return "DOUBLE";
      case "VARCHAR":
      case "CHAR":
      case "BPCHAR":
      case "TEXT":
      case "STRING":
      case "UUID":
        return "VARCHAR";
      case "BLOB":
      case "BYTEA":
      case "BINARY":
        return "BINARY";
      case "DATE":
        return "DATE";
      case "TIMESTAMP":
      case "TIMESTAMP_NS":
      case "TIMESTAMP_MS":
      case "TIMESTAMP_S":
      case "DATETIME":
        return "TIMESTAMP";
      case "TIMESTAMP WITH TIME ZONE":
      case "TIMESTAMPTZ":
        return "TIMESTAMPTZ";
      default:
        throw new IllegalStateException(
            "Unsupported DuckDB type for Iceberg schema inference: '" + duckType
            + "'. Add an explicit mapping in duckdbTypeToCanonical.");
    }
  }

  /**
   * Drops an Iceberg table.
   *
   * @param config The catalog configuration
   * @param tableId The table identifier
   * @param purge If true, also delete all data files; if false, only drop metadata
   * @return true if the table was dropped, false if it didn't exist
   */
  public static boolean dropTable(Map<String, Object> config, String tableId, boolean purge) {
    String catalogType = (String) config.get("catalog");
    if (catalogType == null) {
      catalogType = "hadoop";
    }

    Catalog catalog = getCatalog(catalogType, config);
    TableIdentifier identifier = parseTableIdentifier(tableId, config);

    if (!catalog.tableExists(identifier)) {
      LOGGER.debug("Table {} does not exist, nothing to drop", identifier);
      return false;
    }

    LOGGER.info("Dropping Iceberg table: {} (purge={})", identifier, purge);
    return catalog.dropTable(identifier, purge);
  }

  /**
   * Checks if a table exists in the catalog.
   *
   * @param config The catalog configuration
   * @param tableId The table identifier
   * @return true if the table exists
   */
  public static boolean tableExists(Map<String, Object> config, String tableId) {
    String catalogType = (String) config.get("catalog");
    if (catalogType == null) {
      catalogType = "hadoop";
    }

    Catalog catalog = getCatalog(catalogType, config);
    TableIdentifier identifier = parseTableIdentifier(tableId, config);
    return catalog.tableExists(identifier);
  }

  /**
   * Lists all alternate partition tables (tables with _mv_ prefix) in the namespace.
   *
   * @param config The catalog configuration
   * @return List of alternate table identifiers
   */
  public static List<TableIdentifier> listAlternateTables(Map<String, Object> config) {
    String catalogType = (String) config.get("catalog");
    if (catalogType == null) {
      catalogType = "hadoop";
    }

    Catalog catalog = getCatalog(catalogType, config);
    String namespace = (String) config.get("namespace");

    List<TableIdentifier> alternates = new ArrayList<TableIdentifier>();
    // Use "default" namespace if none specified - HadoopCatalog requires a non-empty namespace
    Namespace ns;
    if (namespace != null && !namespace.isEmpty()) {
      ns = Namespace.of(namespace);
    } else {
      ns = Namespace.of("default");
    }

    try {
      for (TableIdentifier tableId : catalog.listTables(ns)) {
        if (tableId.name().startsWith(ALTERNATE_PREFIX)) {
          alternates.add(tableId);
        }
      }
    } catch (Exception e) {
      // Namespace may not exist yet, return empty list
      LOGGER.debug("Could not list tables in namespace {}: {}", ns, e.getMessage());
      return alternates;
    }

    LOGGER.debug("Found {} alternate tables in namespace {}", alternates.size(), ns);
    return alternates;
  }

  /**
   * Lists all alternate tables for a specific source table.
   * This uses a naming convention where alternates are tagged with source info.
   *
   * @param config The catalog configuration
   * @param sourceTableName The source table name
   * @return List of alternate table identifiers for the source
   */
  public static List<TableIdentifier> listAlternatesForSource(
      Map<String, Object> config, String sourceTableName) {
    // In the current implementation, we rely on the AlternateRegistry in FileSchema
    // to track source->alternate mappings. This method returns all alternates
    // since the naming convention (_mv_xxx) doesn't encode the source table.
    return listAlternateTables(config);
  }

  /**
   * Generates a random alternate table name with the _mv_ prefix.
   * Format: _mv_{random32} where random32 is 32 lowercase alphanumeric characters.
   *
   * @return The generated alternate table name
   */
  public static String generateAlternateName() {
    StringBuilder sb = new StringBuilder(ALTERNATE_PREFIX);
    for (int i = 0; i < RANDOM_NAME_LENGTH; i++) {
      sb.append(CHARS.charAt(RANDOM.nextInt(CHARS.length())));
    }
    return sb.toString();
  }

  /**
   * Checks if a table name is an alternate table (starts with _mv_).
   *
   * @param tableName The table name to check
   * @return true if this is an alternate table name
   */
  public static boolean isAlternateName(String tableName) {
    return tableName != null && tableName.startsWith(ALTERNATE_PREFIX);
  }

  /**
   * Column definition for schema creation.
   */
  public static class ColumnDef {
    private final String name;
    private final String type;
    private final String doc;

    /**
     * Creates a column definition with documentation.
     *
     * @param name Column name
     * @param type Column type (defaults to VARCHAR if null)
     * @param doc Column documentation/comment (can be null)
     */
    public ColumnDef(String name, String type, String doc) {
      this.name = name;
      this.type = type != null ? type : "VARCHAR";
      this.doc = doc;
    }

    /**
     * Creates a column definition without documentation.
     * Backward-compatible constructor.
     */
    public ColumnDef(String name, String type) {
      this(name, type, null);
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    /**
     * Returns the column documentation/comment.
     *
     * @return Column doc, or null if not set
     */
    public String getDoc() {
      return doc;
    }
  }
}
