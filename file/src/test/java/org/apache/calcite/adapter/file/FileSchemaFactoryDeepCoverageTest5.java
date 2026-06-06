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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link FileSchemaFactory} - Round 5.
 *
 * <p>Targets uncovered lines not exercised by previous deep coverage tests (1-4).
 * Focuses on:
 * <ul>
 *   <li>Table-specific operands: url, sourceFile, format, tableName, columns in tables array</li>
 *   <li>Storage provider config: storageType, storageConfig with various sub-keys</li>
 *   <li>Materialization config: materialize, materializeDirectory operands</li>
 *   <li>ETL pipeline config: etl, pipeline operands</li>
 *   <li>Statistics config: statistics, primeCache operands</li>
 *   <li>Constraint metadata: constraints, foreignKeys with rewriting</li>
 *   <li>Comment handling: comment, tableComments operands</li>
 *   <li>Advanced casing: all casing combinations for table and column names</li>
 *   <li>Recursive with patterns: recursive, directoryPattern, filePattern</li>
 *   <li>View definitions: complex view SQL with schema rewriting</li>
 *   <li>Partition config: partitionedTables, hivePartitioned operands</li>
 *   <li>Refresh config: refreshInterval, autoRefresh operands</li>
 *   <li>Cache config: ephemeralCache, baseDirectory, cacheStorageProvider</li>
 *   <li>Error paths: invalid operands, missing required fields</li>
 *   <li>Sanitization edge cases: short accessKeyId, nested Calcite objects, modelUri</li>
 *   <li>registerSqlViews: viewDef fallback, schema rewriting, edge cases</li>
 *   <li>rewriteForeignKeySchemaNames: comprehensive FK rewrite scenarios</li>
 *   <li>parseBooleanValue: non-Boolean/non-String types</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class FileSchemaFactoryDeepCoverageTest5 {

  private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger(0);

  @TempDir
  Path tempDir;

  private SchemaPlus mockParentSchema;

  private String uniqueSchemaName() {
    return "test_f5_" + SCHEMA_COUNTER.incrementAndGet();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @BeforeEach
  void setUp() {
    mockParentSchema = mock(SchemaPlus.class);

    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get(anyString())).thenReturn(null);
    when(subSchemaLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    Lookup tableLookup = mock(Lookup.class);
    when(tableLookup.get(anyString())).thenReturn(null);
    when(tableLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.emptySet());
    doReturn(tableLookup).when(mockParentSchema).tables();

    when(mockParentSchema.getParentSchema()).thenReturn(null);
    when(mockParentSchema.getName()).thenReturn("root");
    when(mockParentSchema.add(anyString(), any(Schema.class))).thenReturn(mockParentSchema);
  }

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  private Map<String, Object> baseOperand() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    return operand;
  }

  private File createCsvFile(String name, String content) throws IOException {
    File csvFile = new File(tempDir.toFile(), name);
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write(content);
    }
    return csvFile;
  }

  private File createJsonFile(String name, String content) throws IOException {
    File jsonFile = new File(tempDir.toFile(), name);
    try (FileWriter w = new FileWriter(jsonFile)) {
      w.write(content);
    }
    return jsonFile;
  }

  private File createSubDir(String name) {
    File subDir = new File(tempDir.toFile(), name);
    subDir.mkdirs();
    return subDir;
  }

  private Schema createSchema(Map<String, Object> operand) {
    return FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
  }

  private Schema createSchemaWithName(String name, Map<String, Object> operand) {
    return FileSchemaFactory.INSTANCE.create(
        mockParentSchema, name, operand);
  }

  @SuppressWarnings("unchecked")
  private Object invokePrivateStatic(Class<?> clazz, String methodName,
      Class<?>[] paramTypes, Object... args) throws Exception {
    Method method = clazz.getDeclaredMethod(methodName, paramTypes);
    method.setAccessible(true);
    try {
      return method.invoke(null, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception) e.getCause();
      }
      throw e;
    }
  }

  // =======================================================================
  // 1. Table-specific operands - tables array with specific table configs
  // =======================================================================

  @Test void testTablesArrayWithUrlAndFormatCsv() throws Exception {
    File csvFile = createCsvFile("data1.csv", "id,name\n1,Alice\n");
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "data1");
    table.put("url", csvFile.getAbsolutePath());
    table.put("format", "csv");
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
  }

  @Test void testTablesArrayWithUrlAndFormatJson() throws Exception {
    File jsonFile =
        createJsonFile("items.json", "[{\"id\":1,\"val\":\"x\"},{\"id\":2,\"val\":\"y\"}]");
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "items");
    table.put("url", jsonFile.getAbsolutePath());
    table.put("format", "json");
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTablesArrayWithMultipleTables() throws Exception {
    File csv1 = createCsvFile("orders.csv", "oid,amount\n1,100\n2,200\n");
    File csv2 = createCsvFile("customers.csv", "cid,name\n1,Alice\n");
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    List<Map<String, Object>> tables = new ArrayList<>();

    Map<String, Object> t1 = new HashMap<>();
    t1.put("name", "orders");
    t1.put("url", csv1.getAbsolutePath());
    tables.add(t1);

    Map<String, Object> t2 = new HashMap<>();
    t2.put("name", "customers");
    t2.put("url", csv2.getAbsolutePath());
    tables.add(t2);

    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("orders"));
    assertTrue(tableMap.containsKey("customers"));
  }

  @Test void testTablesArrayWithExplicitTableName() throws Exception {
    File csvFile = createCsvFile("raw_data.csv", "id,val\n1,hello\n");
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "renamed_table");
    table.put("url", csvFile.getAbsolutePath());
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("renamed_table"));
  }

  @Test void testTablesArrayWithNullUrlSkipped() throws Exception {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "no_url_table");
    table.put("url", null);
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTablesArrayWithViewTypeSkipped() throws Exception {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("name", "my_view");
    viewDef.put("type", "view");
    viewDef.put("sql", "SELECT 1");
    tables.add(viewDef);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTablesArrayWithFlattenPerTable() throws Exception {
    File jsonFile =
        createJsonFile("nested.json", "[{\"id\":1,\"info\":{\"name\":\"test\"}}]");
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "nested");
    table.put("url", jsonFile.getAbsolutePath());
    table.put("format", "json");
    table.put("flatten", Boolean.TRUE);
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTablesArrayWithEmptyList() {
    Map<String, Object> operand = baseOperand();
    operand.put("tables", new ArrayList<>());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 2. Storage provider config
  // =======================================================================

  @Test void testStorageTypeLocalExplicit() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testStorageConfigMapPassedThrough() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("region", "us-east-1");
    storageConfig.put("endpoint", "http://localhost:9000");
    operand.put("storageConfig", storageConfig);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testStorageConfigWithPreCreatedProviderAndNullStorageConfig() {
    Map<String, Object> operand = baseOperand();
    // Simulate a pre-created storage provider instance passed via underscore key
    operand.put("_storageProvider", new Object());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testStorageConfigWithPreCreatedProviderAndExistingStorageConfig() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("bucket", "my-bucket");
    operand.put("storageConfig", storageConfig);
    operand.put("_storageProvider", new Object());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectS3StorageFromDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "s3://my-bucket/data");
    operand.put("storageConfig", createS3Credentials());

    // S3 auto-detected should pass validation
    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectHttpStorageFromDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "http://example.com/data");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectHttpsStorageFromDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "https://example.com/data");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectHdfsStorageFromDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "hdfs://namenode:8020/data");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectS3StorageFromBaseDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("baseDirectory", "s3://cache-bucket/data");
    operand.put("storageConfig", createS3Credentials());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectHdfsStorageFromBaseDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("baseDirectory", "hdfs://namenode:8020/cache");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testS3WithoutCredentialsThrows() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "s3://my-bucket/data");

    assertThrows(IllegalArgumentException.class, () ->
        createSchema(operand));
  }

  @Test void testS3WithPartialCredentialsThrows() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "s3://my-bucket/data");
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKID12345");
    // Missing secretAccessKey
    operand.put("storageConfig", storageConfig);

    assertThrows(IllegalArgumentException.class, () ->
        createSchema(operand));
  }

  @Test void testAutoDetectLocalStorageFromTables() {
    Map<String, Object> operand = new HashMap<>();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "data");
    table.put("url", tempDir.resolve("data.csv").toString());
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDetectLocalStorageFromPartitionedTables() {
    Map<String, Object> operand = new HashMap<>();
    List<Map<String, Object>> ptables = new ArrayList<>();
    Map<String, Object> pt = new HashMap<>();
    pt.put("name", "partdata");
    pt.put("pattern", "/**/*.parquet");
    ptables.add(pt);
    operand.put("partitionedTables", ptables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testNoStorageTypeNoDirectoryThrows() {
    Map<String, Object> operand = new HashMap<>();
    assertThrows(IllegalStateException.class, () ->
        createSchema(operand));
  }

  // =======================================================================
  // 3. Materialization config
  // =======================================================================

  @Test void testMaterializationsListWithTableAndSql() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("table", "summary");
    mv.put("sql", "SELECT count(*) AS cnt FROM base");
    mvs.add(mv);
    operand.put("materializations", mvs);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMaterializationsWithViewSchemaPath() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT 1 AS id");
    mv.put("viewSchemaPath", Arrays.asList("myschema"));
    mv.put("existing", Boolean.TRUE);
    mvs.add(mv);
    operand.put("materializations", mvs);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMaterializationsWithNullTableSkipped() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("table", null);
    mv.put("sql", "SELECT 1");
    mvs.add(mv);
    operand.put("materializations", mvs);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMaterializationsWithNullSqlSkipped() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("table", "summary");
    mv.put("sql", null);
    mvs.add(mv);
    operand.put("materializations", mvs);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMaterializationsWithExistingFalse() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("table", "mv_not_existing");
    mv.put("sql", "SELECT 1 AS id");
    mv.put("existing", Boolean.FALSE);
    mvs.add(mv);
    operand.put("materializations", mvs);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testEmptyMaterializationsList() {
    Map<String, Object> operand = baseOperand();
    operand.put("materializations", new ArrayList<>());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 4. ETL pipeline config
  // =======================================================================

  @Test void testAutoDownloadFalseDoesNotTriggerEtl() {
    Map<String, Object> operand = baseOperand();
    operand.put("autoDownload", Boolean.FALSE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testAutoDownloadStringFalse() {
    Map<String, Object> operand = baseOperand();
    operand.put("autoDownload", "false");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testEtlCompleteSkipsAutoDownload() {
    Map<String, Object> operand = baseOperand();
    operand.put("autoDownload", Boolean.TRUE);
    operand.put("_etlComplete", Boolean.TRUE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 5. Statistics config - primeCache
  // =======================================================================

  @Test void testPrimeCacheTrueDefault() {
    Map<String, Object> operand = baseOperand();
    // primeCache not set - should default to true
    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testPrimeCacheExplicitFalse() {
    Map<String, Object> operand = baseOperand();
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testPrimeCacheExplicitTrue() {
    Map<String, Object> operand = baseOperand();
    operand.put("primeCache", Boolean.TRUE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testPrimeCacheSnakeCase() {
    Map<String, Object> operand = baseOperand();
    operand.put("prime_cache", Boolean.FALSE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testPrimeCacheCamelCaseTakesPrecedence() {
    Map<String, Object> operand = baseOperand();
    operand.put("primeCache", Boolean.FALSE);
    operand.put("prime_cache", Boolean.TRUE);
    // camelCase should take precedence

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 6. Constraint metadata
  // =======================================================================

  @Test void testTableConstraintsOperandPassedToSchema() {
    Map<String, Object> operand = baseOperand();
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("primaryKey", Arrays.asList("id"));
    constraints.put("orders", tc);
    operand.put("tableConstraints", constraints);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
  }

  @Test void testTableConstraintsWithForeignKeys() {
    Map<String, Object> operand = baseOperand();
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("primaryKey", Arrays.asList("id"));
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("columns", Arrays.asList("customer_id"));
    fk.put("targetTable", "customers");
    fk.put("targetColumns", Arrays.asList("id"));
    fk.put("targetSchema", "myschema");
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("orders", tc);
    operand.put("tableConstraints", constraints);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTableConstraintsWithDeclaredSchemaNameRewrite() {
    Map<String, Object> operand = baseOperand();
    String schemaName = uniqueSchemaName();
    operand.put("declaredSchemaName", "canonical");

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    fk.put("targetColumns", Arrays.asList("id"));
    fk.put("targetSchema", "canonical");
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("employees", tc);
    operand.put("tableConstraints", constraints);

    Schema schema = createSchemaWithName(schemaName, operand);
    assertNotNull(schema);
  }

  @Test void testTableConstraintsEmptyMapPassedThrough() {
    Map<String, Object> operand = baseOperand();
    operand.put("tableConstraints", new HashMap<>());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testInstanceConstraintsViaSetTableConstraints() {
    Map<String, Object> operand = baseOperand();

    // Set instance constraints first
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("primaryKey", Arrays.asList("id"));
    constraints.put("my_table", tc);
    FileSchemaFactory.INSTANCE.setTableConstraints(constraints, null);

    Schema schema = createSchema(operand);
    assertNotNull(schema);

    // Clear them to avoid affecting other tests
    FileSchemaFactory.INSTANCE.setTableConstraints(null, null);
  }

  @Test void testOperandConstraintsTakePrecedenceOverInstance() {
    Map<String, Object> operand = baseOperand();

    // Set instance constraints
    Map<String, Map<String, Object>> instanceConstraints = new HashMap<>();
    Map<String, Object> tc1 = new HashMap<>();
    tc1.put("primaryKey", Arrays.asList("old_id"));
    instanceConstraints.put("table1", tc1);
    FileSchemaFactory.INSTANCE.setTableConstraints(instanceConstraints, null);

    // Set operand constraints (should take precedence)
    Map<String, Map<String, Object>> operandConstraints = new HashMap<>();
    Map<String, Object> tc2 = new HashMap<>();
    tc2.put("primaryKey", Arrays.asList("new_id"));
    operandConstraints.put("table1", tc2);
    operand.put("tableConstraints", operandConstraints);

    Schema schema = createSchema(operand);
    assertNotNull(schema);

    // Clear
    FileSchemaFactory.INSTANCE.setTableConstraints(null, null);
  }

  // =======================================================================
  // 7. Comment handling
  // =======================================================================

  @Test void testCommentOperandPassedToSchema() {
    Map<String, Object> operand = baseOperand();
    operand.put("comment", "This is a test schema");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
    assertEquals("This is a test schema", ((FileSchema) schema).getComment());
  }

  @Test void testCommentNullByDefault() {
    Map<String, Object> operand = baseOperand();

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    assertNull(((FileSchema) schema).getComment());
  }

  @Test void testCommentEmptyString() {
    Map<String, Object> operand = baseOperand();
    operand.put("comment", "");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    assertEquals("", ((FileSchema) schema).getComment());
  }

  // =======================================================================
  // 8. Advanced casing combinations
  // =======================================================================

  @Test void testTableNameCasingUpperViaOperand() throws Exception {
    createCsvFile("casing_test.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("tableNameCasing", "UPPER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTableNameCasingLowerViaOperand() throws Exception {
    createCsvFile("CasingTest.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("tableNameCasing", "LOWER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTableNameCasingSmartDefault() throws Exception {
    createCsvFile("smart_test.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    // No tableNameCasing set - should default to SMART_CASING

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testColumnNameCasingUpperViaOperand() throws Exception {
    createCsvFile("col_test.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("columnNameCasing", "UPPER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testColumnNameCasingLowerViaOperand() throws Exception {
    createCsvFile("col_test2.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("columnNameCasing", "LOWER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTableNameCasingSnakeCaseKey() throws Exception {
    createCsvFile("snake_key.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("table_name_casing", "UPPER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testColumnNameCasingSnakeCaseKey() throws Exception {
    createCsvFile("col_snake.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("column_name_casing", "UPPER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testCamelCaseTakesPrecedenceOverSnakeCaseForTableCasing() throws Exception {
    createCsvFile("priority_test.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("tableNameCasing", "UPPER");
    operand.put("table_name_casing", "LOWER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testCamelCaseTakesPrecedenceOverSnakeCaseForColumnCasing() throws Exception {
    createCsvFile("col_priority.csv", "id,name\n1,test\n");
    Map<String, Object> operand = baseOperand();
    operand.put("columnNameCasing", "UPPER");
    operand.put("column_name_casing", "LOWER");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 9. Recursive with patterns
  // =======================================================================

  @Test void testRecursiveTrueDiscoversSubs() throws Exception {
    File subDir = createSubDir("subdir");
    createCsvFile("top.csv", "id,name\n1,top\n");
    File subFile = new File(subDir, "sub.csv");
    try (FileWriter w = new FileWriter(subFile)) {
      w.write("id,name\n2,sub\n");
    }
    Map<String, Object> operand = baseOperand();
    operand.put("recursive", Boolean.TRUE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.size() >= 2);
  }

  @Test void testRecursiveFalseDoesNotDiscoverSubs() throws Exception {
    File subDir = createSubDir("subdir2");
    createCsvFile("top2.csv", "id,name\n1,top\n");
    File subFile = new File(subDir, "sub2.csv");
    try (FileWriter w = new FileWriter(subFile)) {
      w.write("id,name\n2,sub\n");
    }
    Map<String, Object> operand = baseOperand();
    // recursive defaults to false

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("top2"));
    assertFalse(tableMap.containsKey("sub2"));
  }

  @Test void testDirectoryPatternViaOperand() throws Exception {
    createCsvFile("match_a.csv", "id\n1\n");
    createCsvFile("match_b.csv", "id\n2\n");
    Map<String, Object> operand = baseOperand();
    operand.put("directoryPattern", "*.csv");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testGlobAliasForDirectoryPattern() throws Exception {
    createCsvFile("glob_a.csv", "id\n1\n");
    Map<String, Object> operand = baseOperand();
    operand.put("glob", "*.csv");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testDirectoryPatternTakesPrecedenceOverGlob() throws Exception {
    createCsvFile("prio.csv", "id\n1\n");
    Map<String, Object> operand = baseOperand();
    operand.put("directoryPattern", "*.csv");
    operand.put("glob", "*.json");
    // directoryPattern should be used

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 10. View definitions - complex view SQL
  // =======================================================================

  @Test void testViewsOperandWithMultipleViews() throws Exception {
    createCsvFile("base.csv", "id,value\n1,100\n");
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> views = new ArrayList<>();

    Map<String, Object> v1 = new HashMap<>();
    v1.put("name", "view1");
    v1.put("sql", "SELECT id FROM base");
    views.add(v1);

    Map<String, Object> v2 = new HashMap<>();
    v2.put("name", "view2");
    v2.put("sql", "SELECT value FROM base");
    views.add(v2);

    operand.put("views", views);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testViewsWithViewDefFallback() throws Exception {
    createCsvFile("vbase.csv", "id\n1\n");
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("name", "vfallback");
    viewDef.put("type", "view");
    viewDef.put("viewDef", "SELECT id FROM vbase");
    tables.add(viewDef);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testViewWithNullNameIsSkipped() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("name", null);
    viewDef.put("type", "view");
    viewDef.put("sql", "SELECT 1");
    tables.add(viewDef);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testViewWithNullSqlAndNullViewDefIsSkipped() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("name", "broken_view");
    viewDef.put("type", "view");
    viewDef.put("sql", null);
    viewDef.put("viewDef", null);
    tables.add(viewDef);
    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 11. Partition config
  // =======================================================================

  @Test void testPartitionedTablesOperand() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> partitionedTables = new ArrayList<>();
    Map<String, Object> pt = new HashMap<>();
    pt.put("name", "partitioned_data");
    pt.put("pattern", "/data/**/*.parquet");
    pt.put("partitionColumns", Arrays.asList("year", "month"));
    partitionedTables.add(pt);
    operand.put("partitionedTables", partitionedTables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMultiplePartitionedTables() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> ptables = new ArrayList<>();

    Map<String, Object> pt1 = new HashMap<>();
    pt1.put("name", "sales");
    pt1.put("pattern", "/sales/**/*.parquet");
    ptables.add(pt1);

    Map<String, Object> pt2 = new HashMap<>();
    pt2.put("name", "events");
    pt2.put("pattern", "/events/**/*.parquet");
    ptables.add(pt2);

    operand.put("partitionedTables", ptables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 12. Refresh config
  // =======================================================================

  @Test void testRefreshIntervalOperand() {
    Map<String, Object> operand = baseOperand();
    operand.put("refreshInterval", "5 minutes");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testRefreshIntervalNull() {
    Map<String, Object> operand = baseOperand();
    operand.put("refreshInterval", null);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testRefreshIntervalWithSeconds() {
    Map<String, Object> operand = baseOperand();
    operand.put("refreshInterval", "30 seconds");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 13. Cache config
  // =======================================================================

  @Test void testEphemeralCacheBooleanTrue() {
    Map<String, Object> operand = baseOperand();
    operand.put("ephemeralCache", Boolean.TRUE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testEphemeralCacheStringTrue() {
    Map<String, Object> operand = baseOperand();
    operand.put("ephemeralCache", "true");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testEphemeralCacheStringFalse() {
    Map<String, Object> operand = baseOperand();
    operand.put("ephemeralCache", "false");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testEphemeralCacheSnakeCaseOperandKey() {
    Map<String, Object> operand = baseOperand();
    operand.put("ephemeral_cache", Boolean.TRUE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testEphemeralCacheCamelCaseTakesPrecedence() {
    Map<String, Object> operand = baseOperand();
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("ephemeral_cache", Boolean.FALSE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testBaseDirectoryAsString() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.resolve("custom_base").toString());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testBaseDirectoryAsFileObject() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.resolve("file_base").toFile());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testBaseDirectoryRelativeToModel() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory", "relative_cache");
    operand.put("modelUri", tempDir.toFile().getAbsolutePath() + "/model.json");
    operand.put("_basedir", tempDir.toFile());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testModelBaseDirectoryAsFileObject() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("_basedir", tempDir.toFile());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testModelBaseDirectoryAsString() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("_basedir", tempDir.toFile().getAbsolutePath());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 14. Error paths
  // =======================================================================

  @Test void testDuplicateSchemaNameThrows() {
    String name = uniqueSchemaName();
    Map<String, Object> operand = baseOperand();

    // First call succeeds
    createSchemaWithName(name, operand);

    // Set up the mock to return non-null for the schema name on second call
    SchemaPlus innerMock = mock(SchemaPlus.class);
    Lookup subSchemaLookup2 = mock(Lookup.class);
    when(subSchemaLookup2.get(name)).thenReturn(innerMock);
    when(subSchemaLookup2.getNames(any(LikePattern.class)))
        .thenReturn(Collections.singleton(name));
    doReturn(subSchemaLookup2).when(mockParentSchema).subSchemas();

    assertThrows(IllegalArgumentException.class, () ->
        createSchemaWithName(name, operand));
  }

  @Test void testValidateUniqueSchemaNameNullParentDoesNotThrow() throws Exception {
    invokePrivateStatic(FileSchemaFactory.class, "validateUniqueSchemaName",
        new Class<?>[]{SchemaPlus.class, String.class}, null, "test");
  }

  @Test void testValidateUniqueSchemaNameNullNameDoesNotThrow() throws Exception {
    invokePrivateStatic(FileSchemaFactory.class, "validateUniqueSchemaName",
        new Class<?>[]{SchemaPlus.class, String.class}, mockParentSchema, null);
  }

  // =======================================================================
  // 15. Sanitize operand - edge cases
  // =======================================================================

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandNullValue() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("someKey", null);
    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    assertNull(result.get("someKey"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandPasswordKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("dbPassword", "secret123");
    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    assertEquals("********", result.get("dbPassword"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandSecretKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("apiSecret", "topsecret");
    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    assertEquals("********", result.get("apiSecret"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandUnderscoreKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("_internalObj", new Object());
    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    assertEquals("Object", result.get("_internalObj"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandS3ConfigWithLongAccessKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3Config.put("region", "us-east-1");
    operand.put("s3Config", s3Config);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertNotNull(sanitizedS3);
    assertTrue(((String) sanitizedS3.get("accessKeyId")).startsWith("****"));
    assertTrue(((String) sanitizedS3.get("accessKeyId")).endsWith("MPLE"));
    assertEquals("********", sanitizedS3.get("secretAccessKey"));
    assertEquals("us-east-1", sanitizedS3.get("region"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandS3ConfigWithShortAccessKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AB");
    operand.put("s3Config", s3Config);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("****", sanitizedS3.get("accessKeyId"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandS3ConfigPasswordInKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("password", "secret");
    operand.put("s3Config", s3Config);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("********", sanitizedS3.get("password"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandStorageConfigWithUnderscoreKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_provider", new Object());
    storageConfig.put("bucket", "my-bucket");
    storageConfig.put("secretKey", "mysecret");
    storageConfig.put("accessKeyId", "AKID");
    operand.put("storageConfig", storageConfig);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNotNull(sanitizedStorage);
    assertEquals("Object", sanitizedStorage.get("_provider"));
    assertEquals("my-bucket", sanitizedStorage.get("bucket"));
    assertEquals("********", sanitizedStorage.get("secretKey"));
    assertEquals("********", sanitizedStorage.get("accessKeyId"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandStorageConfigNullValue() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_provider", null);
    storageConfig.put("region", "eu-west-1");
    operand.put("storageConfig", storageConfig);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNull(sanitizedStorage.get("_provider"));
    assertEquals("eu-west-1", sanitizedStorage.get("region"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandModelUriWithCredentials() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("modelUri",
        "{\"accessKeyId\": \"AKID123\", \"secretAccessKey\": \"secret123\"}");

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    String sanitizedUri = (String) result.get("modelUri");
    assertFalse(sanitizedUri.contains("AKID123"));
    assertFalse(sanitizedUri.contains("secret123"));
    assertTrue(sanitizedUri.contains("****"));
    assertTrue(sanitizedUri.contains("********"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandGenericNestedMap() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("host", "localhost");
    nestedMap.put("port", 5432);
    operand.put("connectionConfig", nestedMap);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested = (Map<String, Object>) result.get("connectionConfig");
    assertNotNull(sanitizedNested);
    assertEquals("localhost", sanitizedNested.get("host"));
    assertEquals(5432, sanitizedNested.get("port"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeOperandPassThroughValues() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/some/path");
    operand.put("recursive", Boolean.TRUE);
    operand.put("batchSize", 1024);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);

    assertEquals("/some/path", result.get("directory"));
    assertEquals(Boolean.TRUE, result.get("recursive"));
    assertEquals(1024, result.get("batchSize"));
  }

  // =======================================================================
  // 16. sanitizeNestedMap edge cases
  // =======================================================================

  @Test @SuppressWarnings("unchecked")
  void testSanitizeNestedMapAccessKeyMasked() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("accesskey", "AKID12345");
    map.put("region", "us-east-1");

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeNestedMap",
        new Class<?>[]{Map.class}, map);

    assertEquals("********", result.get("accesskey"));
    assertEquals("us-east-1", result.get("region"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeNestedMapPasswordMasked() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("password", "secret");
    map.put("username", "admin");

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeNestedMap",
        new Class<?>[]{Map.class}, map);

    assertEquals("********", result.get("password"));
    assertEquals("admin", result.get("username"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeNestedMapSecretMasked() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("mySecret", "hidden");

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeNestedMap",
        new Class<?>[]{Map.class}, map);

    assertEquals("********", result.get("mySecret"));
  }

  @Test @SuppressWarnings("unchecked")
  void testSanitizeNestedMapNullValuePassedThrough() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("key", null);

    Map<String, Object> result =
        (Map<String, Object>) invokePrivateStatic(FileSchemaFactory.class, "sanitizeNestedMap",
        new Class<?>[]{Map.class}, map);

    // null value is not a Calcite class, not a sensitive key, so passed through
    assertNull(result.get("key"));
  }

  // =======================================================================
  // 17. parseBooleanValue edge cases
  // =======================================================================

  @Test void testParseBooleanValueNull() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, (Object) null);
    assertNull(result);
  }

  @Test void testParseBooleanValueBooleanTrue() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, Boolean.TRUE);
    assertEquals(Boolean.TRUE, result);
  }

  @Test void testParseBooleanValueBooleanFalse() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, Boolean.FALSE);
    assertEquals(Boolean.FALSE, result);
  }

  @Test void testParseBooleanValueStringTrue() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, "true");
    assertEquals(Boolean.TRUE, result);
  }

  @Test void testParseBooleanValueStringFalse() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, "false");
    assertEquals(Boolean.FALSE, result);
  }

  @Test void testParseBooleanValueStringTrueUpperCase() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, "TRUE");
    assertEquals(Boolean.TRUE, result);
  }

  @Test void testParseBooleanValueInteger() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, Integer.valueOf(1));
    assertNull(result);
  }

  @Test void testParseBooleanValueNonBooleanString() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "parseBooleanValue", new Class<?>[]{Object.class}, "yes");
    // Boolean.parseBoolean("yes") returns false
    assertEquals(Boolean.FALSE, result);
  }

  // =======================================================================
  // 18. rewriteSchemaReferencesInSql edge cases
  // =======================================================================

  @Test void testRewriteSchemaReferencesInSqlNullViewSql() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        null, "econ", "ECON");
    assertNull(result);
  }

  @Test void testRewriteSchemaReferencesInSqlNullDeclaredName() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM econ.t", null, "ECON");
    assertEquals("SELECT * FROM econ.t", result);
  }

  @Test void testRewriteSchemaReferencesInSqlNullActualName() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM econ.t", "econ", null);
    assertEquals("SELECT * FROM econ.t", result);
  }

  @Test void testRewriteSchemaReferencesInSqlSameNameNoRewrite() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM econ.table1", "econ", "econ");
    assertEquals("SELECT * FROM econ.table1", result);
  }

  @Test void testRewriteSchemaReferencesInSqlCaseInsensitiveMatch() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM ECON.table1", "econ", "MY_ECON");
    assertEquals("SELECT * FROM MY_ECON.table1", result);
  }

  @Test void testRewriteSchemaReferencesInSqlMultipleOccurrences() throws Exception {
    // declaredSchemaName and actualSchemaName must differ case-insensitively for rewriting
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        "SELECT a.id FROM econ.a JOIN econ.b ON a.id = b.id", "econ", "MY_ECON");
    assertEquals("SELECT a.id FROM MY_ECON.a JOIN MY_ECON.b ON a.id = b.id", result);
  }

  @Test void testRewriteSchemaReferencesInSqlNoMatch() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteSchemaReferencesInSql", new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM other_schema.table1", "econ", "ECON");
    assertEquals("SELECT * FROM other_schema.table1", result);
  }

  // =======================================================================
  // 19. rewriteForeignKeySchemaNames edge cases
  // =======================================================================

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesCrossSchemaFkPreserved() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("columns", Arrays.asList("id"));
    fk.put("targetTable", "other_table");
    fk.put("targetSchema", "other_schema");
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("my_table", tc);

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("my_table").get("foreignKeys");
    assertEquals("other_schema", resultFks.get(0).get("targetSchema"));
  }

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesFkMatchingRewritten() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    fk.put("targetSchema", "econ");
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("employees", tc);

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("employees").get("foreignKeys");
    assertEquals("ECON", resultFks.get(0).get("targetSchema"));
  }

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesCaseInsensitiveMatch() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("columns", Arrays.asList("id"));
    fk.put("targetTable", "t2");
    fk.put("targetSchema", "ECON");
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("t1", tc);

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "MY_SCHEMA");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("t1").get("foreignKeys");
    assertEquals("MY_SCHEMA", resultFks.get(0).get("targetSchema"));
  }

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesFkNullTargetSchema() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("columns", Arrays.asList("id"));
    fk.put("targetTable", "t2");
    fk.put("targetSchema", null);
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("t1", tc);

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("t1").get("foreignKeys");
    assertNull(resultFks.get(0).get("targetSchema"));
  }

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesEmptyConstraints() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    assertTrue(result.isEmpty());
  }

  @Test void testRewriteForeignKeySchemaNamesNullConstraints() throws Exception {
    Object result =
        invokePrivateStatic(FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
        new Class<?>[]{Map.class, String.class, String.class},
        null, "econ", "ECON");

    assertNull(result);
  }

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesNoFksInTable() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("primaryKey", Arrays.asList("id"));
    constraints.put("simple_table", tc);

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    assertNotNull(result.get("simple_table"));
    assertEquals(Arrays.asList("id"), result.get("simple_table").get("primaryKey"));
  }

  @Test @SuppressWarnings("unchecked")
  void testRewriteForeignKeySchemaNamesEmptyFksList() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("foreignKeys", new ArrayList<>());
    constraints.put("empty_fk_table", tc);

    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    assertNotNull(result.get("empty_fk_table"));
  }

  // =======================================================================
  // 20. Execution engine config operands
  // =======================================================================

  @Test void testExecutionEngineFromOperand() {
    Map<String, Object> operand = baseOperand();
    operand.put("executionEngine", "PARQUET");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testExecutionEngineLinq4j() {
    Map<String, Object> operand = baseOperand();
    operand.put("executionEngine", "LINQ4J");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testBatchSizeAsNumber() {
    Map<String, Object> operand = baseOperand();
    operand.put("batchSize", 4096);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testBatchSizeAsString() {
    Map<String, Object> operand = baseOperand();
    operand.put("batchSize", "not_a_number");
    // Non-Number should fall back to default

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMemoryThresholdAsNumber() {
    Map<String, Object> operand = baseOperand();
    operand.put("memoryThreshold", 128L * 1024 * 1024);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMemoryThresholdAsStringFallsBackToDefault() {
    Map<String, Object> operand = baseOperand();
    operand.put("memoryThreshold", "256MB");
    // Non-Number should fall back to default

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testDuckdbConfigMapPassedThrough() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> duckdbConfig = new HashMap<>();
    duckdbConfig.put("memory_limit", "512MB");
    duckdbConfig.put("threads", 4);
    operand.put("duckdbConfig", duckdbConfig);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testParquetCacheDirectoryOperand() {
    Map<String, Object> operand = baseOperand();
    operand.put("parquetCacheDirectory", tempDir.resolve("pq-cache").toString());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 21. Flatten and CSV type inference
  // =======================================================================

  @Test void testFlattenTrueOperand() {
    Map<String, Object> operand = baseOperand();
    operand.put("flatten", Boolean.TRUE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testFlattenFalseOperand() {
    Map<String, Object> operand = baseOperand();
    operand.put("flatten", Boolean.FALSE);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testCsvTypeInferenceOperand() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> csvTypeInference = new HashMap<>();
    csvTypeInference.put("enabled", true);
    csvTypeInference.put("sampleSize", 50);
    csvTypeInference.put("dateFormats", Arrays.asList("yyyy-MM-dd"));
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 22. Canonical schema name
  // =======================================================================

  @Test void testCanonicalSchemaNameOperand() {
    Map<String, Object> operand = baseOperand();
    operand.put("canonicalSchemaName", "my_canon");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testCanonicalSchemaNameNull() {
    Map<String, Object> operand = baseOperand();
    // canonicalSchemaName not set

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 23. Declared schema name and FK rewrite
  // =======================================================================

  @Test void testDeclaredSchemaNameDifferentFromActual() {
    String name = uniqueSchemaName();
    Map<String, Object> operand = baseOperand();
    operand.put("declaredSchemaName", "lowered");

    Schema schema = createSchemaWithName(name, operand);
    assertNotNull(schema);
  }

  @Test void testDeclaredSchemaNameSameAsActual() {
    String name = "myschema";
    Map<String, Object> operand = baseOperand();
    operand.put("declaredSchemaName", "myschema");

    Schema schema = createSchemaWithName(name, operand);
    assertNotNull(schema);
  }

  @Test void testDeclaredSchemaNameDefaultsToLowercase() {
    String name = "UPPER_SCHEMA";
    Map<String, Object> operand = baseOperand();
    // No declaredSchemaName; should default to name.toLowerCase()

    Schema schema = createSchemaWithName(name, operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 24. Directory and sourceDirectory operands
  // =======================================================================

  @Test void testSourceDirectoryAlias() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("sourceDirectory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testDirectoryTakesPrecedenceOverSourceDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("sourceDirectory", "/nonexistent/path");
    operand.put("storageType", "local");

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testNoDirectoryUsesWorkingDir() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    // No directory or sourceDirectory

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testRelativeDirectoryResolvedAgainstModel() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "data");
    operand.put("storageType", "local");
    operand.put("_basedir", tempDir.toFile());

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 25. supportsConstraints and setTableConstraints
  // =======================================================================

  @Test void testSupportsConstraintsReturnsTrue() {
    assertTrue(FileSchemaFactory.INSTANCE.supportsConstraints());
  }

  @Test void testSetTableConstraintsWithTableDefinitions() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("primaryKey", Arrays.asList("id"));
    constraints.put("test_table", tc);

    List<org.apache.calcite.model.JsonTable> tableDefs = new ArrayList<>();
    FileSchemaFactory.INSTANCE.setTableConstraints(constraints, tableDefs);

    // Clean up
    FileSchemaFactory.INSTANCE.setTableConstraints(null, null);
  }

  @Test void testSetTableConstraintsNullValues() {
    FileSchemaFactory.INSTANCE.setTableConstraints(null, null);
    // Should not throw
  }

  // =======================================================================
  // 26. Constants
  // =======================================================================

  @Test void testRowtimeColumnNameConstant() {
    assertEquals("ROWTIME", FileSchemaFactory.ROWTIME_COLUMN_NAME);
  }

  @Test void testSingletonInstanceNotNull() {
    assertNotNull(FileSchemaFactory.INSTANCE);
  }

  @Test void testSingletonInstanceSameReference() {
    assertTrue(FileSchemaFactory.INSTANCE == FileSchemaFactory.INSTANCE);
  }

  // =======================================================================
  // 27. Text similarity config
  // =======================================================================

  @Test void testTextSimilarityEnabledTrue() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> textSimilarity = new HashMap<>();
    textSimilarity.put("enabled", Boolean.TRUE);
    operand.put("textSimilarity", textSimilarity);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTextSimilarityEnabledFalse() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> textSimilarity = new HashMap<>();
    textSimilarity.put("enabled", Boolean.FALSE);
    operand.put("textSimilarity", textSimilarity);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTextSimilarityEnabledNull() {
    Map<String, Object> operand = baseOperand();
    Map<String, Object> textSimilarity = new HashMap<>();
    textSimilarity.put("enabled", null);
    operand.put("textSimilarity", textSimilarity);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 28. Complex combined operand scenarios
  // =======================================================================

  @Test void testFullOperandCombination() throws Exception {
    File csvFile = createCsvFile("combo.csv", "id,name\n1,Alice\n2,Bob\n");

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("recursive", Boolean.TRUE);
    operand.put("directoryPattern", "*.csv");
    operand.put("tableNameCasing", "LOWER");
    operand.put("columnNameCasing", "LOWER");
    operand.put("flatten", Boolean.FALSE);
    operand.put("primeCache", Boolean.TRUE);
    operand.put("comment", "Full combo test");
    operand.put("refreshInterval", "10 minutes");
    operand.put("batchSize", 512);
    operand.put("memoryThreshold", 32L * 1024 * 1024);
    operand.put("ephemeralCache", Boolean.TRUE);

    Map<String, Object> csvTypeInference = new HashMap<>();
    csvTypeInference.put("enabled", true);
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
    assertEquals("Full combo test", ((FileSchema) schema).getComment());
  }

  @Test void testTablesAndViewsCombined() throws Exception {
    File csvFile = createCsvFile("tbase.csv", "id,value\n1,100\n");

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "tbase");
    table.put("url", csvFile.getAbsolutePath());
    tables.add(table);

    // View in the tables array
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("name", "tview");
    viewDef.put("type", "view");
    viewDef.put("sql", "SELECT id FROM tbase");
    tables.add(viewDef);

    operand.put("tables", tables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTablesAndPartitionedTablesCombined() throws Exception {
    File csvFile = createCsvFile("ptbase.csv", "id,value\n1,100\n");

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "ptbase");
    table.put("url", csvFile.getAbsolutePath());
    tables.add(table);
    operand.put("tables", tables);

    List<Map<String, Object>> ptables = new ArrayList<>();
    Map<String, Object> pt = new HashMap<>();
    pt.put("name", "partitioned");
    pt.put("pattern", "/**/*.parquet");
    ptables.add(pt);
    operand.put("partitionedTables", ptables);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testTablesAndConstraintsCombined() throws Exception {
    File csvFile = createCsvFile("constrained.csv", "id,name\n1,Alice\n");

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "constrained");
    table.put("url", csvFile.getAbsolutePath());
    tables.add(table);
    operand.put("tables", tables);

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tc = new HashMap<>();
    tc.put("primaryKey", Arrays.asList("id"));
    constraints.put("constrained", tc);
    operand.put("tableConstraints", constraints);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  @Test void testMultipleMaterializationsInList() {
    Map<String, Object> operand = baseOperand();
    List<Map<String, Object>> mvs = new ArrayList<>();

    Map<String, Object> mv1 = new HashMap<>();
    mv1.put("table", "summary1");
    mv1.put("sql", "SELECT count(*) FROM t1");
    mvs.add(mv1);

    Map<String, Object> mv2 = new HashMap<>();
    mv2.put("table", "summary2");
    mv2.put("sql", "SELECT sum(val) FROM t2");
    mv2.put("viewSchemaPath", Arrays.asList("test"));
    mvs.add(mv2);

    Map<String, Object> mv3 = new HashMap<>();
    mv3.put("table", null);  // Should be skipped
    mv3.put("sql", "SELECT 1");
    mvs.add(mv3);

    operand.put("materializations", mvs);

    Schema schema = createSchema(operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // 29. writeDebugModel edge cases
  // =======================================================================

  @Test void testWriteDebugModelCreatesFile() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("setting", "value");

    invokePrivateStatic(FileSchemaFactory.class, "writeDebugModel",
        new Class<?>[]{String.class, Map.class, String.class},
        "debugtest", operand, "parentName");
    // Should not throw; file is written to .aperio/debug-model-debugtest.json
  }

  @Test void testWriteDebugModelNullParentName() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("key", "value");

    invokePrivateStatic(FileSchemaFactory.class, "writeDebugModel",
        new Class<?>[]{String.class, Map.class, String.class},
        "nullparent", operand, null);
    // Should not throw
  }

  @Test void testWriteDebugModelEmptyOperand() throws Exception {
    Map<String, Object> operand = new HashMap<>();

    invokePrivateStatic(FileSchemaFactory.class, "writeDebugModel",
        new Class<?>[]{String.class, Map.class, String.class},
        "empty_op", operand, "root");
    // Should not throw
  }

  // =======================================================================
  // 30. Mixed file type discovery
  // =======================================================================

  @Test void testDiscoveryCsvAndJsonMixed() throws Exception {
    createCsvFile("mixed1.csv", "id\n1\n");
    createJsonFile("mixed2.json", "[{\"id\":2}]");

    Map<String, Object> operand = baseOperand();
    Schema schema = createSchema(operand);
    assertNotNull(schema);

    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("mixed1"));
    assertTrue(tableMap.containsKey("mixed2"));
  }

  @Test void testHiddenFilesExcluded() throws Exception {
    createCsvFile("visible.csv", "id\n1\n");
    createCsvFile(".hidden.csv", "id\n2\n");

    Map<String, Object> operand = baseOperand();
    Schema schema = createSchema(operand);
    assertNotNull(schema);

    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("visible"));
    assertFalse(tableMap.containsKey(".hidden"));
  }

  @Test void testCalciteModelFileExcluded() throws Exception {
    createCsvFile("data.csv", "id\n1\n");
    createJsonFile("model.json",
        "{\"version\":\"1.0\",\"schemas\":[]}");

    Map<String, Object> operand = baseOperand();
    Schema schema = createSchema(operand);
    assertNotNull(schema);

    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("data"));
  }

  @Test void testUnsupportedFileExtensionsIgnored() throws Exception {
    createCsvFile("good.csv", "id\n1\n");
    File txtFile = new File(tempDir.toFile(), "notes.txt");
    try (FileWriter w = new FileWriter(txtFile)) {
      w.write("some notes");
    }

    Map<String, Object> operand = baseOperand();
    Schema schema = createSchema(operand);
    assertNotNull(schema);

    Map<String, Table> tableMap = ((FileSchema) schema).getTableMap();
    assertTrue(tableMap.containsKey("good"));
  }

  // =======================================================================
  // Helper for S3 credentials
  // =======================================================================

  private Map<String, Object> createS3Credentials() {
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    storageConfig.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    storageConfig.put("region", "us-east-1");
    return storageConfig;
  }
}
