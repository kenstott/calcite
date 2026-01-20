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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Model-level lifecycle processor for file adapter.
 *
 * <p>Orchestrates ETL and schema creation across multiple schemas,
 * sharing storage providers and coordinating cross-schema concerns.
 *
 * <p>The processor manages two storage providers:
 * <ul>
 *   <li><b>sourceStorage</b> - For raw data downloads and caching</li>
 *   <li><b>materializedStorage</b> - For Parquet/Iceberg output</li>
 * </ul>
 *
 * <p>These are created once and shared across all schemas to ensure
 * thread-safe coordination via shared locks.
 *
 * <h3>Lifecycle</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │  ModelLifecycleProcessor.process()                              │
 * ├─────────────────────────────────────────────────────────────────┤
 * │  1. Initialize shared resources                                 │
 * │     ├─ sourceStorage (raw data)                                 │
 * │     ├─ materializedStorage (parquet/iceberg)                    │
 * │     └─ operatingDirectory (.aperio/ - local, for locks)         │
 * │                                                                 │
 * │  2. For each schema:                                            │
 * │     ├─ Create FileSchemaBuilder with shared resources           │
 * │     ├─ factory.configureHooks() - apply schema hooks            │
 * │     ├─ hookOverrides.accept() - apply user overrides            │
 * │     ├─ Run ETL (if autoDownload)                                │
 * │     └─ Create and register Calcite schema                       │
 * │                                                                 │
 * │  3. Return SchemaPlus ready for SQL queries                     │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * SchemaPlus rootSchema = ModelLifecycleProcessor.builder()
 *     .sourceStorage(StorageProviderFactory.createFromUrl("s3://bucket/raw/"))
 *     .materializedStorage(StorageProviderFactory.createFromUrl("s3://bucket/parquet/"))
 *     .operatingDirectory(".aperio/govdata")
 *
 *     // Factory's default hooks
 *     .addSchema("ECON_REFERENCE", new EconReferenceSchemaFactory(), operand)
 *     .addSchema("ECON", new EconSchemaFactory(), operand)
 *
 *     // Factory hooks + user overrides
 *     .addSchema("GEO", new GeoSchemaFactory(), operand, builder -> {
 *         builder.isEnabled("tiger_roads", ctx -> false);
 *     })
 *
 *     .build()
 *     .process();
 *
 * // Ready for SQL
 * conn.unwrap(CalciteConnection.class).setRootSchema(rootSchema);
 * }</pre>
 *
 * @see SubSchemaFactory
 * @see FileSchemaBuilder
 */
public class ModelLifecycleProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModelLifecycleProcessor.class);

  private final StorageProvider sourceStorage;
  private final StorageProvider materializedStorage;
  private final String operatingDirectory;
  private final IncrementalTracker incrementalTracker;
  private final List<SchemaDefinition> schemas;

  private ModelLifecycleProcessor(Builder builder) {
    this.sourceStorage = builder.sourceStorage;
    this.materializedStorage = builder.materializedStorage;
    this.operatingDirectory = builder.operatingDirectory;
    this.incrementalTracker = builder.incrementalTracker != null
        ? builder.incrementalTracker : IncrementalTracker.NOOP;
    this.schemas = new ArrayList<>(builder.schemas);
  }

  /**
   * Creates a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Run ETL and create schemas.
   *
   * @return ProcessResult containing rootSchema and created schemas
   */
  public ProcessResult process() {
    LOGGER.info("Starting model lifecycle processing with {} schemas", schemas.size());
    long startTime = System.currentTimeMillis();

    // Ensure operating directory exists
    if (operatingDirectory != null) {
      File opDir = new File(operatingDirectory);
      if (!opDir.exists()) {
        opDir.mkdirs();
        LOGGER.debug("Created operating directory: {}", operatingDirectory);
      }
    }

    // Create root schema
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Map<String, Schema> createdSchemas = new HashMap<>();

    // Process each schema definition
    for (int i = 0; i < schemas.size(); i++) {
      SchemaDefinition def = schemas.get(i);
      LOGGER.info("Processing schema {}/{}: {}", i + 1, schemas.size(), def.name);

      try {
        Schema schema = processSchema(def, rootSchema);
        rootSchema.add(def.name, schema);
        createdSchemas.put(def.name, schema);
        LOGGER.info("Schema '{}' created successfully", def.name);
      } catch (Exception e) {
        LOGGER.error("Failed to create schema '{}': {}", def.name, e.getMessage(), e);
        throw new RuntimeException("Failed to create schema: " + def.name, e);
      }
    }

    long elapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Model lifecycle complete: {} schemas in {}ms", schemas.size(), elapsed);

    return new ProcessResult(rootSchema, createdSchemas);
  }

  /**
   * Result of processing containing root schema and all created schemas.
   *
   * <p>The createdSchemas map provides direct access to the underlying Schema
   * objects without SchemaPlus wrappers. This is essential for returning schemas
   * from SchemaFactory.create() because Calcite's snapshot mechanism fails
   * with SchemaPlus wrappers (SchemaPlusImpl.snapshot() throws UnsupportedOperationException).
   */
  public static class ProcessResult {
    private final SchemaPlus rootSchema;
    private final Map<String, Schema> createdSchemas;

    ProcessResult(SchemaPlus rootSchema, Map<String, Schema> createdSchemas) {
      this.rootSchema = rootSchema;
      this.createdSchemas = createdSchemas;
    }

    /** Returns the root schema containing all created schemas. */
    public SchemaPlus getRootSchema() {
      return rootSchema;
    }

    /**
     * Returns the underlying Schema (not SchemaPlus wrapper) for the given name.
     * This should be used when returning from SchemaFactory.create().
     */
    public Schema getSchema(String name) {
      return createdSchemas.get(name);
    }
  }

  /**
   * Process a single schema definition.
   */
  private Schema processSchema(SchemaDefinition def, SchemaPlus parentSchema) {
    // Set SCHEMA_NAME property BEFORE loading schema resource
    // This ensures ${SCHEMA_NAME:default} patterns in YAML resolve to this schema's name
    // Critical: Must be set before schemaResource() call which resolves env vars
    System.setProperty("SCHEMA_NAME", def.name);

    // Create builder with shared resources
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaResource(def.factory.getSchemaResourceName())
        .operand(def.operand);

    // Set shared storage providers
    if (materializedStorage != null) {
      builder.storageProvider(materializedStorage);
      // Also put in operand so configureHooks() can access it
      def.operand.put("_storageProvider", materializedStorage);
    }
    if (sourceStorage != null) {
      builder.cacheStorageProvider(sourceStorage);
      def.operand.put("_cacheStorageProvider", sourceStorage);
    }

    // Use schema-specific tracker (captured when addSchema was called)
    // This ensures each schema stores completion status in its own database
    IncrementalTracker schemaTracker = def.incrementalTracker != null
        ? def.incrementalTracker : this.incrementalTracker;
    builder.incrementalTracker(schemaTracker);

    // Use schema-specific operating directory
    String schemaOpDir = def.operatingDirectory != null
        ? def.operatingDirectory : this.operatingDirectory;
    if (schemaOpDir != null) {
      builder.operand("operatingDirectory", schemaOpDir);
    }

    // Apply factory's hooks
    def.factory.configureHooks(builder, def.operand);

    // Apply user overrides (if any)
    if (def.hookOverrides != null) {
      def.hookOverrides.accept(builder);
    }

    // Set auto-download based on factory preference
    builder.autoDownload(def.factory.shouldAutoDownload(def.operand));

    // Build the schema (triggers ETL if autoDownload is enabled)
    return builder.build(parentSchema, def.name);
  }

  /**
   * Builder for ModelLifecycleProcessor.
   */
  public static class Builder {
    private StorageProvider sourceStorage;
    private StorageProvider materializedStorage;
    private String operatingDirectory;
    private IncrementalTracker incrementalTracker;
    private final List<SchemaDefinition> schemas = new ArrayList<>();

    /**
     * Set the storage provider for raw/source data.
     *
     * <p>Used for downloading and caching source data before materialization.
     *
     * @param provider Storage provider for raw data (S3, local, etc.)
     */
    public Builder sourceStorage(StorageProvider provider) {
      this.sourceStorage = provider;
      return this;
    }

    /**
     * Set the storage provider for materialized data.
     *
     * <p>Used for writing Parquet/Iceberg output that will be queried.
     *
     * @param provider Storage provider for materialized data (S3, local, etc.)
     */
    public Builder materializedStorage(StorageProvider provider) {
      this.materializedStorage = provider;
      return this;
    }

    /**
     * Set the operating directory for locks and metadata.
     *
     * <p>This should always be a local filesystem path (not S3) because
     * file locking doesn't work on remote storage.
     *
     * @param dir Operating directory path (e.g., ".aperio/govdata")
     */
    public Builder operatingDirectory(String dir) {
      this.operatingDirectory = dir;
      return this;
    }

    /**
     * Set the incremental tracker for resumable ETL.
     *
     * <p>The tracker persists which dimension combinations have been processed,
     * allowing ETL to resume from where it left off after restarts. This is
     * critical for datasets that span decades of data.
     *
     * <p>If not set, defaults to {@link IncrementalTracker#NOOP} which
     * reprocesses all data on each run.
     *
     * @param tracker Incremental tracker for resumability
     */
    public Builder incrementalTracker(IncrementalTracker tracker) {
      this.incrementalTracker = tracker;
      return this;
    }

    /**
     * Add a schema using factory's default hooks.
     *
     * @param name Schema name (e.g., "ECON")
     * @param factory Sub-schema factory providing hooks
     * @param operand Configuration operand from model file
     */
    public Builder addSchema(String name, SubSchemaFactory factory,
        Map<String, Object> operand) {
      return addSchema(name, factory, operand, null);
    }

    /**
     * Add a schema with factory hooks plus user overrides.
     *
     * <p>The hook overrides are applied after the factory's hooks,
     * allowing users to customize or extend the factory behavior.
     *
     * <p>The current {@link #incrementalTracker} and {@link #operatingDirectory}
     * are captured at this point and stored with the schema definition.
     * This ensures each schema uses its own tracker for completion status,
     * which is critical when processing dependency schemas.
     *
     * @param name Schema name (e.g., "ECON")
     * @param factory Sub-schema factory providing hooks
     * @param operand Configuration operand from model file
     * @param hookOverrides Optional callback to override/extend hooks
     */
    public Builder addSchema(String name, SubSchemaFactory factory,
        Map<String, Object> operand, Consumer<FileSchemaBuilder> hookOverrides) {
      // Capture current tracker and operatingDirectory for this schema
      // This ensures each schema uses its own tracker, not a shared one
      schemas.add(new SchemaDefinition(name, factory,
          operand != null ? new HashMap<>(operand) : new HashMap<>(),
          hookOverrides,
          this.incrementalTracker,
          this.operatingDirectory));
      return this;
    }

    /**
     * Build the processor.
     */
    public ModelLifecycleProcessor build() {
      if (schemas.isEmpty()) {
        throw new IllegalStateException("At least one schema must be added");
      }
      return new ModelLifecycleProcessor(this);
    }
  }

  /**
   * Internal holder for schema definition.
   *
   * <p>Each schema has its own incremental tracker and operating directory
   * to ensure completion status is stored in the correct location.
   * This is critical when processing dependency schemas (e.g., ECON_REFERENCE)
   * before the main schema (e.g., ECON) - each needs its own tracker.
   */
  private static class SchemaDefinition {
    final String name;
    final SubSchemaFactory factory;
    final Map<String, Object> operand;
    final Consumer<FileSchemaBuilder> hookOverrides;
    final IncrementalTracker incrementalTracker;
    final String operatingDirectory;

    SchemaDefinition(String name, SubSchemaFactory factory,
        Map<String, Object> operand, Consumer<FileSchemaBuilder> hookOverrides,
        IncrementalTracker incrementalTracker, String operatingDirectory) {
      this.name = name;
      this.factory = factory;
      this.operand = operand;
      this.hookOverrides = hookOverrides;
      this.incrementalTracker = incrementalTracker;
      this.operatingDirectory = operatingDirectory;
    }
  }
}
