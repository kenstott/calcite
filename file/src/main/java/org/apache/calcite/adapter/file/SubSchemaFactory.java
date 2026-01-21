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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory interface for configuring sub-schema ETL hooks.
 *
 * <p>Implementations provide schema-specific configuration for the ETL pipeline,
 * including dimension resolvers, table enablement hooks, and other customizations.
 *
 * <p>Used by {@link ModelLifecycleProcessor} to configure each schema before
 * running ETL and creating the final Calcite schema.
 *
 * <p>Example implementation:
 * <pre>{@code
 * public class EconSchemaFactory implements SubSchemaFactory {
 *
 *   @Override
 *   public String getSchemaResourceName() {
 *     return "/econ/econ-schema.yaml";
 *   }
 *
 *   @Override
 *   public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
 *     builder.resolveDimensions("world_indicators", (ctx, dims) ->
 *         resolveWorldBankDimensions(ctx, dims));
 *
 *     builder.isEnabled("employment_statistics", ctx ->
 *         hasApiKey("BLS_API_KEY"));
 *   }
 * }
 * }</pre>
 *
 * @see ModelLifecycleProcessor
 * @see FileSchemaBuilder
 */
public interface SubSchemaFactory {

  /**
   * Returns the schema YAML resource path.
   *
   * <p>The resource is loaded from the classpath and defines:
   * <ul>
   *   <li>Table definitions (name, source, columns, dimensions)</li>
   *   <li>Materialization configuration (format, partitioning)</li>
   *   <li>Foreign key constraints</li>
   * </ul>
   *
   * @return Schema resource path (e.g., "/econ/econ-schema.yaml")
   */
  String getSchemaResourceName();

  /**
   * Configure schema-specific hooks on the builder.
   *
   * <p>Called by {@link ModelLifecycleProcessor} before running ETL.
   * Implementations should register any hooks needed for this schema:
   * <ul>
   *   <li>{@link FileSchemaBuilder#resolveDimensions} - Dynamic dimension resolution</li>
   *   <li>{@link FileSchemaBuilder#isEnabled} - Conditional table enablement</li>
   *   <li>{@link FileSchemaBuilder#beforeSource} - Pre-fetch hooks</li>
   *   <li>{@link FileSchemaBuilder#beforeMaterialize} - Pre-write hooks</li>
   * </ul>
   *
   * @param builder The schema builder to configure
   * @param operand Configuration operand from model file
   */
  void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand);

  /**
   * Check if auto-download (ETL) should be enabled.
   *
   * <p>When enabled, ETL runs automatically when the schema is created.
   * When disabled, requires explicit call to {@link FileSchemaBuilder#runEtl()}.
   *
   * @param operand Configuration map
   * @return true if auto-download is enabled (default: true)
   */
  default boolean shouldAutoDownload(Map<String, Object> operand) {
    Object autoDownload = operand.get("autoDownload");
    if (autoDownload instanceof Boolean) {
      return (Boolean) autoDownload;
    }
    if (autoDownload instanceof String) {
      return Boolean.parseBoolean((String) autoDownload);
    }
    return true;
  }

  /**
   * Returns the list of data source names this schema depends on.
   *
   * <p>Dependencies are processed before this schema's ETL runs.
   * For example, the 'econ' schema depends on 'econ_reference' because
   * it needs dimension lookup tables populated first.
   *
   * <p>Dependency names should match the canonical data source names
   * used by the parent schema factory (e.g., "econ_reference", "geo").
   *
   * @return List of dependency data source names, empty if none
   */
  default List<String> getDependencies() {
    return Collections.emptyList();
  }
}
