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

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.file.SubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for government data sub-schema factories.
 *
 * <p>Extends {@link SubSchemaFactory} to provide govdata-specific functionality.
 * Sub-schema factories implement this interface to configure ETL hooks for
 * their specific domain (econ, geo, sec, census, etc.).
 *
 * <p>Example implementation:
 * <pre>
 * public class EconSchemaFactory implements GovDataSubSchemaFactory {
 *
 *   public String getSchemaResourceName() {
 *     return "/econ/econ-schema.yaml";
 *   }
 *
 *   public void configureHooks(FileSchemaBuilder builder, Map&lt;String, Object&gt; operand) {
 *     builder.resolveDimensions("world_indicators", (ctx, dims) -&gt;
 *         resolveWorldBankDimensions(ctx, dims));
 *
 *     for (String tableName : BLS_TABLES) {
 *       builder.isEnabled(tableName, ctx -&gt; isSourceEnabled(operand, "bls"));
 *     }
 *   }
 * }
 * </pre>
 *
 * @see SubSchemaFactory
 * @see org.apache.calcite.adapter.file.ModelLifecycleProcessor
 */
public interface GovDataSubSchemaFactory extends SubSchemaFactory {

  /**
   * Build operand configuration for this sub-schema.
   *
   * <p>Note: For new schemas, prefer using {@link #configureHooks(FileSchemaBuilder, Map)}
   * with ModelLifecycleProcessor which manages storage providers centrally.
   *
   * @param operand Base operand configuration from model file
   * @param parent Parent factory (for shared services if needed)
   * @return Enriched operand after ETL execution
   */
  default Map<String, Object> buildOperand(Map<String, Object> operand, GovDataSchemaFactory parent) {
    // Default implementation for backward compatibility
    // Creates a FileSchemaBuilder, applies hooks, and returns operand
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaResource(getSchemaResourceName())
        .operand(operand);

    // Share storage providers from parent
    if (parent.getStorageProvider() != null) {
      builder.storageProvider(parent.getStorageProvider());
    }
    if (parent.getCacheStorageProvider() != null) {
      builder.cacheStorageProvider(parent.getCacheStorageProvider());
    }

    // Apply hooks
    configureHooks(builder, operand);

    // Run ETL and return operand
    return builder.autoDownload(shouldAutoDownload(operand)).getOperand();
  }

  /**
   * Configure schema-specific hooks on the builder.
   *
   * <p>Called by {@link org.apache.calcite.adapter.file.ModelLifecycleProcessor}
   * before running ETL, via the {@link #configureHooks(FileSchemaBuilder, Map)} default
   * below, which layers the generic {@code enabledTables} gate ({@link #applyEnabledTablesFilter})
   * on top after this method registers whatever schema-specific hooks it needs:
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
  void configureSchemaHooks(FileSchemaBuilder builder, Map<String, Object> operand);

  /**
   * Derives and sets any system properties this schema's own YAML resource needs resolved
   * during {@code dimension_values}/dimension-config parsing.
   *
   * <p>Called by {@link GovDataSchemaFactory#create} immediately after
   * {@code setCrossSchemaProperties}, before the schema's YAML resource is loaded and its
   * {@code ${VAR:default}} placeholders are resolved into {@link
   * org.apache.calcite.adapter.file.etl.DimensionConfig} objects. {@link #configureSchemaHooks}
   * runs too late for this: it is invoked by {@code ModelLifecycleProcessor} well after
   * dimension parsing has already resolved and cached those placeholders — a system property
   * set there can never affect the current schema build, only (accidentally) a later one within
   * the same JVM. Default no-op; override only when a schema needs to derive one operand value
   * (e.g. a Congress number) into another before dimension resolution, the way
   * {@code OfficialsSchemaFactory} derives {@code GOVDATA_START_CONGRESS}/
   * {@code GOVDATA_END_CONGRESS} from the standard {@code startYear}/{@code endYear} operand.
   *
   * @param operand Configuration operand from model file
   */
  default void deriveEarlyProperties(Map<String, Object> operand) {
    // No-op by default.
  }

  /**
   * Registers this schema's own hooks ({@link #configureSchemaHooks}), then layers a generic
   * {@code enabledTables} gate on top for every table the schema's YAML declares
   * ({@code partitionedTables} + {@code tables}).
   *
   * <p>{@code enabledTables} is an optional operand — a list of table names — that scopes a run
   * to exactly those tables, e.g. for a targeted DQ/backfill run against one or two new tables
   * instead of the whole schema. Absent or empty means no filtering (every table runs, as
   * before). Because {@link FileSchemaBuilder#isEnabled} now AND-composes multiple predicates
   * for the same table rather than overwriting, this applies uniformly whether or not the
   * schema already has its own bespoke filter (e.g. econ's {@code blsConfig}, fiscal's
   * {@code enabledSources}) — both must agree for a table to run.
   *
   * <p>Implementations should not override this method; override {@link #configureSchemaHooks}
   * instead. This method is intentionally not {@code default} so every implementor is forced to
   * go through {@link #configureSchemaHooks} — a factory that still declares its own
   * {@code configureHooks} would silently skip the generic gate.
   */
  @Override default void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    configureSchemaHooks(builder, operand);
    applyEnabledTablesFilter(builder, operand);
  }

  /**
   * Applies the generic {@code enabledTables} operand (a list of table names) as an
   * AND-composed {@link FileSchemaBuilder#isEnabled} predicate on every table this schema's
   * YAML declares. No-op when the operand is absent or empty.
   */
  default void applyEnabledTablesFilter(FileSchemaBuilder builder, Map<String, Object> operand) {
    Object enabledTablesObj = operand.get("enabledTables");
    if (!(enabledTablesObj instanceof List) || ((List<?>) enabledTablesObj).isEmpty()) {
      return;
    }
    Set<String> enabledTables = new HashSet<>();
    for (Object o : (List<?>) enabledTablesObj) {
      if (o != null) {
        enabledTables.add(String.valueOf(o));
      }
    }
    Logger logger = LoggerFactory.getLogger(getClass());
    List<Map<String, Object>> tableDefs =
        GovDataUtils.loadTableDefinitions(getClass(), getSchemaResourceName());
    int gated = 0;
    for (Map<String, Object> tableDef : tableDefs) {
      Object nameObj = tableDef.get("name");
      if (nameObj == null) {
        continue;
      }
      String tableName = String.valueOf(nameObj);
      builder.isEnabled(tableName, ctx -> enabledTables.contains(tableName));
      gated++;
    }
    logger.info("enabledTables filter: {} of {} — gated {} tables from {}",
        enabledTables, tableDefs.size(), gated, getSchemaResourceName());
  }
}
