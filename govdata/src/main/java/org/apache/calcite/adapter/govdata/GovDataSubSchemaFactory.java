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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.file.SubSchemaFactory;

import java.util.Map;

/**
 * Interface for government data sub-schema factories.
 *
 * <p>Extends {@link SubSchemaFactory} to provide govdata-specific functionality.
 * Sub-schema factories implement this interface to configure ETL hooks for
 * their specific domain (econ, geo, sec, census, etc.).
 *
 * <p>Example implementation:
 * <pre>{@code
 * public class EconSchemaFactory implements GovDataSubSchemaFactory {
 *
 *   @Override *   public String getSchemaResourceName() {
 *     return "/econ/econ-schema.yaml";
 *   }
 *
 *   @Override *   public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
 *     builder.resolveDimensions("world_indicators", (ctx, dims) ->
 *         resolveWorldBankDimensions(ctx, dims));
 *
 *     for (String tableName : BLS_TABLES) {
 *       builder.isEnabled(tableName, ctx -> isSourceEnabled(operand, "bls"));
 *     }
 *   }
 * }
 * }</pre>
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
   * before running ETL. Implementations should register any hooks needed:
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
  @Override void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand);
}
