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

import org.apache.calcite.adapter.file.similarity.SimilarityFunctions;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard SQL UDFs that the file adapter exposes on every schema it builds,
 * regardless of execution engine.
 *
 * <p>These are intrinsic file-adapter features — not bound to any downstream
 * adapter (e.g. govdata). They register the function on the Calcite schema so it
 * <em>validates</em>; execution then runs in Calcite's enumerable layer, or, when
 * the name matches a DuckDB built-in (spatial {@code ST_*}, {@code array_cosine_*}),
 * passes through to DuckDB at pushdown via the dialect's function mapping.
 *
 * <ul>
 *   <li><b>Vector similarity</b> — {@code COSINE_SIMILARITY}, {@code EUCLIDEAN_DISTANCE},
 *       {@code DOT_PRODUCT}, … (from {@link SimilarityFunctions}).</li>
 *   <li><b>Spatial</b> — {@code ST_*} (from calcite-core's {@link SpatialTypeFunctions});
 *       the names match DuckDB's spatial extension so they pass through unchanged.</li>
 * </ul>
 */
public final class FileAdapterFunctions {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileAdapterFunctions.class);

  private FileAdapterFunctions() {
  }

  /**
   * Registers the standard vector-similarity and spatial UDFs on the given schema.
   * Each family is registered independently and defensively, so a failure in one
   * never prevents the schema from being created or the other family from registering.
   *
   * @param schema schema to register the functions on (no-op if null)
   */
  public static void registerStandardFunctions(SchemaPlus schema) {
    if (schema == null) {
      return;
    }
    // Functions must live on a MUTABLE schema. The custom adapter schemas
    // (DuckDB-backed, iceberg) are immutable — adding a function to them is
    // silently lost (or rejected as "not mutable"). Walk to the root schema,
    // which is mutable and whose functions resolve for unqualified calls from
    // any default schema.
    SchemaPlus root = schema;
    while (root.getParentSchema() != null) {
      root = root.getParentSchema();
    }
    // registerStandardFunctions runs once per schema created; register only once
    // per root.
    if (!root.getFunctions("COSINE_SIMILARITY").isEmpty()) {
      return;
    }
    try {
      SimilarityFunctions.registerFunctions(root);
    } catch (Exception e) {
      LOGGER.warn("Failed to register vector/semantic similarity functions: {}", e.getMessage());
    }
    // Spatial ST_* are provided by Calcite's built-in SPATIAL library — connect
    // with fun=...,spatial. No schema registration is needed (and reflective
    // registration would duplicate the library operators).
  }
}
