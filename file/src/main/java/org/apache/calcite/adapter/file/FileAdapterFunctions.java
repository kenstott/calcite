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
   * @param duckDbEngine whether the schema runs on the DuckDB execution engine; the
   *     DuckDB-native statistical aggregates are registered ONLY then, since they exist
   *     purely to push down to DuckDB and have no implementation on the other engines
   *     (parquet, arrow, linq4j)
   */
  public static void registerStandardFunctions(SchemaPlus schema, boolean duckDbEngine) {
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
    // registerStandardFunctions runs once per schema created; each family registers
    // once per root, keyed on its OWN marker function so they stay independent. The
    // similarity family registers unconditionally while the DuckDB stats register only
    // on the DuckDB engine — so they must NOT share a guard: otherwise a non-DuckDB (or
    // earlier) connection that primes similarity first permanently suppresses the stats
    // for a later DuckDB schema on the same root (e.g. a multi-schema connection opened
    // before a single-schema query).
    if (root.getFunctions("COSINE_SIMILARITY").isEmpty()) {
      try {
        SimilarityFunctions.registerFunctions(root);
      } catch (Exception e) {
        LOGGER.warn("Failed to register vector/semantic similarity functions: {}", e.getMessage());
      }
    }
    // DuckDB-native statistical aggregates that Calcite core lacks. DuckDB engine ONLY:
    // they are declarations that push down to DuckDB and have no implementation on the
    // other file-adapter engines, so registering them elsewhere would only offer a
    // function that always fails at execution.
    //
    // Registered under lowercase names: this project's standard connection settings
    // (Oracle lex, unquotedCasing=TO_LOWER) lowercase every unquoted identifier before
    // function-name lookup, and SchemaPlus function lookup is exact-match, not
    // case-insensitive. A name registered uppercase therefore never resolves for any
    // normal (unquoted) call, in any case the caller typed it — only an explicitly
    // quoted, exact-uppercase call like "JSON_EXTRACT"(...) would have matched, which
    // is not a usage pattern anyone writes. Lowercase matches what TO_LOWER normalizes
    // every unquoted call to, so STRING_SPLIT/string_split/String_Split all resolve
    // identically. DuckDBFunctionMapping's FUNCTION_MAP already upper-cases
    // operator.getName() before its own lookup, so it needs no change here.
    if (duckDbEngine && root.getFunctions("agg_corr").isEmpty()) {
      try {
        registerDuckDBStatsAggregates(root);
      } catch (Exception e) {
        LOGGER.warn("Failed to register duckdb stats functions: {}", e.getMessage());
      }
    }
    // DuckDB-native JSON_EXTRACT, which Calcite core does not define under that name
    // (the ANSI equivalent is JSON_VALUE). DuckDB engine ONLY, for the same reason as
    // the stats aggregates above: it has no implementation on the other engines.
    if (duckDbEngine && root.getFunctions("json_extract").isEmpty()) {
      try {
        root.add("json_extract", org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
            org.apache.calcite.adapter.file.duckdb.DuckDBJsonFunctions.class, "jsonExtract"));
      } catch (Exception e) {
        LOGGER.warn("Failed to register duckdb json functions: {}", e.getMessage());
      }
    }
    // DuckDB-native STRING_SPLIT, which Calcite core does not define. Same rationale as
    // JSON_EXTRACT above: DuckDB engine ONLY, no implementation on the other engines.
    if (duckDbEngine && root.getFunctions("string_split").isEmpty()) {
      try {
        root.add("string_split", org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
            org.apache.calcite.adapter.file.duckdb.DuckDBStringFunctions.class, "stringSplit"));
      } catch (Exception e) {
        LOGGER.warn("Failed to register duckdb string functions: {}", e.getMessage());
      }
    }
    // Spatial ST_* are provided by Calcite's built-in SPATIAL library — connect
    // with fun=...,spatial. No schema registration is needed (and reflective
    // registration would duplicate the library operators).
  }

  /**
   * Registers the non-reserved DuckDB statistical aggregates on the (root) schema so they
   * validate and push down to DuckDB. The reserved regression aggregates (corr, regr_*)
   * are handled separately via a pre-parse alias rewrite, since their names are reserved
   * parser keywords and cannot be registered under their real names here.
   */
  private static void registerDuckDBStatsAggregates(SchemaPlus root) {
    root.add("median", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.MedianUdaf.class));
    root.add("skewness", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.SkewnessUdaf.class));
    root.add("kurtosis", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.KurtosisUdaf.class));
    root.add("mad", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.MadUdaf.class));
    root.add("quantile_cont", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.QuantileContUdaf.class));
    root.add("quantile_disc", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.QuantileDiscUdaf.class));

    // Reserved regression aggregates: registered under non-reserved ALIAS names. A
    // pre-parse rewrite turns the user's corr(...) into agg_corr(...); the DuckDB dialect
    // (DuckDBFunctionMapping) maps agg_corr -> corr on unparse. Each gets its OWN class:
    // they share an accumulator but the operator name is the only thing distinguishing
    // corr from regr_slope, and a static UDAF cannot see the operator name, so one shared
    // class could not tell which statistic to return.
    root.add("agg_corr", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.CorrUdaf.class));
    root.add("agg_regr_slope", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.RegrSlopeUdaf.class));
    root.add("agg_regr_intercept", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.RegrInterceptUdaf.class));
    root.add("agg_regr_r2", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.RegrR2Udaf.class));
    root.add("agg_regr_avgx", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.RegrAvgXUdaf.class));
    root.add("agg_regr_avgy", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.RegrAvgYUdaf.class));
    root.add("agg_regr_sxy", org.apache.calcite.schema.impl.AggregateFunctionImpl.create(
        org.apache.calcite.adapter.file.duckdb.DuckDBStatsFunctions.RegrSxyUdaf.class));
  }
}
