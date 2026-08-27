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
package org.apache.calcite.adapter.file.duckdb;

import java.util.List;

/**
 * Scalar-function DECLARATION for DuckDB's native {@code string_split}, which Calcite core
 * does not define. Registering this on the schema makes {@code STRING_SPLIT(str, delim)}
 * <em>validate</em> (Calcite infers an {@code ARRAY<VARCHAR>} return type from the
 * {@code List<String>} signature, so {@code UNNEST(STRING_SPLIT(...))} validates too); the
 * DuckDB engine then pushes the call down and DuckDB computes it. The Java body is never
 * meant to run in Calcite's enumerable layer — it throws, so a query that fails to push down
 * surfaces loudly instead of returning a wrong (Java-computed stub) value.
 */
public final class DuckDBStringFunctions {

  private DuckDBStringFunctions() {
  }

  /** {@code string_split(str, delimiter)}. */
  public static List<String> stringSplit(String str, String delimiter) {
    throw new UnsupportedOperationException(
        "string_split is a DuckDB-only function and must be pushed down to the DuckDB engine; "
        + "it has no Calcite enumerable implementation.");
  }
}
