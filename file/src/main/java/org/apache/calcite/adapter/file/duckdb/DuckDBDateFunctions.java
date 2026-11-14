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

import java.sql.Date;
import java.sql.Timestamp;

/**
 * Scalar-function DECLARATIONS for DuckDB's native date-difference functions, which Calcite
 * does not push down: its {@code TIMESTAMPDIFF} is expanded into interval arithmetic that has
 * no JDBC rendering, and {@code DATE - DATE} does not validate. Registering these makes the
 * calls validate; the DuckDB engine pushes them down and computes them.
 *
 * <ul>
 *   <li>{@code datediff(unit, start, end)}: DuckDB's boundary-counting difference.</li>
 *   <li>{@code duckdb_date_sub(unit, start, end)}: whole elapsed units, the semantics of
 *       {@code TIMESTAMPDIFF}; {@link DuckDBSqlRewriter} rewrites {@code TIMESTAMPDIFF} to it
 *       and {@link DuckDBFunctionMapping} renders it as DuckDB's {@code date_sub}.</li>
 * </ul>
 *
 * <p>The Java bodies are never meant to run: they throw, so a query that fails to push down
 * surfaces loudly instead of returning a wrong value.
 */
public final class DuckDBDateFunctions {

  private DuckDBDateFunctions() {
  }

  /** {@code datediff(unit, start, end)} over dates. */
  public static Long datediff(String unit, Date start, Date end) {
    throw notPushedDown("datediff");
  }

  /** {@code datediff(unit, start, end)} over timestamps. */
  public static Long datediff(String unit, Timestamp start, Timestamp end) {
    throw notPushedDown("datediff");
  }

  /** {@code date_sub(unit, start, end)} over dates. */
  public static Long duckdbDateSub(String unit, Date start, Date end) {
    throw notPushedDown("date_sub");
  }

  /** {@code date_sub(unit, start, end)} over timestamps. */
  public static Long duckdbDateSub(String unit, Timestamp start, Timestamp end) {
    throw notPushedDown("date_sub");
  }

  private static UnsupportedOperationException notPushedDown(String name) {
    return new UnsupportedOperationException(
        name + " is a DuckDB-only function and must be pushed down to the DuckDB engine; "
        + "it has no Calcite enumerable implementation.");
  }
}
