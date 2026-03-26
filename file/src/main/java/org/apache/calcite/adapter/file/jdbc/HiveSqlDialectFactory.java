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
package org.apache.calcite.adapter.file.jdbc;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.dialect.HiveSqlDialect;

import java.sql.DatabaseMetaData;

/**
 * Factory that always returns {@link HiveSqlDialect}.
 *
 * <p>The Hive JDBC driver's {@link DatabaseMetaData} implementation throws
 * exceptions from several metadata methods (e.g. {@code nullsAreSortedAtEnd}),
 * which breaks Calcite's automatic dialect detection in
 * {@link org.apache.calcite.sql.SqlDialectFactoryImpl}. This factory bypasses
 * the auto-detection and returns the correct dialect directly.
 *
 * <p>Usage in a Calcite model JSON:
 * <pre>{@code
 * "sqlDialectFactory": "org.apache.calcite.adapter.file.jdbc.HiveSqlDialectFactory"
 * }</pre>
 */
public class HiveSqlDialectFactory implements SqlDialectFactory {

  @Override public SqlDialect create(DatabaseMetaData databaseMetaData) {
    return HiveSqlDialect.DEFAULT;
  }
}
