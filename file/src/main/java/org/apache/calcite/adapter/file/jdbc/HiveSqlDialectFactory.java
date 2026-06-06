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
