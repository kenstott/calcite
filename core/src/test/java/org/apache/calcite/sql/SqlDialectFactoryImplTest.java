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
package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.dialect.TrinoSqlDialect;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link SqlDialectFactoryImpl}.
 */
class SqlDialectFactoryImplTest {
  /** Returns database metadata that reports the given product name, a
   * double-quote identifier quote, nulls sorted last, and no other
   * capabilities. */
  private static DatabaseMetaData metaData(String productName) {
    return (DatabaseMetaData) Proxy.newProxyInstance(
        DatabaseMetaData.class.getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class},
        (proxy, method, args) -> {
          switch (method.getName()) {
          case "getDatabaseProductName":
            return productName;
          case "getDatabaseProductVersion":
            return "1";
          case "getIdentifierQuoteString":
            return "\"";
          case "nullsAreSortedAtEnd":
            return true;
          case "getDatabaseMajorVersion":
          case "getDatabaseMinorVersion":
            return 1;
          default:
            if (method.getReturnType() == boolean.class) {
              return false;
            }
            throw new UnsupportedOperationException(method.getName());
          }
        });
  }

  /** A database reporting the product name "Trino" gets the Trino dialect,
   * not the Presto one (REQ-2256). */
  @Test void testTrinoProductNameGetsTrinoDialect() {
    final SqlDialect dialect =
        SqlDialectFactoryImpl.INSTANCE.create(metaData("Trino"));
    assertThat(dialect, instanceOf(TrinoSqlDialect.class));
  }

  @Test void testPrestoProductNameGetsPrestoDialect() {
    final SqlDialect dialect =
        SqlDialectFactoryImpl.INSTANCE.create(metaData("Presto"));
    assertThat(dialect, instanceOf(PrestoSqlDialect.class));
    assertThat(dialect, not(instanceOf(TrinoSqlDialect.class)));
  }
}
