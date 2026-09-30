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
package org.apache.calcite.jdbc;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.CompatibilityLookup;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/**
 * A schema that lists a name it cannot resolve (e.g. a deferred view whose on-demand creation
 * fails) must not abort {@link java.sql.DatabaseMetaData#getTables} for every schema.
 */
@Tag("unit")
class CalciteMetaImplUnresolvableTableTest {

  /** Lists "broken" but returns null for it, like a deferred view whose CREATE failed. */
  private static class LazySchema extends AbstractSchema {
    private final Map<String, Table> tables;

    LazySchema(String... names) {
      ImmutableMap.Builder<String, Table> b = ImmutableMap.builder();
      for (String n : names) {
        b.put(n, new OneColumnTable());
      }
      this.tables = b.build();
    }

    @Override public Lookup<Table> tables() {
      return new CompatibilityLookup<>(this::lookup, this::listedNames);
    }

    private @Nullable Table lookup(String name) {
      return tables.get(name);
    }

    private Set<String> listedNames() {
      Set<String> names = new LinkedHashSet<>(tables.keySet());
      names.add("broken");
      return names;
    }
  }

  /** Minimal table with one integer column. */
  private static class OneColumnTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("x", SqlTypeName.INTEGER).build();
    }
  }

  @Test void unresolvableListedTableIsSkippedAndOtherSchemasStillEnumerate() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:")) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      root.add("a_first", new LazySchema("t1"));
      root.add("z_last", new LazySchema("t2"));

      List<String> seen = new ArrayList<>();
      try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
        while (rs.next()) {
          String schema = rs.getString("TABLE_SCHEM");
          if (schema.equals("a_first") || schema.equals("z_last")) {
            seen.add(schema + "." + rs.getString("TABLE_NAME"));
          }
        }
      }
      assertThat(seen, contains("a_first.t1", "z_last.t2"));
    }
  }
}
