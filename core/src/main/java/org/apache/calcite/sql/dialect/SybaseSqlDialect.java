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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlDialect</code> implementation for the Sybase database.
 */
public class SybaseSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SYBASE);

  public static final SqlDialect DEFAULT = new SybaseSqlDialect(DEFAULT_CONTEXT);

  /** Creates a SybaseSqlDialect. */
  public SybaseSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    writer.keyword("rows limit");
    requireNonNull(fetch, "fetch");
    fetch.unparse(writer, -1, -1);

    if (offset != null) {
      writer.keyword("offset");
      offset.unparse(writer, -1, -1);
    }
  }

  @Override public void unparseTopN(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    // No-op; see unparseTopN.
  }
}
