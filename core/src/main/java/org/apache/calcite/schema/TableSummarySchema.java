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
package org.apache.calcite.schema;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A schema that can report a table's type and comment without resolving the table.
 *
 * <p>Resolving a table can be expensive (object-store discovery, view creation); a metadata
 * listing such as {@code INFORMATION_SCHEMA.TABLES} only needs these two values, so it asks here
 * first and resolves the table only when the schema cannot answer.
 */
public interface TableSummarySchema {
  /**
   * The type and comment of {@code tableName}, or null when they cannot be known without
   * resolving the table itself.
   */
  @Nullable TableSummary tableSummary(String tableName);

  /** A table's type and comment. */
  final class TableSummary {
    public final Schema.TableType type;
    public final @Nullable String comment;

    public TableSummary(Schema.TableType type, @Nullable String comment) {
      this.type = type;
      this.comment = comment;
    }
  }
}
