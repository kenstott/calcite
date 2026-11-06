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

import java.util.Set;

/**
 * A schema whose source lists more tables than the schema exposes for query.
 *
 * <p>{@link org.apache.calcite.schema.impl.InformationSchema} lists every table such a schema
 * names here, exposed or not: {@code information_schema.tables} from the names alone, and
 * {@code information_schema.columns} from {@link #listedTable}, asked only for the tables the
 * statement's predicates leave. A listed table is never queryable through this interface.
 */
public interface ListingSchema {

  /**
   * Returns the name of every table the source lists, the exposed ones among them. Reads no
   * column metadata.
   */
  Set<String> listedTableNames();

  /**
   * Returns the listed table {@code name}, for its row type and comments, or null when the
   * source lists no such table. Called once per table a metadata query reaches.
   */
  @Nullable Table listedTable(String name);
}
