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
package org.apache.calcite.adapter.file.refresh;

/**
 * Extended refresh listener that supports pattern-based refresh notifications.
 * Used by DuckDB+Hive optimization where the pattern is sufficient for view recreation
 * without requiring explicit file lists.
 */
public interface PatternAwareRefreshListener extends TableRefreshListener {
  /**
   * Called when a table has been refreshed with a file pattern.
   * This is used for DuckDB views where the pattern can be used directly
   * in the DDL (e.g., parquet_scan('s3://bucket/table/**\/*.parquet')).
   *
   * @param tableName the name of the table that was refreshed
   * @param pattern the file pattern (glob) for the table
   */
  void onTableRefreshedWithPattern(String tableName, String pattern);
}
