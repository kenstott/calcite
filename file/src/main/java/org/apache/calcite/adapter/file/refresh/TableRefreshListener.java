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
package org.apache.calcite.adapter.file.refresh;

import java.io.File;

/**
 * Listener interface for table refresh events.
 * Implementations can react to table refreshes, such as updating external systems.
 */
public interface TableRefreshListener {
  /**
   * Called when a table has been refreshed.
   *
   * @param tableName the name of the table that was refreshed
   * @param parquetFile the updated parquet file
   */
  void onTableRefreshed(String tableName, File parquetFile);
}
