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
package org.apache.calcite.adapter.splunk.search;

/**
 * Called each time a search returns a record.
 */
public interface SearchResultListener {
  /**
   * Handles a record from a search result.
   *
   * @param fieldValues Values of the record
   * @return true to continue parsing, false otherwise
   */
  boolean processSearchResult(String[] fieldValues);

  void setFieldNames(String[] fieldNames);
}
