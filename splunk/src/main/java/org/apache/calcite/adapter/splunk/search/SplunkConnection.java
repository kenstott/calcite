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

import org.apache.calcite.linq4j.Enumerator;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for connections to Splunk that can execute searches and return results.
 * Supports both streaming result processing via SearchResultListener and
 * enumerable result processing via Enumerator.
 */
public interface SplunkConnection {

  /**
   * Executes a Splunk search and streams results to a listener.
   * This method is suitable for processing large result sets without
   * loading everything into memory at once.
   *
   * @param search the Splunk search query
   * @param otherArgs additional search parameters (earliest, latest, etc.)
   * @param fieldList list of fields to include in results (null for all)
   * @param listener callback interface to receive results
   */
  void getSearchResults(String search, Map<String, String> otherArgs,
                       List<String> fieldList, SearchResultListener listener);

  /**
   * Executes a Splunk search and returns an Enumerator for result processing.
   * This method provides Calcite-compatible result iteration with support
   * for explicit field mapping and overflow data handling.
   *
   * @param search the Splunk search query
   * @param otherArgs additional search parameters (earliest, latest, etc.)
   * @param fieldList list of fields to include in results (null for all)
   * @param explicitFields set of explicitly defined fields for schema mapping
   * @return Enumerator for iterating over search results
   */
  Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields);

  /**
   * Executes a Splunk search and returns an Enumerator for result processing with field mapping.
   * This method provides Calcite-compatible result iteration with support for bidirectional
   * field mapping between schema field names and Splunk field names.
   *
   * @param search the Splunk search query
   * @param otherArgs additional search parameters (earliest, latest, etc.)
   * @param fieldList list of Splunk field names to include in results
   * @param explicitFields set of explicitly defined schema fields for overflow mapping
   * @param reverseFieldMapping map from Splunk field names to schema field names
   * @return Enumerator for iterating over search results with schema field names
   */
  Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields,
      Map<String, String> reverseFieldMapping);
}
