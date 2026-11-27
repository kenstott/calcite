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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Resolves the {@code issue_date} and {@code issue_file} dimensions of patent_grants_weekly from
 * the USPTO PTBLXML product catalog: one {@code issue_date} per weekly issue published in the
 * year, newest first, and for each the file name of its highest revision. A new week or a
 * re-issued file is therefore a new combination, which the incremental tracker has not seen.
 */
public class PatentWeeklyIssueResolver implements DimensionResolver {

  private final PatentGrantsWeeklyTransformer catalog = new PatentGrantsWeeklyTransformer();

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    String year = context.get("year");
    if (year == null) {
      throw new IllegalStateException(
          "Dimension " + dimensionName + " requires year to be declared before it");
    }
    SortedMap<String, String> issues = catalog.issuesForYear(year);
    if ("issue_date".equals(dimensionName)) {
      List<String> dates = new ArrayList<String>(issues.keySet());
      Collections.reverse(dates);
      return dates;
    }
    if ("issue_file".equals(dimensionName)) {
      String file = issues.get(context.get("issue_date"));
      if (file == null) {
        throw new IllegalStateException(
            "No PTBLXML file for issue date " + context.get("issue_date"));
      }
      return new ArrayList<String>(Collections.singletonList(file));
    }
    throw new IllegalArgumentException("Unknown dimension: " + dimensionName);
  }
}
