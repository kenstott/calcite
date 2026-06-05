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

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the Patents schema.
 *
 * <p>Provides USPTO patent grant and trademark application data from:
 * <ul>
 *   <li>PatentsView bulk TSV downloads (S3: data.patentsview.org)</li>
 *   <li>USPTO trademark bulk CSV (bulkdata.uspto.gov)</li>
 * </ul>
 *
 * <p>Patent tables are year-partitioned by grant_year. Trademark tables are
 * year-partitioned by application_year. Full-dump files are cached locally
 * under GOVDATA_CACHE_DIR/patents/ with a quarterly TTL.
 *
 * <p>Run cadence: quarterly. PatentsView releases updated bulk files ~6 weeks
 * after each quarter end (months 3, 6, 9, 12). Worker 81 (daily) gates on
 * {@code within_release_window "patent" "3,6,9,12"}. Worker 80 (historical)
 * skips the release-window check and backfills 2010 through
 * GOVDATA_INCREMENTAL_START_YEAR - 1.
 */
public class PatentsSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PatentsSchemaFactory.class);

  private static final List<String> ALL_TABLES = Arrays.asList(
      "patent_grants", "patent_assignees", "patent_inventors",
      "patent_cpc_classes", "patent_claims", "patent_summaries",
      "trademark_applications",
      // Faithful append-only snapshot tables (loaded once in the snapshot pass).
      "patent_abstracts", "patent_applications", "patent_figures",
      "patent_locations");

  @Override public String getSchemaResourceName() {
    return "/patents/patents-schema.yaml";
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for PATENTS schema");
    @SuppressWarnings("unchecked")
    List<String> enabledTablesList = (List<String>) operand.get("enabledTables");
    if (enabledTablesList == null || enabledTablesList.isEmpty()) {
      LOGGER.debug("enabledTables not set — all patent tables enabled");
      return;
    }
    final Set<String> enabled = new HashSet<>(enabledTablesList);
    LOGGER.debug("Patents enabledTables filter: {}", enabled);
    for (final String tableName : ALL_TABLES) {
      builder.isEnabled(tableName, ctx -> enabled.contains(tableName));
    }
  }
}
