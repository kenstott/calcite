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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.CongressRange;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory for the law schema: the home for the legal corpus, one table per source.
 *
 * <p>Today it provides the current text of the U.S. Code (codified federal statutes) from the
 * Office of the Law Revision Counsel's USLM XML release points at uscode.house.gov, and the
 * status of every bill and resolution introduced in Congress from GovInfo's Bill Status bulk
 * data. No API key is required.
 *
 * <p>Tables:
 * <ul>
 *   <li>{@code usc_sections} — one row per section of the Code, with the operative text in
 *       {@code section_text} (chunked and embedded into {@code ref.vectorized_chunks} by
 *       ChunkOrganizer) and the enactment history in {@code source_credit}.</li>
 *   <li>{@code bills} and its child tables ({@code bill_actions}, {@code bill_cosponsors},
 *       {@code bill_committees}, {@code bill_committee_activities}, {@code bill_subjects},
 *       {@code bill_text_versions}, {@code bill_related_bills}, {@code bill_amendments},
 *       {@code bill_amendment_actions}, {@code bill_amendment_cosponsors},
 *       {@code bill_action_committees}, {@code bill_recorded_votes}, {@code bill_titles},
 *       {@code bill_summaries}, {@code bill_cbo_cost_estimates},
 *       {@code bill_committee_reports}, {@code bill_notes}) — one XML file per bill, read by
 *       {@link CongressBillStatusProvider}, fanned out by Congress and bill type.</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "LAW",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "law",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class LawSchemaFactory implements GovDataSubSchemaFactory {

  @Override public String getSchemaResourceName() {
    return "/law/law-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureSchemaHooks(FileSchemaBuilder builder,
      Map<String, Object> operand) {
    // No per-table gating: every table is enabled unless the operand's enabledTables narrows it.
  }

  /** The bill tables iterate Congresses, derived here from the standard startYear/endYear. */
  @Override public void deriveEarlyProperties(Map<String, Object> operand) {
    CongressRange.deriveEarlyProperties(operand);
  }
}
