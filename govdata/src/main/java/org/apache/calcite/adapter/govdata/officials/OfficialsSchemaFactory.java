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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory for the Federal Officials schema.
 *
 * <p>Provides access to federally elected and federally appointed officeholders,
 * sourced exclusively from official U.S. government systems of record — no
 * community-maintained aggregators.
 *
 * <p>Tables:
 * <ul>
 *   <li>{@code members} — every House/Senate member, current + historical, from the
 *       Congress.gov API member endpoint. Requires {@code API_DATA_GOV}.</li>
 *   <li>{@code nominations} — PAS (Senate-confirmed) presidential nominations, per
 *       Congress, from the Congress.gov API nomination endpoint. Requires
 *       {@code API_DATA_GOV}.</li>
 *   <li>{@code federal_judges} — every Article III judge ever commissioned, from the
 *       Federal Judicial Center's biographical directory export. No key required.</li>
 *   <li>{@code electoral_college_votes} — Electoral College votes by state, President
 *       and VP, from NARA's per-election results pages. No key required.</li>
 *   <li>{@code presidential_election_results} — popular vote by state, from FEC's
 *       official post-election results workbook. No key required, but only covers
 *       years FEC published in this specific XLSX format (confirmed: 2024 only, as
 *       of when this table was built — see the table's schema comment).</li>
 * </ul>
 *
 * <p>All five response transformers are implemented and verified against live
 * downloads: {@code CongressMemberTransformer} and {@code NominationTransformer}
 * against a live keyed Congress.gov response, {@code FederalJudgesTransformer}
 * against a live FJC CSV download (2026-08-01); {@code ElectoralCollegeTransformer}
 * against live NARA pages spanning 1824-2024, and {@code PresidentialResultsTransformer}
 * against FEC's live 2024 discovery + workbook (2026-08-02).
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "OFFICIALS",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "officials",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class OfficialsSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(OfficialsSchemaFactory.class);

  @Override public String getSchemaResourceName() {
    return "/officials/officials-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureSchemaHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for OFFICIALS schema");
  }

  /**
   * Congress.gov's endpoints are Congress-scoped, not year-scoped — but the only demarc a
   * caller ever sets is {@code startYear}/{@code endYear} (the standard GOVDATA_START_YEAR/
   * END_YEAR operand), same as every other schema. Derived here, once, at the moment this
   * schema loads — the single place every invocation path (worker.sh, force-reprocess.sh, a
   * direct model) goes through — rather than relying on any caller to separately compute and
   * pass a Congress number.
   *
   * <p>Must run from {@link #deriveEarlyProperties}, not {@link #configureSchemaHooks}: the
   * latter is invoked by {@code ModelLifecycleProcessor} after this schema's YAML resource has
   * already been parsed and its {@code congress_range} dimension's
   * {@code ${GOVDATA_START_CONGRESS:117}} placeholder already resolved — a system property set
   * that late can never affect the current schema build (confirmed live: the property read back
   * correctly immediately after being set, while the dimension config logged moments later
   * still showed the literal 117-119 default).
   *
   * <p>Presidential-election-year tables need no equivalent: they're plain annual ranges on
   * GOVDATA_START_YEAR/END_YEAR directly, and a non-election year already resolves as an
   * expected zero-row gap at the source (electoral_college_votes' skipOn: [404];
   * presidential_election_results' transformer logging and returning zero rows for a year with
   * no matching link) — no dimension-level derivation needed.
   */
  @Override public void deriveEarlyProperties(Map<String, Object> operand) {
    Object startYearObj = operand.get("startYear");
    Object endYearObj = operand.get("endYear");
    if (startYearObj == null || endYearObj == null) {
      return;
    }
    int startYear = Integer.parseInt(String.valueOf(startYearObj));
    int endYear = Integer.parseInt(String.valueOf(endYearObj));

    // Congress N spans [1789+2(N-1), 1791+2(N-1)); year Y -> N = (Y-1789)/2 + 1.
    int startCongress = (startYear - 1789) / 2 + 1;
    int endCongress = (endYear - 1789) / 2 + 1;
    System.setProperty("GOVDATA_START_CONGRESS", String.valueOf(startCongress));
    System.setProperty("GOVDATA_END_CONGRESS", String.valueOf(endCongress));
  }
}
