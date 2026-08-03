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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.EtlResult;
import org.apache.calcite.adapter.file.etl.MaterializationWriter;
import org.apache.calcite.adapter.file.etl.MaterializationWriterFactory;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.etl.TableContext;
import org.apache.calcite.adapter.file.etl.TableLifecycleListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the cross-schema entity-resolution bridge described in {@code entity-resolution-plan.md}
 * (repo root): links free-text org and individual names across 9 AskAmerica schemas to a GLEIF
 * LEI / SEC EIN hub (orgs) or to each other (individuals).
 *
 * <p>Wired via {@code hooks.tableLifecycleListener} on {@code ref.entity_org_bridge} only
 * (arbitrarily — none of the four tables this class writes has a {@code source:} block, so
 * {@code afterTable} is the only lifecycle hook that fires for any of them; see
 * {@code SchemaLifecycleProcessor.process()}). On that one table's {@link #afterTable} it opens
 * its own DuckDB connection, reads every registry source directly via {@code iceberg_scan(...)},
 * and writes all four tables ({@code entity_org_bridge}, {@code entity_person_bridge},
 * {@code canonical_org_entity}, {@code canonical_person_entity}) in a single pass.
 *
 * <p>The matching pipeline is generic — every org-type source runs the same
 * EIN-exact / name-exact / blocked-fuzzy pipeline; every person-type pair runs the same
 * exact-last-name-block / fuzzy-first-name pipeline. Adding a 9th source is a new
 * {@link OrgSource}/{@link PersonSource} registry entry, not new match logic.
 */
public class EntityBridgeListener implements TableLifecycleListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityBridgeListener.class);

  /** The one table this listener is wired to in ref-schema.yaml; all others computed here too. */
  private static final String TRIGGER_TABLE = "entity_org_bridge";

  // 0.95, not the plan's original starting guess of 0.92 — hand-adjudicated against 60 real
  // org-track candidate pairs sampled live (15 per 0.03-wide band from 0.85 to 0.99, after the
  // remainder-scoring/recursive-suffix/person-routing fixes above): precision was ~60% at
  // 0.95-0.99 vs ~47% at 0.92-0.95, a real jump, not noise (e.g. 0.92-0.95 still included false
  // matches like "JAMES B WILLIAMSON"/"JAMES C WILLIAMSON TRUST" and "KAMALDEEP SINGH BHULLAR"/
  // "AMANDEEP SINGH BHULLAR" scoring above true matches like "Y INVEST INC"/"Y Investment Ltd").
  // This only affects the org-track (runOrgSource) — the person-track's matchPersonPair never
  // assigns 'high' from a fuzzy match at all, by design (see its own comment), so it only ever
  // reads FUZZY_LOW_THRESHOLD.
  private static final double FUZZY_HIGH_THRESHOLD = 0.95;
  // Left at the plan's original 0.85 — this band measured ~13-33% precision even after all
  // fixes, but it's meant to be exactly that weak and always visibly flagged 'low', never
  // silently trusted; sharpening it further trades away real matches for a tier that downstream
  // consumers should already treat with suspicion regardless of its precise precision number.
  private static final double FUZZY_LOW_THRESHOLD = 0.85;

  /**
   * Cap on GLEIF-side block size for the org-track fuzzy pass, empirically derived by querying
   * the live {@code ref.gleif_entities}/{@code transport.fmcsa_carriers} Iceberg tables directly
   * (not estimated): {@code fmcsa_carriers} alone (4,469,030 rows; {@code dot_number} is already
   * fully unique, so the per-source GROUP BY dedup in {@link #runOrgSource} doesn't shrink it at
   * all) would need ~2.37 <b>billion</b> {@code jaro_winkler_similarity()} candidate pairs with
   * blocking alone — of which 79% (1.88B) comes from just the 14% of carriers (630K rows) whose
   * first-token block has 1,000+ GLEIF members (generic words like "global"/"trustee"/"new", bare
   * initials). A block that large was never going to yield a trustworthy fuzzy match anyway.
   * Capping at 500 cuts the total to ~300M pairs (87% reduction) while skipping the fuzzy attempt
   * for only 20% of carriers (903K rows) — logged per-source below as "skipped: block too large"
   * rather than silently dropped. Applies uniformly to every org-type source (not fmcsa-specific),
   * consistent with this class's generic, table-driven design; every other registry source is
   * orders of magnitude smaller than fmcsa_carriers and rarely hits this cap in practice.
   */
  private static final int MAX_FUZZY_BLOCK_SIZE = 500;

  /** Legal-suffix/entity-marker tokens used by the sec.insider_transactions row classifier. */
  private static final String SEC_ENTITY_MARKER_REGEX =
      "\\b(LLC|LP|LLLP|INC|CORP|HOLDINGS|PARTNERS|FUND|TRUST|N A|CO|LTD|PARTNERSHIP|MANAGEMENT|GROUP)\\b";

  private static final String SEC_ORG_FILTER =
      "(regexp_matches(upper(s.reporting_person_name), '" + SEC_ENTITY_MARKER_REGEX + "') "
      + "OR (s.reporting_person_name = upper(s.reporting_person_name) "
      + "AND (length(s.reporting_person_name) "
      + "- length(replace(s.reporting_person_name, ' ', '')) + 1) "
      + "NOT BETWEEN 2 AND 4))";

  private static final String SEC_PERSON_FILTER = "NOT " + SEC_ORG_FILTER;

  /** FAA registrant-type codes, decoded exactly as the faa_aircraft_registry Calcite view does. */
  private static final String FAA_REGISTRANT_TYPE_EXPR =
      "CASE s.type_registrant "
      + "WHEN '1' THEN 'Individual' WHEN '2' THEN 'Partnership' WHEN '3' THEN 'Corporation' "
      + "WHEN '4' THEN 'Co-Owned' WHEN '5' THEN 'Government' WHEN '7' THEN 'LLC' "
      + "WHEN '8' THEN 'Non-Citizen Corporation' WHEN '9' THEN 'Non-Citizen Co-Owned' END";

  private static final String FAA_ORG_FILTER =
      "(" + FAA_REGISTRANT_TYPE_EXPR + ") IS NOT NULL "
      + "AND (" + FAA_REGISTRANT_TYPE_EXPR + ") <> 'Individual'";

  private static final String FAA_PERSON_FILTER =
      "(" + FAA_REGISTRANT_TYPE_EXPR + ") = 'Individual'";

  /** One row per org-type entry in entity-resolution-plan.md's "Org-type sources" table. */
  private static final List<OrgSource> ORG_SOURCES = Arrays.asList(
      new OrgSource(
          "fec", "committees", null,
          "s.connected_org_name", "s.committee_id", null,
          "connected_org_name", null, "s.year", "fec_committee_id"),
      new OrgSource(
          "patents", "patent_assignees", null,
          "s.assignee_organization", "s.assignee_id", null,
          "assignee_organization",
          "s.assignee_type IN ('US company','foreign company')", null,
          "patents_assignee_id"),
      new OrgSource(
          "sec", "insider_transactions", null,
          "s.reporting_person_name", "s.reporting_person_cik", null,
          "reporting_person_name", SEC_ORG_FILTER, null, "sec_reporting_person_cik"),
      new OrgSource(
          "health", "fda_drug_approvals", null,
          "s.sponsor_name", null, null,
          "sponsor_name", null, null, "fda_sponsor_name"),
      new OrgSource(
          "environment", "ghg_facilities", null,
          "s.parent_company", null, null,
          "parent_company", null, null, "ghg_parent_company_name"),
      new OrgSource(
          "health", "cms_open_payments", null,
          "s.paying_entity_name", null, null,
          "paying_entity_name", null, null, "cms_paying_entity_name"),
      new OrgSource(
          "energy", "eia_utility_annual", null,
          "s.utility_name", "CAST(s.utility_id AS VARCHAR)", null,
          "utility_name", null, "s.report_year", "eia_utility_id"),
      new OrgSource(
          "energy", "eia_coal_mines", null,
          "s.controller_name", null, null,
          "controller_name", null, null, "eia_coal_controller_name"),
      new OrgSource(
          "energy", "eia_coal_mines", null,
          "s.operator_name", null, null,
          "operator_name", null, null, "eia_coal_operator_name"),
      new OrgSource(
          "transport", "fmcsa_carriers", null,
          "s.carrier_name", "s.dot_number", null,
          "carrier_name", null, null, "fmcsa_dot_number"),
      // faa_aircraft_registry is a Calcite view (CASE over type_registrant); the physical
      // Iceberg table is faa_aircraft_master, so the classifier CASE is replicated here — see
      // entity-resolution-plan.md's "Mixed sources needing row-level classification".
      new OrgSource(
          "transport", "faa_aircraft_master", "faa_aircraft_registry",
          "s.registrant_name", null, null,
          "registrant_name", FAA_ORG_FILTER, null, "faa_registrant_name"),
      new OrgSource(
          "fiscal", "exempt_org_master", null,
          "s.org_name", "s.ein", "s.ein",
          "org_name", null, null, "exempt_org_ein"),
      new OrgSource(
          "fiscal", "sba_loan_approvals", null,
          "s.borrower_name", null, null,
          "borrower_name", null, null, "sba_borrower_name"),
      new OrgSource(
          "fiscal", "sba_loan_approvals", null,
          "s.lender_name", null, null,
          "lender_name", null, null, "sba_lender_name"));

  /** One row per person-type entry in entity-resolution-plan.md's "Person-type sources" table. */
  private static final List<PersonSource> PERSON_SOURCES = Arrays.asList(
      new PersonSource(
          "fec", "candidates", null,
          "s.candidate_id", "s.candidate_name",
          "split_part(s.candidate_name, ',', 1)",
          "list_element(string_split(trim(split_part(s.candidate_name, ',', 2)), ' '), 1)",
          "array_to_string(list_slice(string_split("
              + "trim(split_part(s.candidate_name, ',', 2)), ' '), 2, "
              + "len(string_split(trim(split_part(s.candidate_name, ',', 2)), ' '))), ' ')",
          null, "s.year", "fec_candidate_id"),
      new PersonSource(
          "sec", "insider_transactions", null,
          "s.reporting_person_cik", "s.reporting_person_name",
          "list_element(string_split(trim(s.reporting_person_name), ' '), 1)",
          "list_element(string_split(trim(s.reporting_person_name), ' '), 2)",
          "array_to_string(list_slice(string_split(trim(s.reporting_person_name), ' '), 3, "
              + "len(string_split(trim(s.reporting_person_name), ' '))), ' ')",
          SEC_PERSON_FILTER, null, "sec_reporting_person_cik"),
      new PersonSource(
          "health", "cms_open_payments", null,
          "s.physician_profile_id",
          "s.physician_first_name || ' ' || s.physician_last_name",
          "s.physician_last_name", "s.physician_first_name", null,
          null, "s.program_year", "cms_physician_profile_id"),
      // Raw name format not yet sampled (per entity-resolution-plan.md's Person track section)
      // — applying the same space-separated "LAST FIRST [MIDDLE]" heuristic as SEC as a
      // documented starting assumption; verify against a live sample and correct if wrong.
      new PersonSource(
          "transport", "faa_aircraft_master", "faa_aircraft_registry",
          null, "s.registrant_name",
          "list_element(string_split(trim(s.registrant_name), ' '), 1)",
          "list_element(string_split(trim(s.registrant_name), ' '), 2)",
          "array_to_string(list_slice(string_split(trim(s.registrant_name), ' '), 3, "
              + "len(string_split(trim(s.registrant_name), ' '))), ' ')",
          FAA_PERSON_FILTER, null, "faa_registrant_name"));

  @Override public void beforeTable(TableContext context) {
    // No-op: this listener does all its work in afterTable, once, on TRIGGER_TABLE.
  }

  @Override public boolean onTableError(TableContext context, Exception error) {
    LOGGER.error("EntityBridgeListener: table '{}' failed upstream of entity resolution",
        context.getTableName(), error);
    return true;
  }

  @Override public void afterTable(TableContext context, EtlResult result) {
    if (!TRIGGER_TABLE.equals(context.getTableName())) {
      return;
    }
    String runId = Instant.now().toString();
    LOGGER.info("EntityBridgeListener: starting entity-resolution build, runId={}", runId);
    try (Connection conn = openDuckDb(context)) {
      String base = context.getSchemaContext().getMaterializeDirectory();
      createMacros(conn);
      stageGleif(conn, base);
      stageEinHub(conn, base);
      execute(conn,
          "CREATE OR REPLACE TEMP TABLE all_org_mentions ("
          + "source_schema VARCHAR, source_table VARCHAR, source_column VARCHAR, source_key VARCHAR, "
          + "name_raw VARCHAR, name_norm VARCHAR, lei VARCHAR, sec_cik VARCHAR, gleif_legal_name VARCHAR, "
          + "match_method VARCHAR, match_confidence VARCHAR, match_score DOUBLE, "
          + "canonical_entity_id VARCHAR, canonical_column VARCHAR)");

      for (OrgSource src : ORG_SOURCES) {
        runOrgSource(conn, base, src);
      }

      List<Map<String, Object>> orgBridgeRows = queryRows(conn,
          "SELECT source_schema, source_table, source_column, source_key, "
          + "name_raw AS source_name_raw, name_norm AS source_name_normalized, lei, sec_cik, "
          + "gleif_legal_name, match_method, match_confidence, match_score, '" + esc(runId)
          + "' AS match_run_id FROM all_org_mentions WHERE match_method IS NOT NULL");
      List<Map<String, Object>> canonicalOrgRows = pivotOrg(conn);

      for (int i = 0; i < PERSON_SOURCES.size(); i++) {
        stagePersonSource(conn, base, i, PERSON_SOURCES.get(i));
      }
      List<Map<String, Object>> personBridgeRows = new ArrayList<Map<String, Object>>();
      for (int i = 0; i < PERSON_SOURCES.size(); i++) {
        for (int j = i + 1; j < PERSON_SOURCES.size(); j++) {
          personBridgeRows.addAll(
              matchPersonPair(conn, i, j, PERSON_SOURCES.get(i), PERSON_SOURCES.get(j), runId));
        }
      }
      List<Map<String, Object>> canonicalPersonRows = pivotPerson(conn, personBridgeRows);

      writeTable(context, "entity_org_bridge", orgBridgeRows);
      writeTable(context, "entity_person_bridge", personBridgeRows);
      writeTable(context, "canonical_org_entity", canonicalOrgRows);
      writeTable(context, "canonical_person_entity", canonicalPersonRows);

      LOGGER.info("EntityBridgeListener: complete — entity_org_bridge={}, entity_person_bridge={}, "
          + "canonical_org_entity={}, canonical_person_entity={}", orgBridgeRows.size(),
          personBridgeRows.size(), canonicalOrgRows.size(), canonicalPersonRows.size());
    } catch (Exception e) {
      // afterTable declares no throws clause; this is a best-effort post-processing pass over
      // already-committed data, so a failure here must not fail the ref schema's own ETL run.
      LOGGER.error("EntityBridgeListener: build failed for runId={}", runId, e);
    }
  }

  // ========================================================================
  // Org track
  // ========================================================================

  private void runOrgSource(Connection conn, String base, OrgSource src) throws SQLException {
    String loc = loc(base, src.schema, src.physicalTable);
    // GROUP BY dedup, not QUALIFY row_number() OVER (...): some sources are enormous with very
    // low cardinality on the name column (cms_open_payments: 76.5M rows -> 3,233 distinct payer
    // names). A window-function rank over the full row set forced DuckDB to materialize/sort all
    // 76.5M rows before filtering to rank=1 and hard-OOM'd inside the worker's memory ceiling on
    // a live run; a hash GROUP BY collapses to the ~3k groups directly and is dramatically
    // cheaper (confirmed live: same source went from OOM to ~1s at a much tighter memory limit).
    boolean hasOrder = src.orderByExpr != null;
    String nameAgg = hasOrder
        ? "arg_max(" + src.nameExpr + ", " + src.orderByExpr + ")"
        : "any_value(" + src.nameExpr + ")";
    String einAgg = src.einExpr == null
        ? "CAST(NULL AS VARCHAR)"
        : (hasOrder ? "arg_max(" + src.einExpr + ", " + src.orderByExpr + ")"
            : "any_value(" + src.einExpr + ")");
    StringBuilder stage = new StringBuilder();
    stage.append("CREATE OR REPLACE TEMP TABLE stage_norm AS ")
        .append("SELECT name_raw, norm_org_name(name_raw) AS name_norm, ")
        .append("org_block_key(name_raw) AS block_key, org_remainder(name_raw) AS remainder, ")
        .append("org_last_token(name_raw) AS last_token, org_given_part(name_raw) AS given_part, ")
        .append("is_person_shaped(name_raw) AS is_person, ")
        .append("source_key, ein_val FROM (")
        .append("SELECT COALESCE(").append(src.keyExpr != null ? src.keyExpr : "NULL")
        .append(", norm_org_name(").append(src.nameExpr).append(")) AS source_key, ")
        .append(nameAgg).append(" AS name_raw, ").append(einAgg).append(" AS ein_val ")
        .append("FROM iceberg_scan('").append(loc).append("', allow_moved_paths=true) AS s ")
        .append("WHERE ").append(src.nameExpr).append(" IS NOT NULL AND trim(").append(src.nameExpr)
        .append(") <> '' ")
        .append(src.extraWhere != null ? "AND (" + src.extraWhere + ") " : "")
        .append("GROUP BY 1) g");
    execute(conn, stage.toString());

    long total = scalarLong(conn, "SELECT count(*) FROM stage_norm");

    String matchSql =
        "WITH ein_matched AS ("
        + "  SELECT n.source_key, n.name_raw, n.name_norm, e.cik AS sec_cik, "
        + "         CAST(NULL AS VARCHAR) AS lei, CAST(NULL AS VARCHAR) AS gleif_legal_name, "
        + "         'exact_ein' AS match_method, 'high' AS match_confidence, 1.0 AS match_score "
        + "  FROM stage_norm n JOIN ein_hub e ON n.ein_val = e.irs_number"
        + "), exact_matched AS ("
        + "  SELECT n.source_key, n.name_raw, n.name_norm, CAST(NULL AS VARCHAR) AS sec_cik, "
        + "         g.lei, g.legal_name AS gleif_legal_name, "
        + "         'exact_normalized' AS match_method, 'high' AS match_confidence, 1.0 AS match_score "
        + "  FROM stage_norm n JOIN current_gleif g ON n.name_norm = g.norm_name "
        + "  WHERE n.source_key NOT IN (SELECT source_key FROM ein_matched)"
        + "), fuzzy_best_business AS ("
        + "  SELECT n.source_key, n.name_raw, n.name_norm, g.lei, g.legal_name AS gleif_legal_name, "
        // Scored on the remainder AFTER the shared block token, not the full name — see
        // org_remainder's javadoc-style comment in createMacros for why full-name scoring here
        // double-counts the blocking join's guaranteed shared prefix and inflates every score.
        + "         jaro_winkler_similarity(n.remainder, g.remainder) AS score "
        + "  FROM stage_norm n "
        + "  JOIN gleif_block_sizes bs ON n.block_key = bs.block_key "
        + "    AND bs.block_size <= " + MAX_FUZZY_BLOCK_SIZE + " "
        + "  JOIN current_gleif g ON n.block_key = g.block_key "
        + "  WHERE NOT n.is_person "
        + "    AND n.source_key NOT IN (SELECT source_key FROM ein_matched) "
        + "    AND n.source_key NOT IN (SELECT source_key FROM exact_matched) "
        + "  QUALIFY row_number() OVER (PARTITION BY n.source_key ORDER BY score DESC) = 1"
        + "), fuzzy_best_person AS ("
        // Person-shaped rows (see is_person_shaped's javadoc in createMacros) get the person-track's
        // conservative pattern instead: blocked by an EXACT last-token match (never fuzzy the
        // surname), scoring only the given-name part — not the first-token/remainder split used
        // for business names.
        + "  SELECT n.source_key, n.name_raw, n.name_norm, g.lei, g.legal_name AS gleif_legal_name, "
        + "         jaro_winkler_similarity(n.given_part, g.given_part) AS score "
        + "  FROM stage_norm n "
        + "  JOIN gleif_last_token_sizes ts ON n.last_token = ts.last_token "
        + "    AND ts.block_size <= " + MAX_FUZZY_BLOCK_SIZE + " "
        + "  JOIN current_gleif g ON n.last_token = g.last_token "
        + "  WHERE n.is_person "
        + "    AND n.source_key NOT IN (SELECT source_key FROM ein_matched) "
        + "    AND n.source_key NOT IN (SELECT source_key FROM exact_matched) "
        + "  QUALIFY row_number() OVER (PARTITION BY n.source_key ORDER BY score DESC) = 1"
        + "), fuzzy_best AS ("
        + "  SELECT * FROM fuzzy_best_business UNION ALL SELECT * FROM fuzzy_best_person"
        + "), fuzzy_matched AS ("
        + "  SELECT source_key, name_raw, name_norm, CAST(NULL AS VARCHAR) AS sec_cik, lei, gleif_legal_name, "
        + "         'fuzzy' AS match_method, "
        + "         CASE WHEN score >= " + FUZZY_HIGH_THRESHOLD + " THEN 'high' ELSE 'low' END AS match_confidence, "
        + "         score AS match_score "
        + "  FROM fuzzy_best WHERE score >= " + FUZZY_LOW_THRESHOLD
        + ") "
        + "INSERT INTO all_org_mentions "
        + "SELECT '" + esc(src.schema) + "', '" + esc(src.sourceTableLabel()) + "', '"
        + esc(src.sourceColumnLabel) + "', n.source_key, n.name_raw, n.name_norm, "
        + "m.lei, m.sec_cik, m.gleif_legal_name, m.match_method, m.match_confidence, "
        + "m.match_score, "
        + "COALESCE(m.lei, m.sec_cik, 'h:' || md5('" + esc(src.schema) + "."
        + esc(src.sourceTableLabel())
        + "." + esc(src.sourceColumnLabel) + ".' || n.source_key)), '"
        + esc(src.canonicalColumn) + "' "
        + "FROM stage_norm n "
        + "LEFT JOIN (SELECT * FROM ein_matched UNION ALL SELECT * FROM exact_matched "
        + "           UNION ALL SELECT * FROM fuzzy_matched) m ON n.source_key = m.source_key";
    execute(conn, matchSql);

    long matched = scalarLong(conn,
        "SELECT count(*) FROM all_org_mentions WHERE source_schema = '" + esc(src.schema)
        + "' AND source_table = '" + esc(src.sourceTableLabel()) + "' AND source_column = '"
        + esc(src.sourceColumnLabel) + "' AND match_method IS NOT NULL");
    // Of this source's unresolved entities, how many never got a fuzzy attempt at all because
    // their block exceeded MAX_FUZZY_BLOCK_SIZE — distinct from "fuzzy ran but scored too low".
    long skippedOversizedBlock = scalarLong(conn,
        "SELECT count(*) FROM stage_norm n "
        + "JOIN gleif_block_sizes bs ON n.block_key = bs.block_key "
        + "WHERE bs.block_size > " + MAX_FUZZY_BLOCK_SIZE + " "
        + "AND n.source_key IN (SELECT source_key FROM all_org_mentions "
        + "  WHERE source_schema = '" + esc(src.schema) + "' AND source_table = '"
        + esc(src.sourceTableLabel()) + "' AND source_column = '" + esc(src.sourceColumnLabel)
        + "' AND match_method IS NULL)");
    LOGGER.info("EntityBridgeListener org source {}.{}.{}: {} entities, {} matched, {} unresolved "
        + "({} skipped: block too large, cap={})",
        src.schema, src.sourceTableLabel(), src.sourceColumnLabel, total, matched, total - matched,
        skippedOversizedBlock, MAX_FUZZY_BLOCK_SIZE);
  }

  private List<Map<String, Object>> pivotOrg(Connection conn) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT canonical_entity_id, ")
        .append("COALESCE(MAX(gleif_legal_name), ")
        .append("arg_max(name_raw, length(name_raw))) AS canonical_name, ")
        .append("MAX(lei) AS lei, MAX(sec_cik) AS sec_cik");
    for (OrgSource src : ORG_SOURCES) {
      sql.append(", MAX(CASE WHEN canonical_column = '").append(esc(src.canonicalColumn))
          .append("' THEN source_key END) AS ").append(src.canonicalColumn);
      sql.append(", MAX(CASE WHEN canonical_column = '").append(esc(src.canonicalColumn))
          .append("' THEN match_confidence END) AS ").append(src.canonicalColumn)
          .append("_confidence");
    }
    sql.append(" FROM all_org_mentions GROUP BY canonical_entity_id");
    return queryRows(conn, sql.toString());
  }

  // ========================================================================
  // Person track
  // ========================================================================

  private void stagePersonSource(Connection conn, String base, int idx, PersonSource src)
      throws SQLException {
    String loc = loc(base, src.schema, src.physicalTable);
    // GROUP BY dedup, matching the org-track fix — see runOrgSource for why QUALIFY
    // row_number() OVER (...) is unsafe here (forces a full-table sort/rank before
    // filtering, which hard-OOM'd on a 76M-row source in the org track).
    boolean hasOrder = src.orderByExpr != null;
    String orderCol = hasOrder ? ", " + src.orderByExpr + " AS order_key" : "";
    String nameRawAgg = hasOrder ? "arg_max(name_raw, order_key)" : "any_value(name_raw)";
    String lastAgg = hasOrder ? "arg_max(last_name, order_key)" : "any_value(last_name)";
    String firstAgg = hasOrder ? "arg_max(first_name, order_key)" : "any_value(first_name)";
    String middleAgg = hasOrder ? "arg_max(middle_name, order_key)" : "any_value(middle_name)";
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE OR REPLACE TEMP TABLE person_stage_").append(idx).append(" AS SELECT ")
        .append("source_key, ")
        .append(nameRawAgg).append(" AS name_raw, ")
        .append("upper(trim(").append(lastAgg).append(")) AS last_name, ")
        .append("upper(trim(").append(firstAgg).append(")) AS first_name, ")
        .append("upper(trim(COALESCE(").append(middleAgg).append(", ''))) AS middle_name ")
        .append("FROM (SELECT ")
        .append("COALESCE(").append(src.keyExpr != null ? src.keyExpr : "NULL")
        .append(", upper(trim(").append(src.lastNameExpr).append(")) || '|' || upper(trim(")
        .append(src.firstNameExpr).append("))) AS source_key, ")
        .append(src.nameRawExpr).append(" AS name_raw, ")
        .append(src.lastNameExpr).append(" AS last_name, ")
        .append(src.firstNameExpr).append(" AS first_name, ")
        .append(src.middleNameExpr != null ? src.middleNameExpr : "CAST(NULL AS VARCHAR)")
        .append(" AS middle_name").append(orderCol).append(" ")
        .append("FROM iceberg_scan('").append(loc).append("', allow_moved_paths=true) AS s ")
        .append("WHERE ").append(src.lastNameExpr).append(" IS NOT NULL AND trim(")
        .append(src.lastNameExpr)
        .append(") <> '' AND ").append(src.firstNameExpr).append(" IS NOT NULL AND trim(")
        .append(src.firstNameExpr).append(") <> '' ")
        .append(src.extraWhere != null ? "AND (" + src.extraWhere + ") " : "")
        .append(") t GROUP BY source_key");
    execute(conn, sql.toString());
  }

  private List<Map<String, Object>> matchPersonPair(Connection conn, int i, int j, PersonSource a,
      PersonSource b, String runId) throws SQLException {
    String ta = "person_stage_" + i;
    String tb = "person_stage_" + j;
    String sql =
        "WITH exact AS ("
        + "  SELECT a.source_key AS a_key, a.name_raw AS a_name, "
        + "         b.source_key AS b_key, b.name_raw AS b_name, "
        + "         'exact_normalized' AS match_method, 'high' AS match_confidence, "
        + "         1.0 AS match_score "
        + "  FROM " + ta + " a JOIN " + tb + " b "
        + "    ON a.last_name = b.last_name AND a.first_name = b.first_name"
        + "), fuzzy_candidates AS ("
        + "  SELECT a.source_key AS a_key, a.name_raw AS a_name, "
        + "         b.source_key AS b_key, b.name_raw AS b_name, "
        + "         jaro_winkler_similarity(a.first_name, b.first_name) AS fscore "
        + "  FROM " + ta + " a JOIN " + tb + " b ON a.last_name = b.last_name "
        + "  WHERE a.first_name <> b.first_name"
        + "), fuzzy AS ("
        + "  SELECT a_key, a_name, b_key, b_name, 'fuzzy_first_name' AS match_method, "
        + "         'low' AS match_confidence, fscore AS match_score "
        + "  FROM fuzzy_candidates WHERE fscore >= " + FUZZY_LOW_THRESHOLD + " "
        + "  QUALIFY row_number() OVER (PARTITION BY a_key ORDER BY fscore DESC) = 1"
        + ") "
        + "SELECT '" + esc(a.schema) + "' AS source_a_schema, '" + esc(a.sourceTableLabel())
        + "' AS source_a_table, a_key AS source_a_key, a_name AS source_a_name_raw, "
        + "'" + esc(b.schema) + "' AS source_b_schema, '" + esc(b.sourceTableLabel())
        + "' AS source_b_table, b_key AS source_b_key, b_name AS source_b_name_raw, "
        + "match_method, match_confidence, match_score, '" + esc(runId) + "' AS match_run_id "
        + "FROM (SELECT * FROM exact UNION ALL SELECT * FROM fuzzy) m";
    List<Map<String, Object>> rows = queryRows(conn, sql);
    LOGGER.info("EntityBridgeListener person pair {}.{} x {}.{}: {} matches",
        a.schema, a.sourceTableLabel(), b.schema, b.sourceTableLabel(), rows.size());
    return rows;
  }

  /**
   * Pivots {@code entity_person_bridge} pairwise matches plus every unmatched person mention into
   * {@code canonical_person_entity} rows via connected components (union-find) over the match
   * graph — done in Java rather than SQL because DuckDB has no built-in graph-closure primitive
   * and the person-type registry is small (4 sources, capped fan-out).
   */
  private List<Map<String, Object>> pivotPerson(Connection conn,
      List<Map<String, Object>> personBridgeRows) throws SQLException {
    UnionFind uf = new UnionFind();
    Map<String, String> bestConfidence = new HashMap<String, String>();
    Map<String, String> mentionCanonicalColumn = new HashMap<String, String>();
    Map<String, String> mentionNameRaw = new LinkedHashMap<String, String>();
    Map<String, String> mentionSourceKey = new HashMap<String, String>();

    for (int idx = 0; idx < PERSON_SOURCES.size(); idx++) {
      PersonSource src = PERSON_SOURCES.get(idx);
      List<Map<String, Object>> mentions = queryRows(conn,
          "SELECT source_key, name_raw FROM person_stage_" + idx);
      for (Map<String, Object> m : mentions) {
        String key = (String) m.get("source_key");
        String node = nodeId(src.schema, src.sourceTableLabel(), key);
        uf.find(node);
        mentionCanonicalColumn.put(node, src.canonicalColumn);
        mentionNameRaw.put(node, (String) m.get("name_raw"));
        mentionSourceKey.put(node, key);
      }
    }

    for (Map<String, Object> row : personBridgeRows) {
      String nodeA = nodeId((String) row.get("source_a_schema"), (String) row.get("source_a_table"),
          (String) row.get("source_a_key"));
      String nodeB = nodeId((String) row.get("source_b_schema"), (String) row.get("source_b_table"),
          (String) row.get("source_b_key"));
      uf.union(nodeA, nodeB);
      String confidence = (String) row.get("match_confidence");
      updateBest(bestConfidence, nodeA, confidence);
      updateBest(bestConfidence, nodeB, confidence);
    }

    Map<String, List<String>> groups = new LinkedHashMap<String, List<String>>();
    for (String node : mentionNameRaw.keySet()) {
      String root = uf.find(node);
      List<String> members = groups.get(root);
      if (members == null) {
        members = new ArrayList<String>();
        groups.put(root, members);
      }
      members.add(node);
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (List<String> members : groups.values()) {
      Collections.sort(members);
      String representative = members.get(0);
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("canonical_entity_id", sha256Hex(representative));
      row.put("canonical_name", mentionNameRaw.get(representative));
      for (PersonSource src : PERSON_SOURCES) {
        row.put(src.canonicalColumn, null);
        row.put(src.canonicalColumn + "_confidence", null);
      }
      for (String node : members) {
        String col = mentionCanonicalColumn.get(node);
        row.put(col, mentionSourceKey.get(node));
        row.put(col + "_confidence", bestConfidence.get(node));
      }
      rows.add(row);
    }
    return rows;
  }

  private static void updateBest(Map<String, String> map, String node, String confidence) {
    if ("high".equals(map.get(node))) {
      return;
    }
    if (confidence != null) {
      map.put(node, confidence);
    }
  }

  private static String nodeId(String schema, String table, String key) {
    return schema + "." + table + ":" + key;
  }

  /** Simple path-compressing union-find over string node ids. */
  private static final class UnionFind {
    private final Map<String, String> parent = new HashMap<String, String>();

    String find(String x) {
      String p = parent.get(x);
      if (p == null) {
        parent.put(x, x);
        return x;
      }
      if (!p.equals(x)) {
        p = find(p);
        parent.put(x, p);
      }
      return p;
    }

    void union(String a, String b) {
      String ra = find(a);
      String rb = find(b);
      if (!ra.equals(rb)) {
        parent.put(ra, rb);
      }
    }
  }

  // ========================================================================
  // Shared staging (GLEIF dedup, EIN hub) and DuckDB connection setup
  // ========================================================================

  private static void createMacros(Connection conn) throws SQLException {
    // Normalization applied identically on every org-type source and on current_gleif: lowercase,
    // '&' -> ' and ', strip '.'/',' , strip one trailing legal-suffix token, collapse whitespace.
    // Leading-article strip ('the '/'a '/'an ') runs BEFORE the block key is derived — without
    // it, block_key = first-normalized-token collapses ~67,000 GLEIF entities ("The Boeing
    // Company" etc.) into a single "the" block, and every source org starting with "The" then
    // fuzzy-joins against all 67k of them. Confirmed live: this alone was enough to push a
    // single org source's join past a 2GB DuckDB memory ceiling inside the ETL worker process.
    // strip_legal_suffix_once removes exactly one trailing legal-suffix token. A single pass is
    // not enough for compound suffixes: "MURFIN DRILLING CO INC" strips only "inc", leaving "co";
    // "MURFIN DRILLING COMPANY, INC." also strips only "inc", leaving "company" — same real
    // company, but "co" != "company" so they'd miss the exact-match tier entirely and fall to
    // fuzzy scoring (confirmed live: this exact pair scored 0.9375 fuzzy instead of matching
    // exactly). Nesting the strip 3 deep (below, in norm_org_name) resolves both to "murfin
    // drilling" and lets the deterministic exact-match tier catch them instead. 3 is a practical
    // bound, not a proof — no realistic legal name has more than 2-3 stacked suffix-like trailing
    // words, and each extra pass costs nothing when there's nothing left to strip.
    execute(conn,
        "CREATE OR REPLACE MACRO strip_legal_suffix_once(x) AS (trim(regexp_replace(x, "
        + "'\\s+(inc|llc|corp|corporation|co|company|ltd|llp|lp|plc|pc|pa|na|n a|partnership|"
        + "holdings|partners|fund|trust|group|management)\\.?\\s*$', '', 'i')))");
    execute(conn,
        "CREATE OR REPLACE MACRO norm_org_name(x) AS (trim(regexp_replace(strip_legal_suffix_once("
        + "strip_legal_suffix_once(strip_legal_suffix_once(regexp_replace(regexp_replace("
        + "regexp_replace(lower(trim(x)), '&', ' and ', 'g'), '[.,]', '', 'g'), "
        + "'^(the|a|an) +', '', 'i')))), '\\s+', ' ', 'g')))");
    execute(conn,
        "CREATE OR REPLACE MACRO org_block_key(x) AS (split_part(norm_org_name(x), ' ', 1))");
    // Every fuzzy candidate pair already shares block_key (the first normalized token) by
    // construction of the blocking join below — scoring jaro_winkler_similarity() on the FULL
    // normalized name therefore double-counts that guaranteed shared prefix, since Jaro-Winkler
    // itself adds an explicit bonus for shared prefixes. Confirmed live against real fmcsa_carriers/
    // gleif_entities data: this inflated scores high enough that random samples at EVERY band,
    // including the ostensibly "high confidence" >=0.92 tier, were mostly wrong matches sharing
    // only a first word (e.g. "GARY MIHM" vs "Gary" scored 0.889; "BRIAN M BLACK" vs
    // "BRIAN C. MALK TRUST" scored 0.968) — a ~53% raw "match" rate for trucking-company names
    // against a global LEI registry, empirically implausible as real entity overlap. Scoring the
    // REMAINDER after the shared token instead directly measures the only actually-discriminating
    // part of the name; both of the examples above score 0.0 on their remainders and are correctly
    // rejected. No fallback to the full name for an empty remainder (single-token name) is used —
    // jaro_winkler_similarity('', x) returns 0.0 in DuckDB (confirmed live), which already fails
    // the low threshold on its own, and falling back to full-name comparison for that case would
    // just reintroduce the same prefix-bonus bug for the common "bare single-word name" shape.
    execute(conn,
        "CREATE OR REPLACE MACRO org_remainder(x) AS "
        + "(trim(regexp_replace(norm_org_name(x), '^\\S+\\s*', '')))");
    // First-token blocking + remainder scoring still isn't enough for names that are just a bare
    // personal name (an individual sole-proprietor's own name used as their business's on-file
    // name, common in fmcsa_carriers): remainder scoring on "GARY MIHM" vs "GARY TAXI CORP" (both
    // sharing block_key "gary") compares "mihm" against "taxi corp" — correctly low — but common
    // full-name coincidences score deceptively high on their OWN remainder comparison: "JOSE
    // HERNANDEZ" vs "Jose Hernandez B.V." scored 0.95 on remainder logic alone (confirmed live),
    // because "Hernandez"-shaped surnames just aren't rare enough for jaro_winkler to distinguish
    // a coincidence from a real match. The existing person-track pipeline already solves exactly
    // this for person-type sources — never fuzzy-match the surname, only fuzzy the given name,
    // after an EXACT surname match — because common surnames are "the single biggest
    // false-positive risk" (see matchPersonPair). is_person_shaped flags org-track rows likely to
    // be a bare personal name (reusing the same legal-marker vocabulary as SEC_ENTITY_MARKER_REGEX,
    // generalized to any source column instead of one hardcoded SEC field) so runOrgSource can
    // route them through the equivalent exact-last-token / fuzzy-given-part pipeline instead of
    // the business-name remainder pipeline. A short, unmarked BUSINESS name (e.g. "FAST LANE") gets
    // misclassified as person-shaped too — an acceptable, safety-biased side effect: it just means
    // that name is held to the stricter exact-last-word standard instead of the looser business
    // path, never the other way around.
    execute(conn,
        "CREATE OR REPLACE MACRO is_person_shaped(x) AS (NOT regexp_matches(upper(x), '"
        + SEC_ENTITY_MARKER_REGEX + "') AND (length(trim(x)) - length(replace(trim(x), ' ', '')) "
        + "+ 1) BETWEEN 2 AND 3)");
    execute(conn,
        "CREATE OR REPLACE MACRO org_last_token(x) AS "
        + "(split_part(norm_org_name(x), ' ', len(string_split(norm_org_name(x), ' '))))");
    execute(conn,
        "CREATE OR REPLACE MACRO org_given_part(x) AS "
        + "(trim(regexp_replace(norm_org_name(x), '\\s*\\S+$', '')))");
  }

  private static void stageGleif(Connection conn, String base) throws SQLException {
    // ref.current_gleif_entities is a Calcite-level SQL view (ref-schema.yaml), not a physical
    // Iceberg table — there is nothing at <warehouse>/ref/current_gleif_entities for iceberg_scan
    // to read. The "latest row per LEI" dedup it performs is replicated here directly against the
    // physical ref/gleif_entities table.
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE current_gleif AS "
        + "SELECT lei, legal_name, norm_org_name(legal_name) AS norm_name, "
        + "org_block_key(legal_name) AS block_key, org_remainder(legal_name) AS remainder, "
        + "org_last_token(legal_name) AS last_token, org_given_part(legal_name) AS given_part "
        + "FROM iceberg_scan('" + loc(base, "ref", "gleif_entities") + "', allow_moved_paths=true) "
        + "QUALIFY row_number() OVER (PARTITION BY lei ORDER BY last_update DESC) = 1");
    // Precomputed once, shared across every org-type source's fuzzy pass — see
    // MAX_FUZZY_BLOCK_SIZE's javadoc for the measured join-cost numbers this bounds.
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE gleif_block_sizes AS "
        + "SELECT block_key, count(*) AS block_size FROM current_gleif GROUP BY block_key");
    // Same cap, applied to the person-shaped branch's exact-last-token blocking join instead of
    // first-token blocking — a common surname (e.g. "smith") can still collect a large GLEIF-side
    // block even matched exactly, so this needs the same safety valve as gleif_block_sizes.
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE gleif_last_token_sizes AS "
        + "SELECT last_token, count(*) AS block_size FROM current_gleif GROUP BY last_token");
  }

  private static void stageEinHub(Connection conn, String base) throws SQLException {
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE ein_hub AS "
        + "SELECT cik, irs_number FROM iceberg_scan('" + loc(base, "sec", "filing_metadata")
        + "', allow_moved_paths=true) WHERE irs_number IS NOT NULL AND trim(irs_number) <> ''");
  }

  private Connection openDuckDb(TableContext context) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=2");
      stmt.execute("SET preserve_insertion_order=false");
      // This listener runs inside a shared ETL worker JVM (observed -Xmx3g
      // -XX:MaxDirectMemorySize=768m), not a dedicated process — DuckDB's own buffer
      // manager competes with the live JVM heap for the same RSS budget. A 4GB DuckDB
      // limit inside that process OOM'd for real (ghg_facilities, ~3.7GiB/3.7GiB used)
      // on a live run. The real fix is temp_directory below (lets DuckDB spill instead
      // of hard-failing); 2GB here just keeps the common case in memory.
      stmt.execute("SET memory_limit='2GB'");
      // Without an explicit temp_directory DuckDB has nowhere to spill once memory_limit
      // is hit, so it hard-OOMs instead of paging to disk — confirmed live: a 1GB limit
      // with no temp_directory OOM'd on the very first org source (current_gleif's ~3.2M
      // deduped GLEIF rows alone approach the limit). This is the actual fix; memory_limit
      // above is just how much stays resident before DuckDB spills the rest here.
      String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + "/entity-bridge-duckdb";
      stmt.execute("SET temp_directory='" + tempDir + "'");
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        LOGGER.debug("Parquet extension already loaded or built-in");
      }
    }
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
      stmt.execute("SET unsafe_enable_version_guessing = true");
    } catch (SQLException e) {
      LOGGER.warn("DuckDB Iceberg extension unavailable: {}", e.getMessage());
    }
    Map<String, String> s3Config = context.getStorageProvider() != null
        ? context.getStorageProvider().getS3Config() : null;
    if (s3Config != null && !s3Config.isEmpty()) {
      try (Statement stmt = conn.createStatement()) {
        configureS3(stmt, s3Config);
      }
    }
    return conn;
  }

  private static void configureS3(Statement stmt, Map<String, String> s3Config)
      throws SQLException {
    stmt.execute("INSTALL httpfs");
    stmt.execute("LOAD httpfs");
    stmt.execute("SET http_timeout=10000");
    stmt.execute("SET http_retries=2");
    stmt.execute("SET http_retry_wait_ms=500");
    String accessKey = s3Config.get("accessKeyId");
    String secretKey = s3Config.get("secretAccessKey");
    String endpoint = s3Config.get("endpoint");
    String region = s3Config.containsKey("region") ? s3Config.get("region") : "auto";
    if (accessKey != null && secretKey != null) {
      StringBuilder secret = new StringBuilder("CREATE OR REPLACE SECRET calcite_s3 (TYPE S3");
      secret.append(", KEY_ID '").append(accessKey).append('\'');
      secret.append(", SECRET '").append(secretKey).append('\'');
      if (endpoint != null && !endpoint.isEmpty()) {
        String endpointHost = endpoint.replaceFirst("^https?://", "");
        secret.append(", ENDPOINT '").append(endpointHost).append('\'');
        secret.append(", URL_STYLE 'path'");
        secret.append(", USE_SSL ").append(endpoint.startsWith("http://") ? "false" : "true");
      }
      secret.append(", REGION '").append(region).append('\'');
      secret.append(')');
      stmt.execute(secret.toString());
    }
  }

  // ========================================================================
  // Write-out
  // ========================================================================

  private static void writeTable(TableContext context, String tableName,
      List<Map<String, Object>> rows) throws IOException {
    EtlPipelineConfig tableConfig = tableConfigOf(context, tableName);
    MaterializeConfig matConfig = tableConfig.getMaterialize();
    String schemaMaterializeDir = context.getSchemaContext().getMaterializeDirectory()
        + "/" + context.getSchemaName();
    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        matConfig, context.getStorageProvider(), schemaMaterializeDir,
        context.getIncrementalTracker());
    writer.initialize(matConfig);
    writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());
    writer.commit();
    writer.close();
    LOGGER.info("EntityBridgeListener: wrote {} rows to ref.{}", rows.size(), tableName);
  }

  private static EtlPipelineConfig tableConfigOf(TableContext context, String tableName) {
    for (EtlPipelineConfig cfg : context.getSchemaContext().getTables()) {
      if (tableName.equals(cfg.getName())) {
        return cfg;
      }
    }
    throw new IllegalStateException(
        "EntityBridgeListener: table config not found for " + tableName);
  }

  // ========================================================================
  // Small SQL/JDBC helpers
  // ========================================================================

  private static String loc(String base, String schema, String table) {
    return base + "/" + schema + "/" + table;
  }

  private static String esc(String s) {
    return s.replace("'", "''");
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private static long scalarLong(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static List<Map<String, Object>> queryRows(Connection conn, String sql)
      throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      int n = md.getColumnCount();
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (int i = 1; i <= n; i++) {
          row.put(md.getColumnLabel(i), rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static String sha256Hex(String input) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        String h = Integer.toHexString(b & 0xff);
        if (h.length() == 1) {
          hex.append('0');
        }
        hex.append(h);
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  // ========================================================================
  // Registry
  // ========================================================================

  /** One org-type source registry entry — see entity-resolution-plan.md's "Org-type sources". */
  private static final class OrgSource {
    final String schema;
    final String physicalTable;
    private final String sourceTableLabel;
    final String nameExpr;
    final String keyExpr;
    final String einExpr;
    final String sourceColumnLabel;
    final String extraWhere;
    final String orderByExpr;
    final String canonicalColumn;

    OrgSource(String schema, String physicalTable, String sourceTableLabel, String nameExpr,
        String keyExpr, String einExpr, String sourceColumnLabel, String extraWhere,
        String orderByExpr, String canonicalColumn) {
      this.schema = schema;
      this.physicalTable = physicalTable;
      this.sourceTableLabel = sourceTableLabel;
      this.nameExpr = nameExpr;
      this.keyExpr = keyExpr;
      this.einExpr = einExpr;
      this.sourceColumnLabel = sourceColumnLabel;
      this.extraWhere = extraWhere;
      this.orderByExpr = orderByExpr;
      this.canonicalColumn = canonicalColumn;
    }

    /** The value written to entity_org_bridge.source_table — physicalTable unless overridden. */
    String sourceTableLabel() {
      return sourceTableLabel != null ? sourceTableLabel : physicalTable;
    }
  }

  /** One person-type registry entry — see entity-resolution-plan.md's "Person-type sources". */
  private static final class PersonSource {
    final String schema;
    final String physicalTable;
    private final String sourceTableLabel;
    final String keyExpr;
    final String nameRawExpr;
    final String lastNameExpr;
    final String firstNameExpr;
    final String middleNameExpr;
    final String extraWhere;
    final String orderByExpr;
    final String canonicalColumn;

    PersonSource(String schema, String physicalTable, String sourceTableLabel, String keyExpr,
        String nameRawExpr, String lastNameExpr, String firstNameExpr, String middleNameExpr,
        String extraWhere, String orderByExpr, String canonicalColumn) {
      this.schema = schema;
      this.physicalTable = physicalTable;
      this.sourceTableLabel = sourceTableLabel;
      this.keyExpr = keyExpr;
      this.nameRawExpr = nameRawExpr;
      this.lastNameExpr = lastNameExpr;
      this.firstNameExpr = firstNameExpr;
      this.middleNameExpr = middleNameExpr;
      this.extraWhere = extraWhere;
      this.orderByExpr = orderByExpr;
      this.canonicalColumn = canonicalColumn;
    }

    /** The value written to entity_person_bridge.source_a/b_table — physicalTable unless set. */
    String sourceTableLabel() {
      return sourceTableLabel != null ? sourceTableLabel : physicalTable;
    }
  }
}
