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
import org.apache.calcite.adapter.file.etl.SchemaConfig;
import org.apache.calcite.adapter.file.etl.TableContext;
import org.apache.calcite.adapter.file.etl.TableLifecycleListener;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Builds the cross-schema entity-resolution bridge described in {@code entity-resolution-plan.md}
 * (repo root): links free-text org and individual names across 9 AskAmerica schemas to a GLEIF
 * LEI / SEC EIN hub (orgs) or to each other (individuals).
 *
 * <p>Invoked as a standalone cross-schema job by EntityBridgeOrganizer.main() after all daily
 * ETL finishes (every schema materialized), not as a schema lifecycle hook. See
 * entity-resolution-plan.md for the full source registry and matching algorithm.
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

  // Batches runOrgSource's expensive matching over a source's distinct-name set (see
  // stage_norm_distinct in runOrgSource) so peak DuckDB memory for one batch stays constant
  // regardless of how many distinct names a source has. 22,803-row fec.committees (already
  // collapsing to 4,934 distinct names) runs fine unbatched in ~11s, so this is set well above
  // that as a practical single-batch size for every source but the rare few needing more than
  // one pass.
  private static final int MAX_ORG_MATCH_BATCH_SIZE = 20_000;

  // matchPersonPair's fuzzy tier joins two person-type sources on exact last_name alone, then
  // scores jaro_winkler_similarity on every resulting pair -- same shape as the org track's
  // blocked fuzzy join, so it needs the same cap for the same reason: an uncapped common surname
  // (SMITH, JOHNSON, GARCIA, ...) fans out to |block_a| x |block_b| pairs to score. Confirmed
  // live: with no cap this ran for 20+ minutes with climbing CPU and no forward progress across
  // the existing (small, <200K-row) person-type registry, before ever reaching a size anywhere
  // near what the org track's fmcsa_carriers case (4.47M rows, capped) handles cleanly in
  // minutes. The exact tier (last_name AND first_name equality) doesn't need this: it's a
  // selective two-column hash join, not a score-every-pair scan.
  private static final int PERSON_MAX_BLOCK_SIZE = 500;

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
      // assignee_type is PatentsView's raw numeric type code (a string, not the human-readable
      // label the source column's own schema comment describes) — confirmed live against the
      // physical Iceberg table: '2' = US company (8,305,494 rows), '3' = foreign company
      // (8,809,668 rows), together 97.8% of all 17.5M rows. The literal strings 'US company'/
      // 'foreign company' never occur in the actual data, so the original filter silently
      // matched zero rows — this source contributed nothing despite being the plan's primary
      // motivating use case (patent output joined to GLEIF-linked orgs).
      new OrgSource(
          "patents", "patent_assignees", null,
          "s.assignee_organization", "s.assignee_id", null,
          "assignee_organization",
          "s.assignee_type IN ('2','3')", null,
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
          "lender_name", null, null, "sba_lender_name"),
      // Coverage-gap audit (2026-08-09), added the same way as every entry above: describe_table-
      // confirmed column names, structured key preferred over name-only where the source has one.
      new OrgSource(
          "sec", "institutional_holdings", null,
          "s.manager_name", "s.manager_cik", null,
          "manager_name", null, "s.report_period", "sec_manager_cik"),
      // sec.beneficial_ownership.filer_name deliberately NOT added here: confirmed live (2026-
      // 08-09) that column is badly broken upstream in the SEC 13D/13G extraction pipeline --
      // average value length 7,217 characters (max 874,326), only 292 of 57,125 non-blank values
      // even plausible as a real name (< 100 chars). It's capturing whole filing text blocks, not
      // the filer's name. A live registry test against it scored 0/9,213 matched, as expected
      // once this was found. Separate bug, different subsystem (SEC filing parsing, not entity
      // resolution) -- add this source once that extraction is fixed, same wiring as every other
      // entry here.
      new OrgSource(
          "patents", "trademark_owner", null,
          "s.own_name", "s.own_id", null,
          "own_name", null, null, "patents_trademark_owner_id"),
      new OrgSource(
          "health", "fda_device_recalls", null,
          "s.recalling_firm", "s.cfres_id", null,
          "recalling_firm", null, null, "fda_device_recalling_firm"),
      // product_ndc is per-product, not per-labeler -- many products share one labeler, same
      // shape as patents.patent_assignees keying by assignee_id. stage_norm_distinct's
      // name-collapse (see runOrgSource) already handles that shape cheaply.
      new OrgSource(
          "health", "fda_ndc_products", null,
          "s.labeler_name", "s.product_ndc", null,
          "labeler_name", null, null, "fda_labeler_name"),
      new OrgSource(
          "research", "nsf_herd_by_institution", null,
          "s.institution", "s.inst_id", null,
          "institution", null, "s.year", "nsf_herd_inst_id"),
      new OrgSource(
          "edu", "ipeds_institutions", null,
          "s.inst_name", "CAST(s.unitid AS VARCHAR)", null,
          "inst_name", null, "s.year", "ipeds_unitid"),
      new OrgSource(
          "health", "clinical_trials", null,
          "s.lead_sponsor", "s.nct_id", null,
          "lead_sponsor", null, null, "clinical_trials_nct_id"),
      new OrgSource(
          "health", "fda_drug_recalls", null,
          "s.recalling_firm", "s.recall_number", null,
          "recalling_firm", null, null, "fda_drug_recalling_firm"));

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
          FAA_PERSON_FILTER, null, "faa_registrant_name"),
      // assignee_type '4'/'5' (US/foreign individual) — a patent assigned directly to a person
      // rather than a company; distinct from and excluded by the org-track's patents entry
      // above (which requires '2'/'3'). Confirmed live: 167,198 rows, 99.5% with clean
      // assignee_name_first/assignee_name_last already split — no comma/space parsing needed,
      // same shape as cms_open_payments below.
      new PersonSource(
          "patents", "patent_assignees", null,
          "s.assignee_id", "s.assignee_name_first || ' ' || s.assignee_name_last",
          "s.assignee_name_last", "s.assignee_name_first", null,
          "s.assignee_type IN ('4','5')", null, "patents_individual_assignee_id"),
      // Coverage-gap audit (2026-08-09). "Last, First Middle" comma format, same as
      // fec.candidates above -- identical parsing expressions, different source columns. A
      // member serving N congresses has N rows sharing one bioguide_id; orderByExpr picks the
      // most recent congress's name spelling, same pattern as fec.committees' s.year.
      new PersonSource(
          "officials", "members", null,
          "s.bioguide_id", "s.name_last_first",
          "split_part(s.name_last_first, ',', 1)",
          "list_element(string_split(trim(split_part(s.name_last_first, ',', 2)), ' '), 1)",
          "array_to_string(list_slice(string_split("
              + "trim(split_part(s.name_last_first, ',', 2)), ' '), 2, "
              + "len(string_split(trim(split_part(s.name_last_first, ',', 2)), ' '))), ' ')",
          null, "s.congress", "officials_member_bioguide_id"),
      new PersonSource(
          "officials", "federal_judges", null,
          "s.jid", "s.first_name || ' ' || s.last_name",
          "s.last_name", "s.first_name", "s.middle_name",
          null, null, "officials_judge_jid"));

  /**
   * Builds entity bridges and canonical entities as a standalone job (not as a lifecycle hook).
   * Called by EntityBridgeOrganizer.main() after all daily ETL finishes.
   *
   * @param pgConn Postgres connection (closed by caller)
   * @param materializeDir Parquet materialization directory (e.g., s3://govdata-parquet-v1)
   * @throws Exception on SQL, I/O, or configuration errors
   */
  public void buildBridges(Connection pgConn, String materializeDir) throws Exception {
    buildBridges(pgConn, materializeDir, materializeDir);
  }

  /**
   * As {@link #buildBridges(Connection, String)}, but writes the four result tables somewhere
   * other than where the sources were read.
   *
   * <p>Reads and writes shared one directory until now, which made this sweep impossible to
   * rehearse: pointing it at the DQ bucket also pointed every source read there, and the DQ bucket
   * carries only a subset of them (it has no {@code fec/committees}, for one), so the run died on
   * the first missing source. Splitting them lets a rehearsal read real production inputs and land
   * its output somewhere disposable.
   *
   * @param readDir where source tables are read from
   * @param writeDir where entity_org_bridge / canonical_org_entity / the person pair are written
   */
  public void buildBridges(Connection pgConn, String readDir, String writeDir) throws Exception {
    String runId = Instant.now().toString();
    LOGGER.info("EntityBridgeListener: starting entity-resolution build, runId={}, read={}, "
        + "write={}", runId, readDir, writeDir);
    try (Connection conn = openDuckDb(pgConn)) {
      try {
        buildBridgesInternal(conn, readDir, writeDir, runId);
      } catch (Exception e) {
        LOGGER.error("EntityBridgeListener: build failed for runId={}", runId, e);
        throw e;
      }
    }
  }

  private void buildBridgesInternal(Connection conn, String base, String writeBase, String runId)
      throws SQLException, IOException {
    createMacros(conn);
    stageGleif(conn, base);
    stageEinHub(conn, base);
    stageGleifCikMapping(conn, base);
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE all_org_mentions ("
        + "source_schema VARCHAR, source_table VARCHAR, source_column VARCHAR, source_key VARCHAR, "
        + "name_raw VARCHAR, name_norm VARCHAR, lei VARCHAR, sec_cik VARCHAR, gleif_legal_name VARCHAR, "
        + "match_method VARCHAR, match_confidence VARCHAR, match_score DOUBLE, "
        + "support_count BIGINT, "
        + "canonical_entity_id VARCHAR, canonical_column VARCHAR)");

    for (OrgSource src : ORG_SOURCES) {
      runOrgSource(conn, base, src);
    }

    ResultSetIterator orgBridgeIter = new ResultSetIterator(conn,
        "SELECT source_schema, source_table, source_column, source_key, "
        + "name_raw AS source_name_raw, name_norm AS source_name_normalized, lei, sec_cik, "
        + "gleif_legal_name, match_method, match_confidence, match_score, '" + esc(runId)
        + "' AS match_run_id FROM all_org_mentions WHERE match_method IS NOT NULL");
    long orgBridgeRowCount = writeTableBridges("entity_org_bridge", orgBridgeIter, writeBase);
    long canonicalOrgRowCount = writeTableBridges("canonical_org_entity", pivotOrg(conn), writeBase);

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
    long canonicalPersonRowCount =
        writeTableBridges("canonical_person_entity", pivotPerson(conn, personBridgeRows), writeBase);

    writeTableBridgesSync("entity_person_bridge", personBridgeRows, writeBase);

    LOGGER.info("EntityBridgeListener: complete — entity_org_bridge={}, entity_person_bridge={}, "
        + "canonical_org_entity={}, canonical_person_entity={}", orgBridgeRowCount,
        personBridgeRows.size(), canonicalOrgRowCount, canonicalPersonRowCount);
  }

  @Override public void beforeTable(TableContext context) {
    // No-op: preserved for backwards compatibility with existing configurations
  }

  @Override public boolean onTableError(TableContext context, Exception error) {
    LOGGER.error("EntityBridgeListener: table '{}' failed upstream of entity resolution",
        context.getTableName(), error);
    return true;
  }

  @Override public void afterTable(TableContext context, EtlResult result) {
    // Deprecated: entity resolution now runs standalone via EntityBridgeOrganizer, not as a
    // lifecycle hook. This method remains for backwards compatibility if ref-schema.yaml is
    // not yet updated, but it is no longer the primary entry point.
    if (!TRIGGER_TABLE.equals(context.getTableName())) {
      return;
    }
    LOGGER.warn("EntityBridgeListener.afterTable called as lifecycle hook; this is deprecated. "
        + "Entity resolution should run via EntityBridgeOrganizer.main() in x-schema.sh instead.");
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
        .append("source_key, ein_val, support_count FROM (")
        .append("SELECT COALESCE(").append(src.keyExpr != null ? src.keyExpr : "NULL")
        .append(", norm_org_name(").append(src.nameExpr).append(")) AS source_key, ")
        .append(nameAgg).append(" AS name_raw, ").append(einAgg).append(" AS ein_val, ")
        // Defect Register B2-4: raw-row count behind this source_key before any collapsing —
        // for patents.patent_assignees (keyExpr=s.assignee_id) this is literally the patent
        // count that assignee_id carries, the exact signal needed to prefer the real id over an
        // arbitrary one when a name maps to several (measured: Schlumberger's 10,227-patent id
        // vs. a sibling id with 1).
        .append("count(*) AS support_count ")
        .append("FROM iceberg_scan('").append(loc).append("', allow_moved_paths=true) AS s ")
        .append("WHERE ").append(src.nameExpr).append(" IS NOT NULL AND trim(").append(src.nameExpr)
        .append(") <> '' ")
        .append(src.extraWhere != null ? "AND (" + src.extraWhere + ") " : "")
        .append("GROUP BY 1) g");
    execute(conn, stage.toString());

    long total = scalarLong(conn, "SELECT count(*) FROM stage_norm");

    // block_key/remainder/last_token/given_part are all defined (see createMacros) as functions
    // of norm_org_name(x) alone, so two stage_norm rows sharing name_norm are guaranteed to agree
    // on all four -- collapsing to one row per distinct name_norm before the expensive exact/
    // fuzzy matching is exact, not approximate, for those columns (is_person is the one column
    // computed from the raw, un-normalized text; any_value's tiny residual imprecision there has
    // the same acceptable, safety-biased shape as is_person_shaped's own javadoc already
    // describes for a short unmarked business name). For most sources this is a no-op: keyExpr is
    // usually null, so source_key already equals norm_org_name(name) and stage_norm is already
    // ~1 row per name_norm.
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE stage_norm_distinct AS "
        + "SELECT name_norm, any_value(block_key) AS block_key, "
        + "any_value(remainder) AS remainder, any_value(last_token) AS last_token, "
        + "any_value(given_part) AS given_part, any_value(is_person) AS is_person "
        + "FROM stage_norm GROUP BY name_norm");

    // The distinct-name collapse above isn't enough on its own for every source: confirmed live,
    // patents.patent_assignees collapses only 511,325 stage_norm rows to 510,542 distinct names
    // (PatentsView's assignee_id is already well-disambiguated per source_key, so there's little
    // raw duplication left to remove there -- unlike patent_assignees' own doc comment on why it
    // keys by assignee_id in the first place, see entity-resolution-plan.md). Matching all
    // 510K+ distinct names against current_gleif's 3.2M rows in one shot still hard-OOM'd the
    // worker even with MAX_FUZZY_BLOCK_SIZE capping each individual block (confirmed live:
    // 1.8GiB/1.8GiB used). Batching by name_norm keyset (not OFFSET, which DuckDB would
    // re-scan-and-discard on every call) bounds peak memory for one batch to a constant,
    // regardless of how many distinct names a source has -- same pattern as ChunkOrganizer's SEC
    // backfill batching (see semantic-search-plan.md). Each batch inserts directly into
    // all_org_mentions, so this is also safely resumable in the sense that a later failure only
    // loses that batch's source, not prior sources' already-committed work.
    String cursor = null;
    while (true) {
      String cursorClause = cursor == null ? "" : "WHERE name_norm > '" + esc(cursor) + "' ";
      execute(conn,
          "CREATE OR REPLACE TEMP TABLE stage_norm_batch AS "
          + "SELECT * FROM stage_norm_distinct " + cursorClause
          + "ORDER BY name_norm LIMIT " + MAX_ORG_MATCH_BATCH_SIZE);
      long batchSize = scalarLong(conn, "SELECT count(*) FROM stage_norm_batch");
      if (batchSize == 0) {
        break;
      }

      String matchSql =
          "WITH ein_matched AS ("
          // Row-grain (not name_norm-grain) on purpose: EIN is a per-row fact, not implied by a
          // shared org name (e.g. same brand name, different franchise-location EIN), and this is
          // a cheap direct equi-join against ein_hub, not the blocked fuzzy join's cost driver.
          + "  SELECT n.source_key, e.cik AS sec_cik "
          + "  FROM stage_norm n JOIN stage_norm_batch b ON n.name_norm = b.name_norm "
          + "  JOIN ein_hub e ON n.ein_val = e.irs_number"
          // Defect Register B2-1: unqualified, this join fans out one row per GLEIF entity
          // sharing a normalized name -- measured live at 6 distinct LEIs for
          // name_norm='apple', every one asserted at 'high' with identical evidence, which
          // blocks ranking (resolve_entity can't tie-break candidates that all carry the same
          // score) as well as counting. The register's own cheapest fix: reserve 'high' for a
          // name that resolves to exactly one LEI here, demote a genuine fan-out to
          // 'ambiguous' rather than asserting every candidate as equally certain. Rows are
          // still written for every candidate -- this changes the label, not the candidate set.
          + "), exact_matched AS ("
          + "  SELECT d.name_norm, gc.cik AS sec_cik, "
          + "         g.lei, g.legal_name AS gleif_legal_name, "
          + "         'exact_normalized' AS match_method, "
          + "         CASE WHEN count(*) OVER (PARTITION BY d.name_norm) > 1 THEN 'ambiguous' "
          + "              ELSE 'high' END AS match_confidence, "
          + "         1.0 AS match_score "
          + "  FROM stage_norm_batch d JOIN current_gleif g ON d.name_norm = g.norm_name "
          + "  LEFT JOIN gleif_cik gc ON g.lei = gc.lei"
          + "), fuzzy_best_business AS ("
          + "  SELECT d.name_norm, g.lei, g.legal_name AS gleif_legal_name, "
          // Scored on the remainder AFTER the shared block token, not the full name — see
          // org_remainder's javadoc-style comment in createMacros for why full-name scoring here
          // double-counts the blocking join's guaranteed shared prefix and inflates every score.
          + "         jaro_winkler_similarity(d.remainder, g.remainder) AS score "
          + "  FROM stage_norm_batch d "
          + "  JOIN gleif_block_sizes bs ON d.block_key = bs.block_key "
          + "    AND bs.block_size <= " + MAX_FUZZY_BLOCK_SIZE + " "
          + "  JOIN current_gleif g ON d.block_key = g.block_key "
          + "  WHERE NOT d.is_person "
          + "    AND d.name_norm NOT IN (SELECT name_norm FROM exact_matched) "
          + "  QUALIFY row_number() OVER (PARTITION BY d.name_norm ORDER BY score DESC) = 1"
          + "), fuzzy_best_person AS ("
          // Person-shaped rows (see is_person_shaped's javadoc in createMacros) get the person-track's
          // conservative pattern instead: blocked by an EXACT last-token match (never fuzzy the
          // surname), scoring only the given-name part — not the first-token/remainder split used
          // for business names.
          + "  SELECT d.name_norm, g.lei, g.legal_name AS gleif_legal_name, "
          + "         jaro_winkler_similarity(d.given_part, g.given_part) AS score "
          + "  FROM stage_norm_batch d "
          + "  JOIN gleif_last_token_sizes ts ON d.last_token = ts.last_token "
          + "    AND ts.block_size <= " + MAX_FUZZY_BLOCK_SIZE + " "
          + "  JOIN current_gleif g ON d.last_token = g.last_token "
          + "  WHERE d.is_person "
          + "    AND d.name_norm NOT IN (SELECT name_norm FROM exact_matched) "
          + "  QUALIFY row_number() OVER (PARTITION BY d.name_norm ORDER BY score DESC) = 1"
          + "), fuzzy_best AS ("
          + "  SELECT * FROM fuzzy_best_business UNION ALL SELECT * FROM fuzzy_best_person"
          + "), fuzzy_matched AS ("
          + "  SELECT fb.name_norm, gc.cik AS sec_cik, fb.lei, fb.gleif_legal_name, "
          + "         'fuzzy' AS match_method, "
          + "         CASE WHEN fb.score >= " + FUZZY_HIGH_THRESHOLD + " THEN 'high' ELSE 'low' END AS match_confidence, "
          + "         fb.score AS match_score "
          + "  FROM fuzzy_best fb LEFT JOIN gleif_cik gc ON fb.lei = gc.lei "
          + "  WHERE fb.score >= " + FUZZY_LOW_THRESHOLD
          + "), name_matched AS ("
          + "  SELECT * FROM exact_matched UNION ALL SELECT * FROM fuzzy_matched"
          + ") "
          + "INSERT INTO all_org_mentions "
          + "SELECT '" + esc(src.schema) + "', '" + esc(src.sourceTableLabel()) + "', '"
          + esc(src.sourceColumnLabel) + "', n.source_key, n.name_raw, n.name_norm, "
          // ein_matched (row-grain, more specific) wins over name_matched (name_norm-grain) on any
          // field it actually supplies, per row.
          + "nm.lei, COALESCE(e.sec_cik, nm.sec_cik), nm.gleif_legal_name, "
          + "COALESCE(CASE WHEN e.sec_cik IS NOT NULL THEN 'exact_ein' END, nm.match_method), "
          + "COALESCE(CASE WHEN e.sec_cik IS NOT NULL THEN 'high' END, nm.match_confidence), "
          + "COALESCE(CASE WHEN e.sec_cik IS NOT NULL THEN 1.0 END, nm.match_score), "
          + "n.support_count, "
          + "COALESCE(nm.lei, e.sec_cik, nm.sec_cik, 'h:' || md5('" + esc(src.schema) + "."
          + esc(src.sourceTableLabel())
          + "." + esc(src.sourceColumnLabel) + ".' || n.source_key)), '"
          + esc(src.canonicalColumn) + "' "
          + "FROM stage_norm n "
          + "JOIN stage_norm_batch b ON n.name_norm = b.name_norm "
          + "LEFT JOIN ein_matched e ON n.source_key = e.source_key "
          + "LEFT JOIN name_matched nm ON n.name_norm = nm.name_norm";
      execute(conn, matchSql);

      cursor = scalarString(conn, "SELECT max(name_norm) FROM stage_norm_batch");
      if (batchSize < MAX_ORG_MATCH_BATCH_SIZE) {
        break;
      }
    }

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

  private CloseableRowIterator pivotOrg(Connection conn) throws SQLException {
    // See PivotOrgIterator's javadoc: a wide SQL GROUP BY (COALESCE/MAX(CASE...) over
    // 4 + 2*ORG_SOURCES.size() columns) hard-OOM'd DuckDB's own query execution once the
    // registry grew past ~22 sources. This reduces the same result in Java over a plain
    // ORDER BY scan instead.
    return new PivotOrgIterator(conn, ORG_SOURCES);
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
        + "), block_sizes_a AS ("
        + "  SELECT last_name, count(*) AS n FROM " + ta + " GROUP BY last_name"
        + "), block_sizes_b AS ("
        + "  SELECT last_name, count(*) AS n FROM " + tb + " GROUP BY last_name"
        + "), fuzzy_candidates AS ("
        + "  SELECT a.source_key AS a_key, a.name_raw AS a_name, "
        + "         b.source_key AS b_key, b.name_raw AS b_name, "
        + "         jaro_winkler_similarity(a.first_name, b.first_name) AS fscore "
        + "  FROM " + ta + " a "
        + "  JOIN block_sizes_a ba ON a.last_name = ba.last_name AND ba.n <= "
        + PERSON_MAX_BLOCK_SIZE + " "
        + "  JOIN " + tb + " b ON a.last_name = b.last_name "
        + "  JOIN block_sizes_b bb ON b.last_name = bb.last_name AND bb.n <= "
        + PERSON_MAX_BLOCK_SIZE + " "
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
   * graph — done in Java rather than SQL because DuckDB has no built-in graph-closure primitive.
   * Builds {@code groups} eagerly (connectivity inherently needs every mention seen first), but
   * streams the row-building step via {@link PivotPersonIterator} instead of also materializing
   * a full {@code List<Map<String,Object>>} of the output alongside it -- confirmed live, holding
   * both simultaneously was enough to exhaust this worker's remaining ~1.5GB free heap once the
   * person-type registry grew to 7 sources.
   */
  private CloseableRowIterator pivotPerson(Connection conn,
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

    return new PivotPersonIterator(groups.values().iterator(), mentionNameRaw,
        mentionCanonicalColumn, mentionSourceKey, bestConfidence);
  }

  /** Streams {@link #pivotPerson}'s row-building step — see its javadoc for why. */
  private static final class PivotPersonIterator implements CloseableRowIterator {
    private final Iterator<List<String>> groupIter;
    private final Map<String, String> mentionNameRaw;
    private final Map<String, String> mentionCanonicalColumn;
    private final Map<String, String> mentionSourceKey;
    private final Map<String, String> bestConfidence;
    private long count;

    PivotPersonIterator(Iterator<List<String>> groupIter, Map<String, String> mentionNameRaw,
        Map<String, String> mentionCanonicalColumn, Map<String, String> mentionSourceKey,
        Map<String, String> bestConfidence) {
      this.groupIter = groupIter;
      this.mentionNameRaw = mentionNameRaw;
      this.mentionCanonicalColumn = mentionCanonicalColumn;
      this.mentionSourceKey = mentionSourceKey;
      this.bestConfidence = bestConfidence;
    }

    @Override public long count() {
      return count;
    }

    @Override public boolean hasNext() {
      return groupIter.hasNext();
    }

    @Override public Map<String, Object> next() {
      List<String> members = groupIter.next();
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
      count++;
      return row;
    }

    @Override public void close() {
      // No JDBC resources held directly; nothing to release.
    }
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
  /** Union by size (below) alongside find()'s path compression: without it, union() always
   *  attached the first root under the second regardless of subtree size, so a big enough or
   *  unluckily-ordered set of unions could build an arbitrarily deep chain -- find()'s
   *  recursion then walks (and this worker's -Xss512k makes) that a real risk before the very
   *  first compression on that path. Union by size bounds tree depth to O(log n) unconditionally,
   *  which is what actually guarantees find()'s amortized near-O(1) behavior; path compression
   *  alone only pays that cost off over repeated calls on the same path, not the first one. */
  private static final class UnionFind {
    private final Map<String, String> parent = new HashMap<String, String>();
    private final Map<String, Integer> size = new HashMap<String, Integer>();

    String find(String x) {
      String p = parent.get(x);
      if (p == null) {
        parent.put(x, x);
        size.put(x, 1);
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
      if (ra.equals(rb)) {
        return;
      }
      int sizeA = size.get(ra);
      int sizeB = size.get(rb);
      if (sizeA < sizeB) {
        parent.put(ra, rb);
        size.put(rb, sizeA + sizeB);
      } else {
        parent.put(rb, ra);
        size.put(ra, sizeA + sizeB);
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

  // Defect Register B2-2: sec_cik was only ever populated via ein_matched (exact_ein against
  // ein_hub, itself sourced from sec.filing_metadata.irs_number), and the only ORG_SOURCES entry
  // that supplies an EIN at all is fiscal.exempt_org_master -- tax-exempt nonprofits, whose EINs
  // essentially never coincide with a public company's. That path is not broken; it is a real
  // mechanism applied to a population with ~no overlap with the target, so it was always going to
  // sit near zero matches regardless of data quality. gleif_cik_mapping (LEI -> CIK, restricted to
  // SEC's own registration authority) is a second, independent path: once a source name has
  // already resolved to a GLEIF lei (via exact_matched or fuzzy_matched below), that lei can also
  // resolve a real sec_cik -- measured live in the register at 20,231 recoverable rows this way.
  private static void stageGleifCikMapping(Connection conn, String base) throws SQLException {
    execute(conn,
        "CREATE OR REPLACE TEMP TABLE gleif_cik AS "
        + "SELECT lei, cik FROM iceberg_scan('" + loc(base, "ref", "gleif_cik_mapping")
        + "', allow_moved_paths=true) WHERE lei IS NOT NULL AND cik IS NOT NULL");
  }

  /** Opens DuckDB for standalone orchestrator (no TableContext/StorageProvider). */
  private static Connection openDuckDb(Connection pgConn) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=2");
      stmt.execute("SET preserve_insertion_order=false");
      stmt.execute("SET memory_limit='2GB'");
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
    configureS3FromEnv(conn);
    return conn;
  }

  private Connection openDuckDb(TableContext context) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=2");
      stmt.execute("SET preserve_insertion_order=false");
      stmt.execute("SET memory_limit='2GB'");
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

  private static void configureS3FromEnv(Connection conn) throws SQLException {
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    // AWS_ENDPOINT_OVERRIDE is the object-store endpoint variable everywhere else — .env.prod,
    // ChunkOrganizer alongside this class, GovDataDriver, StorageProviderFactory. S3_ENDPOINT is
    // set by nothing and resolved to null, so this whole path failed to reach storage.
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    String region = System.getenv("AWS_REGION");
    if (region == null || region.isEmpty()) {
      region = "auto";
    }
    if (accessKey != null && secretKey != null) {
      try (Statement stmt = conn.createStatement()) {
        configureS3(stmt, Map.of(
            "accessKeyId", accessKey,
            "secretAccessKey", secretKey,
            "endpoint", endpoint != null ? endpoint : "",
            "region", region
        ));
      }
    }
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
  // Write-out (legacy lifecycle-hook methods and standalone orchestrator methods)
  // ========================================================================

  /**
   * Standalone streaming write (for the {@link #buildBridges} orchestrator path).
   *
   * <p>Builds the target table's writer from ref-schema.yaml rather than a {@code TableContext},
   * which the standalone orchestrator has no way to obtain — it holds only a DuckDB connection
   * and the materialize directory. The lifecycle-hook path's {@link #writeTableStreaming} stays
   * as-is for callers that do have a context.
   */
  private static long writeTableBridges(String tableName, CloseableRowIterator rows,
      String materializeDir) throws IOException {
    if (materializeDir == null || materializeDir.isEmpty()) {
      throw new IOException("EntityBridgeListener: no write directory — cannot write ref."
          + tableName);
    }
    MaterializeConfig matConfig = standaloneMaterializeConfig(tableName);
    StorageProvider storageProvider = StorageProviderFactory.createFromUrl(materializeDir);
    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        matConfig, storageProvider, materializeDir + "/ref");
    writer.initialize(matConfig);
    try {
      writer.writeBatch(rows, Collections.<String, String>emptyMap());
      writer.commit();
    } finally {
      rows.close();
      writer.close();
    }
    long count = rows.count();
    LOGGER.info("EntityBridgeListener: wrote {} rows to ref.{}", count, tableName);
    return count;
  }

  /** Standalone non-streaming write (for the {@link #buildBridges} orchestrator path). */
  private static void writeTableBridgesSync(String tableName, List<Map<String, Object>> rows,
      String materializeDir) throws IOException {
    writeTableBridges(tableName, new ListRowIterator(rows), materializeDir);
  }

  /** Adapts an in-memory row list to the streaming write path. */
  private static final class ListRowIterator implements CloseableRowIterator {
    private final Iterator<Map<String, Object>> src;
    private long count;

    ListRowIterator(List<Map<String, Object>> rows) {
      this.src = rows.iterator();
    }

    @Override public boolean hasNext() {
      return src.hasNext();
    }

    @Override public Map<String, Object> next() {
      count++;
      return src.next();
    }

    @Override public long count() {
      return count;
    }

    @Override public void close() {
    }
  }

  /**
   * Reads one ref table's materialize config straight from the bundled schema YAML, with
   * {@code ${VAR}} placeholders resolved the same way the pipeline resolves them.
   *
   * <p>Deliberately reads the table's own YAML block rather than going through
   * {@link SchemaConfig#fromMap}: that method returns only the schema's <em>ETL pipelines</em>,
   * and it drops any table that declares neither a {@code source} nor enabled {@code hooks}.
   * All four tables written here are source-less, and the two bridge tables have their hooks
   * disabled because this sweep — not the per-table lifecycle — is what populates them. So they
   * are absent from {@code getTables()} by design, and looking for them there can only fail.
   */
  private static MaterializeConfig standaloneMaterializeConfig(String tableName)
      throws IOException {
    MaterializeConfig mat =
        EtlPipelineConfig.materializeFromTableMap(refTableMap(tableName));
    if (mat == null) {
      throw new IOException("EntityBridgeListener: ref." + tableName
          + " has no materialize block in ref-schema.yaml");
    }
    return mat;
  }

  /** Finds one table's raw definition in the bundled ref schema. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> refTableMap(String tableName) throws IOException {
    Map<String, Object> schema = refSchemaMap();
    Object tables = schema.get("partitionedTables");
    if (!(tables instanceof List)) {
      tables = schema.get("tables");
    }
    if (tables instanceof List) {
      for (Object entry : (List<Object>) tables) {
        if (entry instanceof Map
            && tableName.equals(((Map<String, Object>) entry).get("name"))) {
          return (Map<String, Object>) entry;
        }
      }
    }
    throw new IOException("EntityBridgeListener: table ref." + tableName
        + " not found in ref-schema.yaml");
  }

  private static volatile Map<String, Object> refSchemaMapCache;

  @SuppressWarnings("unchecked")
  private static Map<String, Object> refSchemaMap() throws IOException {
    Map<String, Object> cached = refSchemaMapCache;
    if (cached != null) {
      return cached;
    }
    try (InputStream is =
             EntityBridgeListener.class.getResourceAsStream("/ref/ref-schema.yaml")) {
      if (is == null) {
        throw new IOException("EntityBridgeListener: /ref/ref-schema.yaml not on the classpath");
      }
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int n;
      while ((n = is.read(chunk)) > 0) {
        buf.write(chunk, 0, n);
      }
      String yaml = VariableResolver.resolveEnvVars(
          new String(buf.toByteArray(), StandardCharsets.UTF_8));
      Map<String, Object> map = (Map<String, Object>) new Yaml().load(yaml);
      refSchemaMapCache = map;
      return map;
    }
  }

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

  /** Streaming counterpart of {@link #writeTable} — see {@link CloseableRowIterator}. */
  private static long writeTableStreaming(TableContext context, String tableName,
      CloseableRowIterator rows) throws IOException {
    EtlPipelineConfig tableConfig = tableConfigOf(context, tableName);
    MaterializeConfig matConfig = tableConfig.getMaterialize();
    String schemaMaterializeDir = context.getSchemaContext().getMaterializeDirectory()
        + "/" + context.getSchemaName();
    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        matConfig, context.getStorageProvider(), schemaMaterializeDir,
        context.getIncrementalTracker());
    writer.initialize(matConfig);
    try {
      writer.writeBatch(rows, Collections.<String, String>emptyMap());
    } finally {
      rows.close();
    }
    writer.commit();
    writer.close();
    LOGGER.info("EntityBridgeListener: wrote {} rows to ref.{}", rows.count(), tableName);
    return rows.count();
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

  private static String scalarString(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      rs.next();
      return rs.getString(1);
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

  /** Common contract for {@link #writeTableStreaming}'s two row sources — a raw ResultSet
   *  stream and a Java-side reduce over one (see {@link ResultSetIterator}, {@link
   *  PivotOrgIterator}). No checked exceptions on close(): neither implementation throws one. */
  private interface CloseableRowIterator extends Iterator<Map<String, Object>>, AutoCloseable {
    long count();

    @Override void close();
  }

  /**
   * Lazily converts one ResultSet row at a time, unlike {@link #queryRows}. Needed for
   * {@code entity_org_bridge}/{@code canonical_org_entity}: {@code all_org_mentions} now totals
   * on the order of 9-10M rows across the full org-type registry (dominated by
   * transport.fmcsa_carriers' ~4.1M unresolved rows alone) -- confirmed live, materializing that
   * into one Java List crashed the JVM natively under memory pressure. {@link #writeTable}'s
   * writer already accepts a plain {@code Iterator}, so streaming straight from the ResultSet
   * into it, one row at a time, keeps peak heap constant regardless of how large the registry
   * grows. The queries that use this also carry an ORDER BY -- see their own comments for why.
   */
  private static final class ResultSetIterator implements CloseableRowIterator {
    private final Statement stmt;
    private final ResultSet rs;
    private final String[] columnLabels;
    private Boolean hasNextCache;
    private long count;

    ResultSetIterator(Connection conn, String sql) throws SQLException {
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);
      ResultSetMetaData md = rs.getMetaData();
      int n = md.getColumnCount();
      columnLabels = new String[n];
      for (int i = 0; i < n; i++) {
        columnLabels[i] = md.getColumnLabel(i + 1);
      }
    }

    @Override public long count() {
      return count;
    }

    @Override public boolean hasNext() {
      if (hasNextCache == null) {
        try {
          hasNextCache = rs.next();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        if (!hasNextCache) {
          close();
        }
      }
      return hasNextCache;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      hasNextCache = null;
      try {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (int i = 0; i < columnLabels.length; i++) {
          row.put(columnLabels[i], rs.getObject(i + 1));
        }
        count++;
        return row;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public void close() {
      try {
        rs.close();
      } catch (SQLException ignored) {
        // best-effort cleanup
      }
      try {
        stmt.close();
      } catch (SQLException ignored) {
        // best-effort cleanup
      }
    }
  }

  /**
   * Computes canonical_org_entity's pivot as a streaming reduce in Java instead of via a wide
   * SQL GROUP BY. The SQL version (COALESCE/MAX(CASE...) over 4 + 2*ORG_SOURCES.size() columns)
   * needs DuckDB to hold an open aggregate accumulator per in-progress group across every one
   * of those columns simultaneously -- confirmed live, this hard-OOM'd DuckDB's own query
   * execution (not the JVM) once the registry grew past ~22 sources (48 pivoted columns),
   * despite temp_directory being configured for spilling; a wide hash-aggregate apparently
   * can't spill as gracefully as a plain sort can. The source query here does only a cheap
   * ORDER BY canonical_entity_id scan (well-supported, spillable), and since matching rows for
   * one entity are then always contiguous, this reduces one group at a time with a small,
   * fixed-size accumulator -- no per-group state proportional to the registry's width, and no
   * new-column-count sensitivity as more sources get added later.
   */
  private static final class PivotOrgIterator implements CloseableRowIterator {
    private final ResultSetIterator src;
    private final List<OrgSource> orgSources;
    private Map<String, Object> lookahead;
    /** Output rows for the group most recently reduced, drained one per next() call. */
    private final List<Map<String, Object>> pending = new ArrayList<Map<String, Object>>();
    private long count;

    PivotOrgIterator(Connection conn, List<OrgSource> orgSources) throws SQLException {
      this.orgSources = orgSources;
      this.src = new ResultSetIterator(conn,
          "SELECT canonical_entity_id, canonical_column, source_key, match_confidence, "
          + "lei, sec_cik, gleif_legal_name, name_raw, support_count FROM all_org_mentions "
          + "ORDER BY canonical_entity_id");
      advanceLookahead();
    }

    private void advanceLookahead() {
      lookahead = src.hasNext() ? src.next() : null;
    }

    @Override public long count() {
      return count;
    }

    @Override public boolean hasNext() {
      return !pending.isEmpty() || lookahead != null;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (pending.isEmpty()) {
        buildNextGroup();
      }
      count++;
      return pending.remove(0);
    }

    /**
     * Reduces one contiguous {@code canonical_entity_id} group into its output rows.
     *
     * <p>Grain is one row per source mention, per entity-resolution-plan.md: "a canonical org
     * with two patents_assignee_id matches gets two rows sharing the same canonical_entity_id,
     * each with a different patents_assignee_id value". Folding the group into a single row
     * instead forces a choice among a source's several keys, and the ones not chosen become
     * unreachable — measured at 8,426 patent assignee_ids dropped across 5,224 companies, one
     * of them losing 77 of its 78. The identity fields (canonical_name/lei/sec_cik) are still
     * reduced across the whole group and repeated on every row, so a mention row carries both
     * its own source key and the org's resolved identity; a cross-source predicate like
     * {@code sec_cik IS NOT NULL AND patents_assignee_id IS NOT NULL} still matches, and now
     * returns every assignee rather than one.
     *
     * <p>The group is buffered because the identity fields are only known after scanning it.
     * That is bounded by the widest entity, not the table: measured max 8,161 mentions, mean
     * 4.6. The OOM that motivated the streaming reduce came from DuckDB holding an aggregate
     * accumulator per in-progress group across every pivoted column, which this still avoids.
     */
    private void buildNextGroup() {
      Object groupId = lookahead.get("canonical_entity_id");
      String gleifName = null;
      String longestRaw = null;
      String lei = null;
      String secCik = null;
      // Keyed by canonical_column + ' ' + source_key: distinct source keys are distinct
      // mentions and each earns its own row, but the SAME key seen twice is one mention and
      // must not multiply rows. Where a duplicate does occur, the higher support_count wins
      // (Defect Register B2-4's ranking, now applied only to true duplicates).
      Map<String, Map<String, Object>> mentions = new LinkedHashMap<String, Map<String, Object>>();

      while (lookahead != null && groupId.equals(lookahead.get("canonical_entity_id"))) {
        Map<String, Object> row = lookahead;
        String rowGleif = (String) row.get("gleif_legal_name");
        if (gleifName == null && rowGleif != null) {
          gleifName = rowGleif;
        }
        String rowRaw = (String) row.get("name_raw");
        if (rowRaw != null && (longestRaw == null || rowRaw.length() > longestRaw.length())) {
          longestRaw = rowRaw;
        }
        String rowLei = (String) row.get("lei");
        if (lei == null && rowLei != null) {
          lei = rowLei;
        }
        String rowCik = (String) row.get("sec_cik");
        if (secCik == null && rowCik != null) {
          secCik = rowCik;
        }
        String canonicalColumn = (String) row.get("canonical_column");
        String sourceKey = (String) row.get("source_key");
        if (canonicalColumn != null && sourceKey != null) {
          Number rowSupportNum = (Number) row.get("support_count");
          long rowSupport = rowSupportNum != null ? rowSupportNum.longValue() : 0L;
          String dedupKey = canonicalColumn + ' ' + sourceKey;
          Map<String, Object> prior = mentions.get(dedupKey);
          if (prior == null || rowSupport > ((Number) prior.get("support")).longValue()) {
            Map<String, Object> m = new HashMap<String, Object>();
            m.put("column", canonicalColumn);
            m.put("key", sourceKey);
            m.put("confidence", row.get("match_confidence"));
            m.put("support", Long.valueOf(rowSupport));
            mentions.put(dedupKey, m);
          }
        }
        advanceLookahead();
      }

      String canonicalName = gleifName != null ? gleifName : longestRaw;
      if (mentions.isEmpty()) {
        // An org known only through a source with no canonical column still gets its row here,
        // carrying identity alone — "still gets a row here, with lei left null".
        pending.add(identityRow(groupId, canonicalName, lei, secCik));
        return;
      }
      for (Map<String, Object> m : mentions.values()) {
        Map<String, Object> result = identityRow(groupId, canonicalName, lei, secCik);
        String col = (String) m.get("column");
        result.put(col, m.get("key"));
        result.put(col + "_confidence", m.get("confidence"));
        pending.add(result);
      }
    }

    /** A row carrying the group's identity with every per-source column still null. */
    private Map<String, Object> identityRow(Object groupId, String canonicalName, String lei,
        String secCik) {
      Map<String, Object> result = new LinkedHashMap<String, Object>();
      result.put("canonical_entity_id", groupId);
      result.put("canonical_name", canonicalName);
      result.put("lei", lei);
      result.put("sec_cik", secCik);
      for (OrgSource s : orgSources) {
        result.put(s.canonicalColumn, null);
        result.put(s.canonicalColumn + "_confidence", null);
      }
      return result;
    }

    @Override public void close() {
      src.close();
    }
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
