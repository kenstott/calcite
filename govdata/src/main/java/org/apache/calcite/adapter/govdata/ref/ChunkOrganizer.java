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

import org.apache.calcite.adapter.file.partition.PGPipelineTracker;
import org.apache.calcite.adapter.govdata.sec.SemanticTextChunker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.function.Consumer;

/**
 * Organizes chunks into the PG compute layer ({@code vc_staging}), in both of its two content
 * modes: see semantic-search-plan.md "Row-level design" / "Table curation".
 * <ul>
 *   <li><b>Row-concat mode</b> ({@code source_type='row_concat'}): concatenates an included
 *   entity-grain dimension table's string columns per row ({@code col: value | col: value}),
 *   naive-chunks the result.</li>
 *   <li><b>Document-blob mode</b> (one {@code source_type} value per source, e.g.
 *   {@code nist_control_description}): runs a single prose column through
 *   {@link SemanticTextChunker#chunkPlainText}, the same sentence-boundary-aware chunker
 *   SEC's own document-chunk mode uses, reused as-is via its plain-text entry point (no HTML
 *   structure needed for a flat blob column).</li>
 * </ul>
 *
 * <p>This class does the organizing only -- embeddings are a separate, later, time-boxed stage.
 * {@link #sweep} lands chunks in {@code vc_staging} (Postgres), which is where {@code
 * vss-local.py} reads them from to produce the quantized codes. Java's job stops at organizing
 * text into chunk rows; it never touches embeddings.
 *
 * <p>No copy of the chunk text is published to the lake, deliberately. {@code chunk_id} is
 * {@code <source_schema>:<source_table>:<stringified_fk>:<sequence>}, so a search hit already
 * names the source table and the row's key: a consumer resolves it against that table, which is
 * in the lake, documented, and the authority on its own text. Copying the text out would put a
 * second, staler answer next to it.
 *
 * <p>Chunking ({@link #sweep}) organizes text into rows and lands them in {@code vc_staging}/
 * {@code vc_tombstones} (see {@code govdata/scripts/sql/vc_schema.sql}). Each source row is a
 * "parent"; a SHA-256 hash of its pre-chunk text ({@code parent_hash}) is compared against what
 * is already staged for that parent. An unchanged hash is a no-op (the idempotency check); a
 * changed or new hash moves every existing row for that parent into {@code vc_tombstones} (an
 * append-only change log -- not itself drained by this class, but available to whatever
 * downstream stage needs to know what changed since it last ran) and inserts the freshly computed
 * set -- always the WHOLE parent's chunk set, never a partial chunk-level upsert, which is what
 * makes a content-addressed chunk key unnecessary here (see vc_schema.sql's design note).
 *
 * <p>{@code vc_staging}'s own durability (independent of the Iceberg materialization above) is a
 * plain {@code pg_dump} backup to object storage (see {@code govdata/scripts/vc_pg_dump.sh}) --
 * chunking is cheap and deterministic from source tables, so the backup exists purely to avoid
 * redoing that work after a PG loss, not because the data is otherwise irreplaceable.
 *
 * <p><b>Not wired into any schema's {@code hooks.tableLifecycleListener} -- this is a standalone
 * job, invoked only via {@link #main}/{@link #sweep} by {@code x-schema.sh} on its own schedule.
 * </b> Per the cross-schema separation-of-concerns principle (schema ETL runs operate only on
 * self-contained elements; cross-schema derivations run in one separate job after daily ETL), no
 * schema's own build may be what decides when this runs.
 *
 * <p>Change tracking is watermark- and version-based, so "find what changed" and "redo a prior
 * run" are the same operation -- no separate backout/removal tool is part of the design (see
 * {@link #CHUNKER_VERSION}, {@link #sourceNeedsSweep}): a source whose own {@code
 * pipeline_tracker.table_completion.completed_at} hasn't advanced since the last sweep is skipped
 * entirely (the coarse layer); within a swept source, a parent whose {@code parent_hash} (which
 * folds in {@link #CHUNKER_VERSION}) is unchanged is a no-op (the fine layer). Bumping {@link
 * #CHUNKER_VERSION} after a chunking-logic fix invalidates every stored hash at once, so the next
 * normal sweep reprocesses everyone with no special-casing.
 */
public class ChunkOrganizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkOrganizer.class);

  // Bump after any change to the chunking logic itself (CHUNK_SIZE/CHUNK_OVERLAP, chunkFixed,
  // SemanticTextChunker's settings, the row-concat text-building rules) so every existing
  // parent_hash is invalidated and the next sweep reprocesses every parent with no separate
  // removal step -- see the class javadoc's change-tracking paragraph. Also bump when the set of
  // columns written to vc_staging widens (e.g. FK columns), since parent_hash covers only the text
  // and an unchanged parent is otherwise never rewritten with the new columns.
  private static final int CHUNKER_VERSION = 2;

  // Naive fixed-window chunk size / overlap over the delimited column concatenation (row-concat
  // mode only -- document-blob mode uses SemanticTextChunker's own target/min/max sizing).
  // Matches the plan's stated default; tunable, not tied to any model's token limit here
  // since row-concat sources are short (reference/dimension rows).
  private static final int CHUNK_SIZE = 1000;
  private static final int CHUNK_OVERLAP = 200;

  /** One row-concat source: an included entity-grain dimension table, per the v1 registry
   *  in semantic-search-plan.md's "Table curation". Add an entry here to onboard a new source --
   *  its FK columns on vc_staging are created by {@link #ensureVcSchema} and written by {@link
   *  #insertParentRows} from the registry, so no other code or schema change is needed. */
  private static final List<RowConcatSource> ROW_CONCAT_SOURCES = Arrays.asList(
      new RowConcatSource("ref", "naics", Arrays.asList("naics_code"),
          Arrays.asList("naics_code", "naics_title"), "ref_naics_code"),
      // stringColumns lists only columns declared in the schema's own `columns:` block --
      // NOT `type`, which is a synthesized Hive-partition dimension marker (declared under
      // `dimensions:`, not `columns:`) on both sources below. Constant-per-row and carries
      // no real content, so it's a structural exclusion (same category as excluding a
      // non-string column like `level`/`as_of`), not the column-level content judgment the
      // plan rejected.
      new RowConcatSource("ref", "naics_vintage", Arrays.asList("vintage", "naics_code"),
          Arrays.asList("naics_code", "naics_title"), null),
      // ticker is part of the PK, not just content: a cik can carry multiple tickers
      // (multiple share classes) on the same as_of -- see the primaryKey comment in
      // ref-schema.yaml's constraints block.
      new RowConcatSource("ref", "sec_company_tickers",
          Arrays.asList("type", "as_of", "cik", "ticker"),
          Arrays.asList("cik", "ticker", "title"), null),
      // Cross-schema source (fedregister, not ref) -- proves the base-path resolution
      // (context.getSchemaContext().getMaterializeDirectory() is the bucket root, not
      // ref-scoped) works for any schema, same as EntityBridgeListener already relies on.
      new RowConcatSource("fedregister", "fr_documents", Arrays.asList("document_number"),
          Arrays.asList("document_number", "title", "doc_type", "publication_date",
              "effective_on", "action", "agency_names", "cfr_references", "rin",
              "docket_ids", "signing_date"), "fedregister_document_number"),
      // semantic-search-plan.md "Table curation": entity-grain (one carrier), size is
      // irrelevant per the plan's own fmcsa_carriers-vs-insider_transactions counter-example.
      new RowConcatSource("transport", "fmcsa_carriers", Arrays.asList("dot_number"),
          Arrays.asList("dot_number", "carrier_name", "dba_name", "phy_city", "state_abbr",
              "phy_zip", "business_org", "classdef", "operation_type", "safety_rating",
              "hazmat_flag", "status_code"), "fmcsa_dot_number"),
      // Row-concat mode covers only patent_title here -- patent_abstracts/patent_claims/
      // patent_summaries/trademark_statement are separate tables, handled below in
      // document-blob mode; including their content here too would double-count it under two
      // source_types (semantic-search-plan.md's "Table curation" note).
      new RowConcatSource("patents", "patent_grants", Arrays.asList("patent_id"),
          Arrays.asList("patent_id", "patent_title"), "patents_patent_id"),
      // PK is the OpenFEMA row UUID (id), NOT disaster_number -- disaster_number is a nullable,
      // non-unique join key (confirmed against the schema's own constraints-block comment), not
      // this table's actual identity.
      new RowConcatSource("disasters", "disaster_declarations", Arrays.asList("id"),
          Arrays.asList("fema_declaration_string", "declaration_type", "declaration_title",
              "incident_type", "state_fips", "county_fips", "designated_area"), null),
      new RowConcatSource("disasters", "public_assistance_projects", Arrays.asList("hash"),
          Arrays.asList("application_title", "applicant_id", "incident_type",
              "damage_category_code", "damage_category", "project_status", "project_size",
              "state_fips", "county_fips", "county_name", "state_abbr"), null),
      // year is a Hive-partition dimension column (declared under dimensions:, not columns:),
      // same category as ref.sec_company_tickers' own "type" -- proven to resolve correctly via
      // iceberg_scan by that existing registration, not a new assumption here.
      new RowConcatSource("geo", "rural_urban_continuum",
          Arrays.asList("county_fips", "year"),
          Arrays.asList("county_fips", "state_fips", "county_name", "rucc_description",
              "metro_nonmetro"), null),
      // tract_fips_20 (NOT tract_fips) -- the schema's own constraints block declares this
      // table's PK/FK as "tract_fips", but no column by that name exists on the table; the real
      // column is tract_fips_20 (confirmed against the table's own columns: block; a separate
      // schema-authoring bug worth fixing independently, not something to replicate here).
      new RowConcatSource("geo", "ruca_codes",
          Arrays.asList("tract_fips_20", "year"),
          Arrays.asList("tract_fips_20", "state_fips_20", "county_fips_20",
              "primary_ruca_description"), null),
      new RowConcatSource("officials", "federal_judges", Arrays.asList("jid"),
          federalJudgeStringColumns(), null),
      // SEC is a contributor like any other -- chunked here, in the centralized sweep, not by a
      // per-schema writer during SEC's own ETL run (that pattern -- materializeSecChunksToRef,
      // removed from SecSchemaFactory.java -- was exactly the per-table-hook coupling this
      // pipeline's cross-schema separation-of-concerns design exists to avoid). No special
      // multi-table grouped chunker needed: these are ordinary per-row text columns, same shape
      // as every other row-concat source.
      // Supreme Court slip opinions. A source table gets one registration, so the case name and the
      // Court's own one-line holding go in the same row-concat as the opinion text (syllabus,
      // opinion of the Court, concurrences, dissents): the first chunk carries name and holding.
      // Fixed 1,000-char windows instead of the semantic chunker -- accepted for that gain.
      // Composite PK (decision_date, listing_docket): us_citation is null for 11 of 584 DQ rows
      // and shared by consolidated cases, so it is not a key.
      new RowConcatSource("law", "scotus_slip_opinions",
          Arrays.asList("decision_date", "listing_docket"),
          Arrays.asList("case_name", "holding_summary", "opinion_text"), null),
      // Bill title plus the Congressional Record's constitutional-authority statement, which is
      // stored as <pre>/<a> markup after a Congressional Record header line. A bill with no
      // statement (45% of the DQ sample) still gets its title embedded.
      new RowConcatSource("law", "bills",
          Arrays.asList("congress", "bill_type", "bill_number"),
          Arrays.asList("title", "constitutional_authority_statement"), null,
          Collections.singleton("constitutional_authority_statement")),
      // Amendment purpose/description: short, and filled for only ~10% of amendments, so a
      // row with both null builds an empty text and produces no chunk.
      new RowConcatSource("law", "bill_amendments",
          Arrays.asList("congress", "amendment_type", "amendment_number"),
          Arrays.asList("purpose", "description"), null),
      new RowConcatSource("sec", "mda_sections",
          Arrays.asList("cik", "accession_number", "section", "paragraph_number"),
          Arrays.asList("section", "subsection", "paragraph_text"), null),
      // Same pre-split-by-SEC's-own-ETL shape as mda_sections above; disabled at the source
      // (hooks.enabled: false in sec-schema.yaml) until D-178's document-mis-partitioning fix
      // lands, so this entry is inert (zero completed rows to sweep) until then, not a new
      // risk on its own.
      new RowConcatSource("sec", "risk_factor_sections",
          Arrays.asList("cik", "accession_number", "section", "paragraph_number"),
          Arrays.asList("section", "subsection", "paragraph_text"), null),
      new RowConcatSource("sec", "earnings_transcripts",
          Arrays.asList("cik", "accession_number", "section_type", "paragraph_number"),
          Arrays.asList("section_type", "speaker_name", "speaker_role", "paragraph_text"),
          null));

  /** federal_judges' content columns: 6 base identity fields plus 12 fields repeated across 6
   *  numbered appointment groups (a judge can hold up to 6 distinct court appointments) --
   *  generated rather than hand-transcribed to avoid a 78-entry literal list. Per
   *  semantic-search-plan.md's "column-level filtering was rejected" rule, every declared
   *  string column goes in, including short categorical ones (aba_rating_N, party_of_
   *  appointing_president_N) -- the plan's own judgment call, not a chunk-parsing decision. */
  private static List<String> federalJudgeStringColumns() {
    List<String> cols = new ArrayList<String>(Arrays.asList(
        "last_name", "first_name", "middle_name", "suffix", "gender", "race_or_ethnicity"));
    String[] perGroup = {"court_type", "court_name", "appointment_title", "appointing_president",
        "party_of_appointing_president", "aba_rating", "nomination_date", "confirmation_date",
        "commission_date", "senior_status_date", "termination_date", "termination_reason"};
    for (int group = 1; group <= 6; group++) {
      for (String field : perGroup) {
        cols.add(field + "_" + group);
      }
    }
    return cols;
  }

  /** A chunking function turns one row's blob text into an ordered list of pieces -- this is
   *  CHUNK PARSING, not doc parsing/sectioning (see the class javadoc's terminology paragraph):
   *  it operates on text a source's own ETL has already extracted into one atomic column, and
   *  never itself decides what counts as a "section". Pluggable per {@link DocumentBlobSource}
   *  so a future source needing a different chunker (see the SEC note on {@link GenericChunk})
   *  is a new registry entry, not a new code path in {@link #chunkDocumentBlobSource}. */
  @FunctionalInterface
  interface ChunkFunction {
    List<GenericChunk> chunk(String text);
  }

  /** One chunker-agnostic output piece. {@code paragraphContinuation} is {@code null} when a
   *  chunker doesn't have the concept (only {@link SemanticTextChunker} does today). */
  static final class GenericChunk {
    final String text;
    final int sequenceNumber;
    final Boolean paragraphContinuation;

    GenericChunk(String text, int sequenceNumber, Boolean paragraphContinuation) {
      this.text = text;
      this.sequenceNumber = sequenceNumber;
      this.paragraphContinuation = paragraphContinuation;
    }
  }

  /** The chunker every current document-blob source uses: {@link SemanticTextChunker}'s
   *  sentence-boundary-aware plain-text splitter (target/min/max sizing per its DEFAULT_*
   *  constants -- these are short-to-medium prose fields, not full filings, so the SEC defaults
   *  apply without retuning), 1-based sequence numbers renumbered to 0-based to match this
   *  table's convention. SEC's own text (mda_sections, earnings_transcripts) doesn't use this --
   *  those tables are already pre-split into one row per paragraph by SEC's own ETL, so they're
   *  registered as ordinary {@link RowConcatSource}s instead (see {@link #ROW_CONCAT_SOURCES}),
   *  same shape as every other source. */
  private static final ChunkFunction SEMANTIC_TEXT_CHUNKER = text -> {
    List<SemanticTextChunker.Chunk> chunks = new SemanticTextChunker().chunkPlainText(text);
    List<GenericChunk> result = new ArrayList<GenericChunk>(chunks.size());
    for (SemanticTextChunker.Chunk c : chunks) {
      result.add(new GenericChunk(c.getText(), c.getSequenceNumber() - 1,
          c.isParagraphContinuation()));
    }
    return result;
  };

  private static final Pattern HTML_BLOCK_TAG =
      Pattern.compile("(?i)</?(?:p|div|li|ul|ol|pre|br|tr|h[1-6])\\b[^>]*>");
  private static final Pattern HTML_TAG = Pattern.compile("<[^>]+>");
  private static final Pattern HTML_NUMERIC_ENTITY = Pattern.compile("&#(\\d{1,5});");
  private static final Pattern BLANK_RUN = Pattern.compile("[\\s\\u00a0]+");

  /** Turns an HTML fragment into plain text: block-level tags become line breaks, every other
   *  tag is dropped, and the character entities the source actually uses ({@code &nbsp; &amp;
   *  &lt; &gt; &quot; &apos;} and numeric {@code &#NNN;}) are decoded. An entity outside that
   *  set is left in the text as written rather than guessed at. {@code &amp;} is decoded last so
   *  an escaped entity ({@code &amp;lt;}) is not decoded twice. */
  static String stripHtml(String html) {
    String s = HTML_BLOCK_TAG.matcher(html).replaceAll("\n");
    s = HTML_TAG.matcher(s).replaceAll("");
    s = s.replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", "\"").replace("&apos;", "'");
    Matcher numeric = HTML_NUMERIC_ENTITY.matcher(s);
    StringBuffer decoded = new StringBuffer();
    while (numeric.find()) {
      numeric.appendReplacement(decoded,
          Matcher.quoteReplacement(String.valueOf((char) Integer.parseInt(numeric.group(1)))));
    }
    numeric.appendTail(decoded);
    s = decoded.toString().replace("&amp;", "&");
    StringBuilder out = new StringBuilder(s.length());
    for (String line : s.split("\n", -1)) {
      String collapsed = BLANK_RUN.matcher(line).replaceAll(" ").trim();
      if (collapsed.isEmpty()) {
        continue;
      }
      if (out.length() > 0) {
        out.append('\n');
      }
      out.append(collapsed);
    }
    return out.toString();
  }

  /** For sources whose blob column holds HTML markup (CRS bill summaries, the Congressional
   *  Record's constitutional-authority statements): strips the markup, then chunks the text like
   *  every other document blob. Markup is not text and would otherwise be embedded as tokens. */
  private static final ChunkFunction HTML_STRIPPING_CHUNKER =
      text -> SEMANTIC_TEXT_CHUNKER.chunk(stripHtml(text));

  /** Document-blob sources per semantic-search-plan.md's "Document-blob sources" table.
   *  Each gets its own source_type value; all three use {@link #SEMANTIC_TEXT_CHUNKER} today,
   *  but the chunker is a per-source field precisely so a future source needing a different one
   *  is a new entry here, not a new branch in {@link #chunkDocumentBlobSource}. */
  private static final List<DocumentBlobSource> DOCUMENT_BLOB_SOURCES = Arrays.asList(
      new DocumentBlobSource("cyber_threat", "nist_controls", Arrays.asList("control_id"),
          "description", "nist_control_description", null, SEMANTIC_TEXT_CHUNKER),
      new DocumentBlobSource("cyber_threat", "cis_controls", Arrays.asList("safeguard_id"),
          "description", "cis_control_description", null, SEMANTIC_TEXT_CHUNKER),
      new DocumentBlobSource("cyber_threat", "owasp_top10", Arrays.asList("entry_id"),
          "overview", "owasp_entry_overview", null, SEMANTIC_TEXT_CHUNKER),
      new DocumentBlobSource("patents", "patent_abstracts", Arrays.asList("patent_id"),
          "patent_abstract", "patent_abstract", null, SEMANTIC_TEXT_CHUNKER),
      // Composite PK (patent_id, claim_sequence) -- one patent has many claims.
      new DocumentBlobSource("patents", "patent_claims",
          Arrays.asList("patent_id", "claim_sequence"),
          "claim_text", "patent_claim", null, SEMANTIC_TEXT_CHUNKER),
      new DocumentBlobSource("patents", "patent_summaries", Arrays.asList("patent_id"),
          "summary_text", "patent_summary", null, SEMANTIC_TEXT_CHUNKER),
      // Composite PK (serial_no, statement_type_cd) -- one trademark can carry several
      // statement types (of which "goods/services description" is one).
      new DocumentBlobSource("patents", "trademark_statement",
          Arrays.asList("serial_no", "statement_type_cd"),
          "statement_text", "trademark_statement", null, SEMANTIC_TEXT_CHUNKER),
      // Composite PK (type, nct_id) -- "type" is a Hive-partition dimension column here (not a
      // hand-declared columns: entry), same category as ref.sec_company_tickers' own "type".
      // brief_summary is truncated to 2000 chars at the source/ETL level.
      new DocumentBlobSource("health", "clinical_trials", Arrays.asList("type", "nct_id"),
          "brief_summary", "clinical_trial_summary", null, SEMANTIC_TEXT_CHUNKER),
      // Composite PK (type, nct_id, intervention_name) -- one trial has many interventions.
      // Unlike clinical_trials above, "type" IS a real declared columns: entry on this table
      // (confirmed against the schema directly). description is truncated to 2000 chars.
      new DocumentBlobSource("health", "clinical_trial_interventions",
          Arrays.asList("type", "nct_id", "intervention_name"),
          "description", "clinical_trial_intervention", null, SEMANTIC_TEXT_CHUNKER),
      // Composite PK (event_id, aircraft_key) -- one accident event can involve several
      // aircraft. probable_cause is truncated to 4000 chars at the source/ETL level.
      new DocumentBlobSource("transport", "ntsb_aviation_accidents",
          Arrays.asList("event_id", "aircraft_key"),
          "probable_cause", "ntsb_probable_cause", null, SEMANTIC_TEXT_CHUNKER),
      // The U.S. Code, embedded at subsection grain: usc_subsections is each section cut at its
      // own structural boundaries into units of at most 2,000 characters, which is under the
      // semantic chunker's 2,200-character maximum, so each unit becomes exactly one chunk and no
      // paragraph is cut. usc_sections (the whole section, for reading) is deliberately not
      // registered: that would embed every statute twice. Composite PK (title_number,
      // section_number, section_seq, unit_seq) -- the Code numbers a few sections twice within a
      // title, so section_number alone is not unique.
      new DocumentBlobSource("law", "usc_subsections",
          Arrays.asList("title_number", "section_number", "section_seq", "unit_seq"),
          "unit_text", "usc_subsection", null, SEMANTIC_TEXT_CHUNKER),
      // Composite PK (volume, first_page): unique across the DQ sample (90 of 90).
      new DocumentBlobSource("law", "scotus_reports_cases",
          Arrays.asList("volume", "first_page"),
          "opinion_text", "scotus_reports_opinion", null, SEMANTIC_TEXT_CHUNKER),
      // CRS summary, stored as HTML (p/strong/em/ol/ul/li/a and &nbsp;/&amp; entities).
      new DocumentBlobSource("law", "bill_summaries",
          Arrays.asList("congress", "bill_type", "bill_number", "version_code"),
          "summary_text", "bill_summary", null, HTML_STRIPPING_CHUNKER),
      // Composite PK (filing_uuid, activity_seq): the filer's free-text statement of the specific
      // issues lobbied, which names bills and programs.
      new DocumentBlobSource("law", "lobbying_activities",
          Arrays.asList("filing_uuid", "activity_seq"),
          "description", "lobbying_activity_description", null, SEMANTIC_TEXT_CHUNKER));

  // ========================================================================
  // Standalone sweep entry point -- invoked by x-schema.sh, not by any schema's own ETL
  // ========================================================================

  /** One sweep over every registered source: skip a source entirely if its own {@code
   *  table_completion.completed_at} hasn't advanced since this source was last swept (the
   *  coarse watermark), otherwise chunk it and hand the rows to {@link #writeToPgStaging} (which
   *  applies the fine-grained {@code parent_hash} skip/tombstone/replace per parent). {@code
   *  pg} must already have its search_path set to the target namespace and {@link
   *  #ensureVcSchema} already applied -- both {@link #main} and a future caller sharing one
   *  connection across multiple sweeps are expected to do that once, not per source. */
  static void sweep(Connection duckdb, Connection pg, String base) throws SQLException {
    sweep(duckdb, pg, base, 0);
  }

  /** As above, but caps each source at {@code maxRowsPerSource} rows ({@code <= 0} = unlimited,
   *  the normal production sweep) -- see {@link #main}'s {@code
   *  CHUNK_ORGANIZER_MAX_ROWS_PER_SOURCE}. */
  static void sweep(Connection duckdb, Connection pg, String base, int maxRowsPerSource)
      throws SQLException {
    LOGGER.info("ChunkOrganizer sweep: checking {} row-concat + {} document-blob source(s)",
        ROW_CONCAT_SOURCES.size(), DOCUMENT_BLOB_SOURCES.size());
    int swept = 0;
    int skipped = 0;
    for (RowConcatSource src : ROW_CONCAT_SOURCES) {
      if (!sourceNeedsSweep(pg, src.sourceTable)) {
        skipped++;
        continue;
      }
      // Narrow the rescan to just the years that actually changed, when the source's own ETL
      // pipeline tracks completion per year -- see selectChangedYears' javadoc. null means no
      // per-year data exists for this source (most reference tables), so chunkRowConcatSource
      // falls back to its original unfiltered full scan, unchanged from before this.
      Long lastSwept = selectLastSweptCompletedAt(pg, src.sourceTable);
      Set<Integer> changedYears = selectChangedYears(pg, src.sourceTable,
          lastSwept != null ? lastSwept : 0L);
      if (changedYears != null) {
        LOGGER.info("ChunkOrganizer: {}.{} has per-year tracking -- scoping rescan to {} "
            + "changed year(s): {}", src.sourceSchema, src.sourceTable, changedYears.size(),
            changedYears);
      }
      chunkRowConcatSource(duckdb, pg, base, src, maxRowsPerSource, changedYears);
      markSwept(pg, src.sourceSchema, src.sourceTable);
      swept++;
    }
    for (DocumentBlobSource src : DOCUMENT_BLOB_SOURCES) {
      if (!sourceNeedsSweep(pg, src.sourceTable)) {
        skipped++;
        continue;
      }
      Long lastSweptBlob = selectLastSweptCompletedAt(pg, src.sourceTable);
      Set<Integer> changedYearsBlob = selectChangedYears(pg, src.sourceTable,
          lastSweptBlob != null ? lastSweptBlob : 0L);
      if (changedYearsBlob != null) {
        LOGGER.info("ChunkOrganizer: {}.{} has per-year tracking -- scoping rescan to {} "
            + "changed year(s): {}", src.sourceSchema, src.sourceTable, changedYearsBlob.size(),
            changedYearsBlob);
      }
      chunkDocumentBlobSource(duckdb, pg, base, src, maxRowsPerSource, changedYearsBlob);
      markSwept(pg, src.sourceSchema, src.sourceTable);
      swept++;
    }
    LOGGER.info("ChunkOrganizer sweep complete: {} source(s) swept, {} unchanged (skipped)",
        swept, skipped);
  }


  /** True if {@code sourceTable}'s own {@code pipeline_tracker.table_completion.completed_at}
   *  has advanced since the last sweep that actually rescanned it (or it has never been swept).
   *  An unchanged completed_at means nothing in that source has changed at all since last time
   *  -- the coarse layer that lets a sweep skip a source without even querying its rows. */
  static boolean sourceNeedsSweep(Connection pg, String sourceTable) throws SQLException {
    Long completedAt = selectTableCompletedAt(pg, sourceTable);
    if (completedAt == null) {
      // Never completed an ETL run at all (yet) -- nothing to sweep regardless of watermark.
      return false;
    }
    Long lastSwept = selectLastSweptCompletedAt(pg, sourceTable);
    return lastSwept == null || completedAt > lastSwept;
  }

  static Long selectTableCompletedAt(Connection pg, String sourceTable)
      throws SQLException {
    try (PreparedStatement ps = pg.prepareStatement(
        "SELECT completed_at FROM table_completion WHERE pipeline_name = ?")) {
      ps.setString(1, sourceTable);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : null;
      }
    }
  }

  static Long selectLastSweptCompletedAt(Connection pg, String sourceTable)
      throws SQLException {
    try (PreparedStatement ps = pg.prepareStatement(
        "SELECT last_swept_completed_at FROM vc_sync_state "
        + "WHERE source_table = ? LIMIT 1")) {
      ps.setString(1, sourceTable);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : null;
      }
    }
  }

  static void markSwept(Connection pg, String sourceSchema, String sourceTable)
      throws SQLException {
    Long completedAt = selectTableCompletedAt(pg, sourceTable);
    long watermark = completedAt != null ? completedAt : System.currentTimeMillis();
    try (PreparedStatement ps = pg.prepareStatement(
        "INSERT INTO vc_sync_state (source_schema, source_table, last_swept_completed_at) "
        + "VALUES (?, ?, ?) "
        + "ON CONFLICT (source_schema, source_table) "
        + "DO UPDATE SET last_swept_completed_at = EXCLUDED.last_swept_completed_at")) {
      ps.setString(1, sourceSchema);
      ps.setString(2, sourceTable);
      ps.setLong(3, watermark);
      ps.executeUpdate();
    }
    pg.commit();
  }

  /** Narrows a full-table rescan (triggered by {@link #sourceNeedsSweep}'s coarse, whole-table
   *  watermark) down to just the years that actually changed, when the ETL pipeline that writes
   *  {@code sourceTable} tracks completion per year -- the same {@code table_completion} table
   *  already carries per-year rows for those pipelines, named {@code
   *  <sourceTable>#iceberg-accession-sync#year=<year>}, alongside the bare whole-table row {@link
   *  #sourceNeedsSweep} reads. No new tracker needed; this data already exists, just wasn't being
   *  read this way. Returns {@code null} (meaning "no per-year data -- caller must full-scan
   *  unfiltered") for sources whose pipeline never writes per-year completion rows (most
   *  reference/dimension tables); otherwise returns exactly the years whose own {@code
   *  completed_at} has advanced past {@code lastSweptWatermark}, which may be empty if the coarse
   *  check's own watermark read raced a commit between the two queries -- correct to fall through
   *  to a full unfiltered scan in that case too, never to silently skip everything.
   *
   *  Confirmed live 2026-09-16: sec.risk_factor_sections' coarse watermark correctly flagged the
   *  whole table as changed (today's targeted #29/#240 backfill touched a few years), but without
   *  this the fix would have re-hashed all 8.7M rows across all 18 years to confirm 13 of them
   *  never changed -- observed as 4h39m of continuous "0 replaced" batches before this existed. */
  static Set<Integer> selectChangedYears(Connection pg, String sourceTable, long lastSweptWatermark)
      throws SQLException {
    String prefix = sourceTable + "#iceberg-accession-sync#year=";
    Set<Integer> changedYears = new HashSet<Integer>();
    boolean anyYearRows = false;
    try (PreparedStatement ps = pg.prepareStatement(
        "SELECT pipeline_name, completed_at FROM table_completion WHERE pipeline_name LIKE ?")) {
      ps.setString(1, prefix + "%");
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          anyYearRows = true;
          String pipelineName = rs.getString(1);
          long completedAt = rs.getLong(2);
          String yearSuffix = pipelineName.substring(prefix.length());
          try {
            int year = Integer.parseInt(yearSuffix);
            if (completedAt > lastSweptWatermark) {
              changedYears.add(year);
            }
          } catch (NumberFormatException e) {
            // Matched the prefix but the suffix isn't a bare year -- not this naming
            // convention (a different sync mechanism reusing a similar prefix); ignore it
            // rather than let a parse failure abort the whole sweep.
            LOGGER.debug("ChunkOrganizer: ignoring non-year table_completion entry '{}' for {}",
                pipelineName, sourceTable);
          }
        }
      }
    }
    return anyYearRows ? changedYears : null;
  }

  private static String ensureSocketTimeout(String jdbcUrl) {
    if (jdbcUrl.contains("socketTimeout")) {
      return jdbcUrl;
    }
    String separator = jdbcUrl.contains("?") ? "&" : "?";
    return jdbcUrl + separator + "socketTimeout=60000";
  }

  /** Standalone entry point for {@code x-schema.sh}. Reads {@code CALCITE_TRACKER_PG_URL}/
   *  {@code _USER}/{@code _PASSWORD} and {@code AWS_*} (all exempt from the model-operand guard
   *  as run/infra config -- see {@code .claude/hooks/model-operand-guard.py}'s EXEMPT_PREFIX)
   *  plus {@code GOVDATA_PARQUET_DIR} (explicitly exempt). Not reachable through any schema's
   *  model operand because this is not schema-model-driven code -- it is the standalone sweep
   *  job itself, run on its own schedule, independent of any one schema's ETL. */
  public static void main(String[] args) throws Exception {
    String jdbcUrl = System.getenv("CALCITE_TRACKER_PG_URL");
    if (jdbcUrl == null) {
      throw new IllegalStateException("CALCITE_TRACKER_PG_URL not set");
    }
    jdbcUrl = ensureSocketTimeout(jdbcUrl);
    String user = System.getenv("CALCITE_TRACKER_PG_USER");
    String password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
    String base = System.getenv("GOVDATA_PARQUET_DIR");
    if (base == null) {
      base = "s3://govdata-parquet-v1";
    }
    String ns = PGPipelineTracker.sanitizeNamespace(base);
    if (ns == null) {
      throw new IllegalStateException("cannot derive a PG namespace from '" + base + "'");
    }
    // Test-only knob: caps every source at N rows regardless of its real size, for a fast sweep
    // across every contributor at once. Unset (the normal production path) means unlimited --
    // see sweep(Connection, Connection, String, int)'s javadoc. A run/infra flag owned by the
    // launch script (x-schema.sh), same exemption category as CALCITE_TRACKER_PG_URL above.
    int maxRowsPerSource = 0;
    String maxRowsEnv = System.getenv("CHUNK_ORGANIZER_MAX_ROWS_PER_SOURCE");
    if (maxRowsEnv != null && !maxRowsEnv.isEmpty()) {
      maxRowsPerSource = Integer.parseInt(maxRowsEnv);
    }

    try (Connection pg = user != null ? DriverManager.getConnection(jdbcUrl, user, password)
            : DriverManager.getConnection(jdbcUrl);
         Connection duckdb = openDuckDbStandalone()) {
      pg.setAutoCommit(false);
      try (Statement stmt = pg.createStatement()) {
        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"" + ns + "\"");
        stmt.execute("SET search_path TO \"" + ns + "\"");
      }
      ensureVcSchema(pg);
      pg.commit();
      sweep(duckdb, pg, base, maxRowsPerSource);
    }
  }

  /** DuckDB connection for the standalone job: same setup as the old {@link #openDuckDb}, but
   *  S3 credentials come from {@code AWS_*} env vars (exempt, infra-layer config) instead of a
   *  {@code TableContext}'s {@code StorageProvider} -- there is no TableContext outside an
   *  actual schema ETL run. */
  static Connection openDuckDbStandalone() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=2");
      stmt.execute("SET memory_limit='2GB'");
      String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + "/chunk-organizer-duckdb";
      stmt.execute("SET temp_directory='" + tempDir + "'");
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        LOGGER.debug("Parquet extension already loaded or built-in");
      }
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
      stmt.execute("SET unsafe_enable_version_guessing = true");
    } catch (SQLException e) {
      LOGGER.warn("DuckDB Iceberg extension unavailable: {}", e.getMessage());
    }
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (accessKey != null && secretKey != null) {
      try (Statement stmt = conn.createStatement()) {
        String s3ConfigMap = accessKey + "|" + secretKey;
        configureS3FromEnv(stmt, accessKey, secretKey,
            System.getenv("AWS_ENDPOINT_OVERRIDE"),
            System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : "auto");
      }
    }
    return conn;
  }

  private static void configureS3FromEnv(Statement stmt, String accessKey, String secretKey,
      String endpoint, String region) throws SQLException {
    stmt.execute("INSTALL httpfs");
    stmt.execute("LOAD httpfs");
    stmt.execute("SET http_timeout=10000");
    stmt.execute("SET http_retries=2");
    stmt.execute("SET http_retry_wait_ms=500");
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

  // ========================================================================
  // Row-concat mode
  // ========================================================================

  /** Streams the source table in fixed-size batches (see {@link #queryRowsBatched} -- required
   *  for large entity-grain tables like {@code transport.fmcsa_carriers} (4.47M rows) or
   *  {@code patents.patent_claims} (tens of millions); the original one-shot query loaded the
   *  entire table into a Java List, an unbounded-memory pattern that OOM'd in practice once a
   *  table that size was registered, 2026-08-30), writing each batch to PG staging as it goes
   *  so peak memory stays O(batch size) regardless of table size. */
  private static void chunkRowConcatSource(Connection conn, Connection pg, String base,
      RowConcatSource src, int maxRowsPerSource) throws SQLException {
    chunkRowConcatSource(conn, pg, base, src, maxRowsPerSource, null);
  }

  /** As above, but when {@code changedYears} is non-null, scopes the scan to just those years'
   *  rows (a {@code year IN (...)} filter added to the underlying query) instead of the whole
   *  table -- see {@link #selectChangedYears}. {@code null} means no per-year data exists for
   *  this source (most reference tables) or the caller didn't compute it; behavior is then
   *  identical to before this parameter existed: an unfiltered full-table scan. An empty
   *  (non-null) set means per-year data exists but nothing changed since the last sweep for any
   *  tracked year -- also scans nothing, correct given the coarse watermark that triggered this
   *  call in the first place could itself be stale by the time this runs. */
  private static void chunkRowConcatSource(Connection conn, Connection pg, String base,
      RowConcatSource src, int maxRowsPerSource, Set<Integer> changedYears) throws SQLException {
    String loc = base + "/" + src.sourceSchema + "/" + src.sourceTable;
    // SELECT DISTINCT pk cols + string cols together: a column can be both (e.g. naics_code
    // is the PK and also carries real text), so query each column once, not once per role.
    List<String> selectCols = new ArrayList<String>(src.pkColumns);
    for (String c : src.stringColumns) {
      if (!selectCols.contains(c)) {
        selectCols.add(c);
      }
    }
    long[] totals = {0, 0}; // [chunkCount, sourceRowCount]
    // Defensive against a source table's own declared primary key not actually being unique --
    // confirmed live on fedregister.fr_documents (17 of 463,338 document_numbers reprinted
    // verbatim across two different months by GPO's own bulk XML, a genuine upstream artifact,
    // not an extraction bug -- see FedRegisterBulkXmlDataProvider). vc_staging's PK is
    // (source_schema, source_table, stringified_fk, sequence); without this guard, a second row
    // sharing the same source PK aborts the whole batch insert for every other row in it. Tracked
    // across the entire scan (not just one batch) since keyset pagination can split a duplicate
    // pair across page boundaries.
    Set<String> seenPk = new HashSet<String>();
    queryRowsBatched(conn, loc, selectCols, src.pkColumns, ROW_CONCAT_BATCH_SIZE, maxRowsPerSource,
        changedYears, batch -> {
      List<Map<String, Object>> chunkRows = new ArrayList<Map<String, Object>>();
      for (Map<String, Object> row : batch) {
        String pkValue = stringifyPk(row, src.pkColumns);
        if (!seenPk.add(pkValue)) {
          LOGGER.warn("ChunkOrganizer: {}.{} has a non-unique declared primary key -- dropping "
              + "duplicate row for {}={}", src.sourceSchema, src.sourceTable, src.pkColumns,
              pkValue);
          continue;
        }
        String text = buildRowConcatText(row, src.stringColumns, src.htmlColumns);
        String parentHash = sha256Hex(CHUNKER_VERSION + ":" + text);
        List<String> chunks = chunkFixed(text);
        for (int seq = 0; seq < chunks.size(); seq++) {
          Map<String, Object> chunkRow = new LinkedHashMap<String, Object>();
          chunkRow.put("chunk_id",
              src.sourceSchema + ":" + src.sourceTable + ":" + pkValue + ":" + seq);
          chunkRow.put("source_schema", src.sourceSchema);
          chunkRow.put("source_table", src.sourceTable);
          chunkRow.put("stringified_fk", pkValue);
          chunkRow.put("sequence", seq);
          chunkRow.put("parent_hash", parentHash);
          chunkRow.put("source_type", "row_concat");
          chunkRow.put("chunk_text", chunks.get(seq));
          chunkRow.put("enriched_text", chunks.get(seq));
          // Write FK columns for each PK component to enable direct SQL joins back to source rows
          putFkColumns(chunkRow, src.fkColumns, src.pkColumns, row);
          chunkRows.add(chunkRow);
        }
      }
      if (!chunkRows.isEmpty()) {
        writeToPgStaging(pg, chunkRows);
      }
      totals[0] += chunkRows.size();
      totals[1] += batch.size();
    });
    LOGGER.info("ChunkOrganizer: row-concat {}.{} -> {} chunks from {} rows",
        src.sourceSchema, src.sourceTable, totals[0], totals[1]);
  }

  /** Stringifies a (possibly composite) primary key as ':'-joined column values -- uniform
   *  handling for single-column and composite PKs alike, per semantic-search-plan.md's
   *  "Storage shape". */
  static String stringifyPk(Map<String, Object> row, List<String> pkColumns) {
    StringBuilder sb = new StringBuilder();
    for (String col : pkColumns) {
      if (sb.length() > 0) {
        sb.append(':');
      }
      sb.append(row.get(col));
    }
    return sb.toString();
  }

  /** Writes each PK component's value into its FK column, read from the source row itself. The
   *  values are not split back out of the ':'-joined stringified key: a key component that
   *  contains ':' would shift every later component into the wrong column. A null component stays
   *  SQL NULL rather than the text "null", which would never join back to the source. */
  static void putFkColumns(Map<String, Object> chunkRow, List<String> fkColumns,
      List<String> pkColumns, Map<String, Object> sourceRow) {
    for (int i = 0; i < Math.min(fkColumns.size(), pkColumns.size()); i++) {
      Object value = sourceRow.get(pkColumns.get(i));
      chunkRow.put(fkColumns.get(i), value == null ? null : value.toString());
    }
  }

  /** Builds 'col: value | col: value | ...' from a row's non-null values, in the given
   *  (source-declared) column order. No per-column inclusion/exclusion -- every listed
   *  column goes in if non-null, per semantic-search-plan.md's "no column-level filtering"
   *  rule. */
  static String buildRowConcatText(Map<String, Object> row, List<String> columns) {
    return buildRowConcatText(row, columns, Collections.<String>emptySet());
  }

  /** As above, but a column named in {@code htmlColumns} has its markup stripped first and is
   *  left out entirely if nothing but markup remains. */
  static String buildRowConcatText(Map<String, Object> row, List<String> columns,
      Set<String> htmlColumns) {
    StringBuilder sb = new StringBuilder();
    for (String col : columns) {
      Object val = row.get(col);
      if (val == null) {
        continue;
      }
      Object text = val;
      if (htmlColumns.contains(col)) {
        String stripped = stripHtml(val.toString());
        if (stripped.isEmpty()) {
          continue;
        }
        text = stripped;
      }
      if (sb.length() > 0) {
        sb.append(" | ");
      }
      sb.append(col).append(": ").append(text);
    }
    return sb.toString();
  }

  /** Naive fixed-window chunker with overlap. A short row-concat text (the common case)
   *  fits in one chunk -- see semantic-search-plan.md "Row-level design". */
  static List<String> chunkFixed(String text) {
    List<String> chunks = new ArrayList<String>();
    if (text == null || text.isEmpty()) {
      return chunks;
    }
    if (text.length() <= CHUNK_SIZE) {
      chunks.add(text);
      return chunks;
    }
    int step = CHUNK_SIZE - CHUNK_OVERLAP;
    int start = 0;
    while (start < text.length()) {
      chunks.add(text.substring(start, Math.min(start + CHUNK_SIZE, text.length())));
      if (start + CHUNK_SIZE >= text.length()) {
        break;
      }
      start += step;
    }
    return chunks;
  }

  // ========================================================================
  // Document-blob mode
  // ========================================================================

  /** Streams the source table in batches -- see {@link #chunkRowConcatSource}'s javadoc for
   *  why. Uses a smaller batch size than row-concat mode: {@link SemanticTextChunker} (or a
   *  future custom {@link ChunkFunction}) does real per-row work, not just a string split. */
  private static void chunkDocumentBlobSource(Connection conn, Connection pg, String base,
      DocumentBlobSource src, int maxRowsPerSource) throws SQLException {
    chunkDocumentBlobSource(conn, pg, base, src, maxRowsPerSource, null);
  }

  /** As above, but scoped to {@code changedYears} when non-null -- see {@link
   *  #chunkRowConcatSource}'s matching overload and {@link #selectChangedYears}; identical
   *  contract, just for the document-blob path (patents/health/cyber_threat/transport document
   *  text) rather than row-concat. */
  private static void chunkDocumentBlobSource(Connection conn, Connection pg, String base,
      DocumentBlobSource src, int maxRowsPerSource, Set<Integer> changedYears)
      throws SQLException {
    String loc = base + "/" + src.sourceSchema + "/" + src.sourceTable;
    List<String> selectCols = new ArrayList<String>(src.pkColumns);
    if (!selectCols.contains(src.blobColumn)) {
      selectCols.add(src.blobColumn);
    }
    long[] totals = {0, 0}; // [chunkCount, sourceRowCount]
    // See the matching guard in chunkRowConcatSource for why this is needed: a source table's
    // declared primary key is not a guarantee its data is actually unique.
    Set<String> seenPk = new HashSet<String>();
    queryRowsBatched(conn, loc, selectCols, src.pkColumns, DOCUMENT_BLOB_BATCH_SIZE,
        maxRowsPerSource, changedYears, batch -> {
      List<Map<String, Object>> chunkRows = new ArrayList<Map<String, Object>>();
      for (Map<String, Object> row : batch) {
        Object blobValue = row.get(src.blobColumn);
        if (blobValue == null) {
          continue;
        }
        String pkValue = stringifyPk(row, src.pkColumns);
        if (!seenPk.add(pkValue)) {
          LOGGER.warn("ChunkOrganizer: {}.{} has a non-unique declared primary key -- dropping "
              + "duplicate row for {}={}", src.sourceSchema, src.sourceTable, src.pkColumns,
              pkValue);
          continue;
        }
        String text = blobValue.toString();
        String parentHash = sha256Hex(CHUNKER_VERSION + ":" + text);
        List<GenericChunk> chunks = src.chunker.chunk(text);
        for (GenericChunk chunk : chunks) {
          int seq = chunk.sequenceNumber;
          Map<String, Object> chunkRow = new LinkedHashMap<String, Object>();
          chunkRow.put("chunk_id",
              src.sourceSchema + ":" + src.sourceTable + ":" + pkValue + ":" + seq);
          chunkRow.put("source_schema", src.sourceSchema);
          chunkRow.put("source_table", src.sourceTable);
          chunkRow.put("stringified_fk", pkValue);
          chunkRow.put("sequence", seq);
          chunkRow.put("parent_hash", parentHash);
          chunkRow.put("source_type", src.sourceType);
          chunkRow.put("chunk_text", chunk.text);
          chunkRow.put("enriched_text", chunk.text);
          chunkRow.put("paragraph_continuation", chunk.paragraphContinuation);
          // Write FK columns for each PK component to enable direct SQL joins back to source rows
          putFkColumns(chunkRow, src.fkColumns, src.pkColumns, row);
          chunkRows.add(chunkRow);
        }
      }
      if (!chunkRows.isEmpty()) {
        writeToPgStaging(pg, chunkRows);
      }
      totals[0] += chunkRows.size();
      totals[1] += batch.size();
    });
    LOGGER.info("ChunkOrganizer: document-blob {}.{} -> {} chunks from {} rows",
        src.sourceSchema, src.sourceTable, totals[0], totals[1]);
  }

  // ========================================================================
  // PG compute-layer write (vc_staging / vc_tombstones) -- see vc_schema.sql
  // ========================================================================

  /** Groups {@code chunkRows} by parent (source_schema, source_table, stringified_fk) and, for
   *  each parent, compares its {@code parent_hash} against what is already staged: unchanged
   *  means skip (idempotent no-op), changed or new means tombstone every existing row for that
   *  parent and insert the freshly computed set. {@code vc_staging} is the only durable copy of
   *  this data (backed up via plain {@code pg_dump}, not synced to Iceberg -- see the class
   *  javadoc); {@code vc_tombstones} is an append-only change log for whatever downstream
   *  consumer (the embeddings stage) needs to know what changed since it last ran.
   *  {@code pg} must already have its search_path set to the target namespace with {@link
   *  #ensureVcSchema} already applied -- callers own the connection lifecycle so one connection
   *  can be reused across an entire sweep instead of reopening per source.
   *
   *  <p>Every row in one call shares the same (source_schema, source_table) -- each caller scans
   *  exactly one registered source per invocation -- so the existing-hash lookup, tombstoning,
   *  and insert are each done as ONE bulk round-trip against the whole batch's parent set (via
   *  {@code = ANY(?)} array parameters) instead of one round-trip per parent. The original
   *  per-parent version cost 3-4 round-trips PER PARENT: at 20,000 parents/batch that's 60,000+
   *  round-trips and was the actual bottleneck (~23s/batch, ~870 rows/sec) -- confirmed live
   *  2026-08-31 the cost was Postgres round-trip chatter, not DuckDB read time or chunking CPU. */
  static void writeToPgStaging(Connection pg, List<Map<String, Object>> chunkRows)
      throws SQLException {
    if (chunkRows.isEmpty()) {
      return;
    }
    String sourceSchema = (String) chunkRows.get(0).get("source_schema");
    String sourceTable = (String) chunkRows.get(0).get("source_table");

    // Group by parent, preserving first-seen order for stable logging only (not semantically
    // required -- every row in a group shares one parent_hash by construction).
    Map<String, List<Map<String, Object>>> byParent =
        new LinkedHashMap<String, List<Map<String, Object>>>();
    for (Map<String, Object> row : chunkRows) {
      byParent.computeIfAbsent((String) row.get("stringified_fk"),
          k -> new ArrayList<Map<String, Object>>()).add(row);
    }

    Map<String, String> existingHashes =
        selectExistingParentHashes(pg, sourceSchema, sourceTable, byParent.keySet());

    List<String> changedFks = new ArrayList<String>();
    List<Map<String, Object>> toInsert = new ArrayList<Map<String, Object>>();
    int skipped = 0;
    for (Map.Entry<String, List<Map<String, Object>>> entry : byParent.entrySet()) {
      String stringifiedFk = entry.getKey();
      String newHash = (String) entry.getValue().get(0).get("parent_hash");
      if (newHash.equals(existingHashes.get(stringifiedFk))) {
        skipped++;
        continue;
      }
      changedFks.add(stringifiedFk);
      toInsert.addAll(entry.getValue());
    }

    if (!changedFks.isEmpty()) {
      tombstoneParents(pg, sourceSchema, sourceTable, changedFks);
      insertParentRows(pg, toInsert);
    }
    pg.commit();
    LOGGER.info("ChunkOrganizer: staged {} parent(s) ({} replaced, {} unchanged/skipped) "
        + "into vc_staging", byParent.size(), changedFks.size(), skipped);
  }

  /** Bulk-fetches every already-staged parent_hash for the given (source_schema, source_table)
   *  whose stringified_fk is in {@code stringifiedFks}, in ONE round-trip via a Postgres array
   *  parameter -- see the writeToPgStaging javadoc for why this replaced a per-parent query. */
  static Map<String, String> selectExistingParentHashes(Connection conn, String sourceSchema,
      String sourceTable, java.util.Collection<String> stringifiedFks) throws SQLException {
    Map<String, String> result = new HashMap<String, String>();
    String sql = "SELECT stringified_fk, parent_hash FROM vc_staging WHERE source_schema = ? "
        + "AND source_table = ? AND stringified_fk = ANY(?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, sourceSchema);
      ps.setString(2, sourceTable);
      ps.setArray(3, conn.createArrayOf("varchar", stringifiedFks.toArray()));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          result.put(rs.getString(1), rs.getString(2));
        }
      }
    }
    return result;
  }

  /** Bulk-tombstones every existing vc_staging row for the given (source_schema, source_table)
   *  whose stringified_fk is in {@code stringifiedFks}, in ONE round-trip pair (insert into
   *  vc_tombstones, then delete) via a Postgres array parameter regardless of how many parents
   *  changed -- see the writeToPgStaging javadoc. A no-op for any fk that has no existing rows
   *  (a genuinely new parent, not a changed one), which is fine: both statements simply match
   *  zero rows for it. */
  static void tombstoneParents(Connection conn, String sourceSchema, String sourceTable,
      List<String> stringifiedFks) throws SQLException {
    long now = System.currentTimeMillis();
    java.sql.Array fkArray = conn.createArrayOf("varchar", stringifiedFks.toArray());
    String insertTombstones = "INSERT INTO vc_tombstones "
        + "(source_schema, source_table, stringified_fk, sequence, chunk_id, tombstoned_at) "
        + "SELECT source_schema, source_table, stringified_fk, sequence, chunk_id, ? "
        + "FROM vc_staging WHERE source_schema = ? AND source_table = ? "
        + "AND stringified_fk = ANY(?)";
    try (PreparedStatement ps = conn.prepareStatement(insertTombstones)) {
      ps.setLong(1, now);
      ps.setString(2, sourceSchema);
      ps.setString(3, sourceTable);
      ps.setArray(4, fkArray);
      ps.executeUpdate();
    }
    String delete = "DELETE FROM vc_staging "
        + "WHERE source_schema = ? AND source_table = ? AND stringified_fk = ANY(?)";
    try (PreparedStatement ps = conn.prepareStatement(delete)) {
      ps.setString(1, sourceSchema);
      ps.setString(2, sourceTable);
      ps.setArray(3, fkArray);
      ps.executeUpdate();
    }
  }

  /** Column order/presence in each row's map is whatever the caller happened to {@code put} --
   *  this inserts by column NAME via a fixed statement, reading each via {@code row.get}
   *  (absent -> null), so callers never need to populate every column. */
  private static final List<String> VC_STAGING_COLUMNS = Arrays.asList(
      "source_schema", "source_table", "stringified_fk", "sequence", "chunk_id", "parent_hash",
      "source_type", "year", "cik", "accession_number", "filing_date", "section", "subsection",
      "section_path", "paragraph_continuation", "chunk_text", "enriched_text", "content_type",
      "financial_concepts", "exhibit_number", "speaker_name", "speaker_role", "paragraph_number",
      "ref_naics_code", "fedregister_document_number");

  /** Postgres truncates identifiers past this length, which would merge or misname FK columns. */
  private static final int PG_MAX_IDENTIFIER_LENGTH = 63;

  /** The fixed columns plus every registered source's FK columns -- the columns ensureVcSchema
   *  guarantees exist, and the ones chunkRowConcatSource/chunkDocumentBlobSource populate. */
  private static final List<String> VC_STAGING_INSERT_COLUMNS = vcStagingInsertColumns();

  private static List<String> vcStagingInsertColumns() {
    java.util.Set<String> cols = new java.util.LinkedHashSet<String>(VC_STAGING_COLUMNS);
    for (RowConcatSource src : ROW_CONCAT_SOURCES) {
      cols.addAll(src.fkColumns);
    }
    for (DocumentBlobSource src : DOCUMENT_BLOB_SOURCES) {
      cols.addAll(src.fkColumns);
    }
    for (String col : cols) {
      if (col.length() > PG_MAX_IDENTIFIER_LENGTH) {
        throw new IllegalStateException("ChunkOrganizer: vc_staging FK column '" + col + "' is "
            + col.length() + " characters; Postgres truncates identifiers past "
            + PG_MAX_IDENTIFIER_LENGTH + ", so it cannot be created or written under that name");
      }
    }
    return new ArrayList<String>(cols);
  }

  static void insertParentRows(Connection conn, List<Map<String, Object>> rows)
      throws SQLException {
    String sql = "INSERT INTO vc_staging (" + String.join(", ", VC_STAGING_INSERT_COLUMNS)
        + ", updated_at) VALUES (" + String.join(", ",
            java.util.Collections.nCopies(VC_STAGING_INSERT_COLUMNS.size() + 1, "?")) + ")";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      long now = System.currentTimeMillis();
      for (Map<String, Object> row : rows) {
        int i = 1;
        for (String col : VC_STAGING_INSERT_COLUMNS) {
          ps.setObject(i++, row.get(col));
        }
        ps.setLong(i, now);
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  /**
   * Fills the FK columns of already-staged rows from their {@code stringified_fk}, for rows staged
   * before {@link #insertParentRows} wrote them. Rows are touched only when every FK column of
   * their source is NULL and {@code stringified_fk} splits on ':' into exactly one part per PK
   * column; anything else (a key value containing ':', or an all-null key) is left for the normal
   * sweep, which derives the values from the source row. A part spelled {@code null} is written as
   * SQL NULL, matching how {@link #stringifyPk} rendered a null component. {@code updated_at} is
   * not touched, so the embedding backlog does not re-fetch these rows.
   *
   * @param onlySourceTable restrict to one source table, or null for every registered source
   * @param apply false only counts what would change
   * @param batchSize rows per UPDATE/commit
   * @return rows updated (or, when not applying, rows that would be updated)
   */
  static long backfillForeignKeys(Connection pg, String onlySourceTable, boolean apply,
      int batchSize) throws SQLException {
    long total = 0;
    boolean matched = false;
    for (RowConcatSource src : ROW_CONCAT_SOURCES) {
      if (onlySourceTable == null || onlySourceTable.equals(src.sourceTable)) {
        matched = true;
        total += backfillSourceForeignKeys(pg, src.sourceSchema, src.sourceTable, src.pkColumns,
            src.fkColumns, apply, batchSize);
      }
    }
    for (DocumentBlobSource src : DOCUMENT_BLOB_SOURCES) {
      if (onlySourceTable == null || onlySourceTable.equals(src.sourceTable)) {
        matched = true;
        total += backfillSourceForeignKeys(pg, src.sourceSchema, src.sourceTable, src.pkColumns,
            src.fkColumns, apply, batchSize);
      }
    }
    if (!matched) {
      throw new IllegalArgumentException("ChunkOrganizer: no registered source has table '"
          + onlySourceTable + "'");
    }
    return total;
  }

  private static long backfillSourceForeignKeys(Connection pg, String sourceSchema,
      String sourceTable, List<String> pkColumns, List<String> fkColumns, boolean apply,
      int batchSize) throws SQLException {
    int parts = Math.min(fkColumns.size(), pkColumns.size());
    StringBuilder allNull = new StringBuilder();
    StringBuilder assignments = new StringBuilder();
    for (int i = 0; i < parts; i++) {
      allNull.append(" AND ").append(fkColumns.get(i)).append(" IS NULL");
      assignments.append(i > 0 ? ", " : "").append(fkColumns.get(i))
          .append(" = NULLIF(split_part(stringified_fk, ':', ").append(i + 1).append("), 'null')");
    }
    String unpopulated = "source_schema = ? AND source_table = ?" + allNull;
    // An all-'null' key would stay NULL after the update and be selected again forever.
    String fixable = unpopulated
        + " AND cardinality(string_to_array(stringified_fk, ':')) = " + parts
        + " AND stringified_fk !~ '^null(:null)*$'";

    long fixableCount = countRows(pg, fixable, sourceSchema, sourceTable);
    long unpopulatedCount = countRows(pg, unpopulated, sourceSchema, sourceTable);
    LOGGER.info("ChunkOrganizer FK backfill {}.{}: {} row(s) fillable from stringified_fk, {} "
        + "unpopulated row(s) left for the sweep", sourceSchema, sourceTable, fixableCount,
        unpopulatedCount - fixableCount);
    if (!apply) {
      return fixableCount;
    }
    String update = "UPDATE vc_staging SET " + assignments + " WHERE ctid IN (SELECT ctid FROM "
        + "vc_staging WHERE " + fixable + " LIMIT ?)";
    long updatedTotal = 0;
    while (updatedTotal < fixableCount) {
      int updated;
      try (PreparedStatement ps = pg.prepareStatement(update)) {
        ps.setString(1, sourceSchema);
        ps.setString(2, sourceTable);
        ps.setInt(3, batchSize);
        updated = ps.executeUpdate();
      }
      pg.commit();
      if (updated == 0) {
        break;
      }
      updatedTotal += updated;
      LOGGER.info("ChunkOrganizer FK backfill {}.{}: {} / {}", sourceSchema, sourceTable,
          updatedTotal, fixableCount);
    }
    return updatedTotal;
  }

  private static long countRows(Connection pg, String where, String sourceSchema,
      String sourceTable) throws SQLException {
    try (PreparedStatement ps = pg.prepareStatement(
        "SELECT count(*) FROM vc_staging WHERE " + where)) {
      ps.setString(1, sourceSchema);
      ps.setString(2, sourceTable);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  static void ensureVcSchema(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS vc_staging ("
          + "  source_schema VARCHAR NOT NULL,"
          + "  source_table VARCHAR NOT NULL,"
          + "  stringified_fk VARCHAR NOT NULL,"
          + "  sequence BIGINT NOT NULL,"
          + "  parent_unit VARCHAR NOT NULL DEFAULT '',"
          + "  chunk_id VARCHAR NOT NULL,"
          + "  parent_hash VARCHAR NOT NULL,"
          + "  source_type VARCHAR NOT NULL,"
          + "  year INT,"
          + "  cik VARCHAR,"
          + "  accession_number VARCHAR,"
          + "  filing_date VARCHAR,"
          + "  section VARCHAR,"
          + "  subsection VARCHAR,"
          + "  section_path VARCHAR,"
          + "  paragraph_continuation BOOLEAN,"
          + "  chunk_text TEXT NOT NULL,"
          + "  enriched_text TEXT,"
          + "  content_type VARCHAR,"
          + "  financial_concepts VARCHAR,"
          + "  exhibit_number VARCHAR,"
          + "  speaker_name VARCHAR,"
          + "  speaker_role VARCHAR,"
          + "  paragraph_number BIGINT,"
          + "  ref_naics_code VARCHAR,"
          + "  fedregister_document_number VARCHAR,"
          + "  updated_at BIGINT NOT NULL,"
          + "  PRIMARY KEY (source_schema, source_table, stringified_fk, sequence)"
          + ")");
      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_vc_staging_updated_at "
          + "ON vc_staging (source_schema, source_table, updated_at)");
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS vc_tombstones ("
          + "  source_schema VARCHAR NOT NULL,"
          + "  source_table VARCHAR NOT NULL,"
          + "  stringified_fk VARCHAR NOT NULL,"
          + "  sequence BIGINT NOT NULL,"
          + "  chunk_id VARCHAR NOT NULL,"
          + "  tombstoned_at BIGINT NOT NULL,"
          + "  PRIMARY KEY (source_schema, source_table, stringified_fk, sequence, tombstoned_at)"
          + ")");
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS vc_sync_state ("
          + "  source_schema VARCHAR NOT NULL,"
          + "  source_table VARCHAR NOT NULL,"
          + "  last_swept_completed_at BIGINT NOT NULL DEFAULT 0,"
          + "  PRIMARY KEY (source_schema, source_table)"
          + ")");
      // A deployment that created vc_sync_state before last_swept_completed_at was added (this
      // session, 2026-08-30) has an existing table missing the column -- CREATE TABLE IF NOT
      // EXISTS above is a no-op against it. Same migration pattern PGPipelineTracker uses for
      // pipeline_tracker.source_as_of.
      stmt.execute("ALTER TABLE vc_sync_state ADD COLUMN IF NOT EXISTS "
          + "last_swept_completed_at BIGINT NOT NULL DEFAULT 0");
      // applied_at/last_synced_at existed only to mark a tombstone/schema as drained into
      // ref.vectorized_chunks -- dead columns now that sync-to-Iceberg was removed in favor of a
      // plain pg_dump backup of vc_staging (see the class javadoc). Dropped here, not just
      // omitted from the CREATE statements above, so an already-deployed instance converges too.
      stmt.execute("DROP INDEX IF EXISTS idx_vc_tombstones_pending");
      stmt.execute("ALTER TABLE vc_tombstones DROP COLUMN IF EXISTS applied_at");
      stmt.execute("ALTER TABLE vc_sync_state DROP COLUMN IF EXISTS last_synced_at");

      // Add FK columns for every registered source to support star-schema joins. Each column is
      // named {sourceSchema}_{sourceTable}_{pkColumn}, one per PK component. This allows direct
      // SQL joins from vc_staging back to source rows without stringified_fk string parsing.
      for (RowConcatSource src : ROW_CONCAT_SOURCES) {
        for (String fkCol : src.fkColumns) {
          stmt.execute("ALTER TABLE vc_staging ADD COLUMN IF NOT EXISTS "
              + fkCol + " VARCHAR");
        }
      }
      for (DocumentBlobSource src : DOCUMENT_BLOB_SOURCES) {
        for (String fkCol : src.fkColumns) {
          stmt.execute("ALTER TABLE vc_staging ADD COLUMN IF NOT EXISTS "
              + fkCol + " VARCHAR");
        }
      }
    }
  }

  static String sha256Hex(String text) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is a JDK-guaranteed algorithm (every JCE provider implements it); this is
      // unreachable in practice, not a real error condition.
      throw new IllegalStateException("SHA-256 unavailable", e);
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

  /** Row-concat mode's per-batch row count -- matches EntityBridgeListener's own
   *  MAX_ORG_MATCH_BATCH_SIZE precedent for the same "bound peak memory for one batch to a
   *  constant regardless of table size" problem. */
  private static final int ROW_CONCAT_BATCH_SIZE = 20_000;

  /** Document-blob mode's per-batch row count -- smaller than row-concat's because {@link
   *  SemanticTextChunker} (or a custom {@link ChunkFunction}) does real per-row CPU work, not
   *  just a string split. */
  private static final int DOCUMENT_BLOB_BATCH_SIZE = 5_000;

  @FunctionalInterface
  private interface BatchConsumer {
    void accept(List<Map<String, Object>> batch) throws SQLException;
  }

  /** Streams {@code loc}'s rows in fixed-size batches via keyset pagination ordered by {@code
   *  pkColumns} -- {@code WHERE (pk1, pk2, ...) > (lastSeen1, lastSeen2, ...) ORDER BY pk1,
   *  pk2, ... LIMIT batchSize}, not {@code OFFSET} (which DuckDB re-scans-and-discards on every
   *  call -- same problem and same fix EntityBridgeListener's own batching already established
   *  for runOrgSource). Confirmed live that DuckDB supports row-tuple comparison
   *  ({@code (a, b) > (x, y)}) correctly, including for composite keys.
   *
   *  <p>Bounds peak memory to one batch regardless of table size -- required once a
   *  multi-million-row entity-grain table (e.g. {@code transport.fmcsa_carriers}) is a
   *  registered source; the original one-shot {@link #queryRows} call loaded the entire table
   *  into a Java List, which OOM'd in practice the moment such a table was added, 2026-08-30. */
  private static void queryRowsBatched(Connection conn, String loc, List<String> selectCols,
      List<String> pkColumns, int batchSize, BatchConsumer batchConsumer) throws SQLException {
    queryRowsBatched(conn, loc, selectCols, pkColumns, batchSize, 0, null, batchConsumer);
  }

  /** As above, but stops once {@code maxTotalRows} rows have been fetched across all pages
   *  ({@code <= 0} means unlimited, the production default). Test-only knob (see {@link #main}'s
   *  {@code CHUNK_ORGANIZER_MAX_ROWS_PER_SOURCE}) for a fast, bounded sweep across every
   *  contributor regardless of a source's real size -- never set in normal production runs. */
  private static void queryRowsBatched(Connection conn, String loc, List<String> selectCols,
      List<String> pkColumns, int batchSize, int maxTotalRows, BatchConsumer batchConsumer)
      throws SQLException {
    queryRowsBatched(conn, loc, selectCols, pkColumns, batchSize, maxTotalRows, null,
        batchConsumer);
  }

  /** As above, but when {@code yearFilter} is non-null, scopes every page to {@code year IN
   *  (...)} instead of the whole table -- see {@link #selectChangedYears}. {@code null} means no
   *  filter (identical to the pre-existing behavior); a non-null but empty set means nothing
   *  changed for any tracked year, so this returns immediately without issuing any query at all
   *  (an empty SQL IN-list is invalid and there is nothing to scan regardless). */
  private static void queryRowsBatched(Connection conn, String loc, List<String> selectCols,
      List<String> pkColumns, int batchSize, int maxTotalRows, Set<Integer> yearFilter,
      BatchConsumer batchConsumer) throws SQLException {
    if (yearFilter != null && yearFilter.isEmpty()) {
      return;
    }
    String yearClause = null;
    if (yearFilter != null) {
      StringBuilder yc = new StringBuilder("year IN (");
      boolean first = true;
      for (Integer year : yearFilter) {
        if (!first) {
          yc.append(", ");
        }
        yc.append(year.intValue());
        first = false;
      }
      yc.append(')');
      yearClause = yc.toString();
    }
    String orderBy = String.join(", ", pkColumns);
    List<Object> cursor = null;
    long fetched = 0;
    while (true) {
      int limit = batchSize;
      if (maxTotalRows > 0) {
        limit = (int) Math.min(batchSize, maxTotalRows - fetched);
        if (limit <= 0) {
          break;
        }
      }
      StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", selectCols))
          .append(" FROM iceberg_scan('").append(loc).append("', allow_moved_paths=true)");
      List<String> whereClauses = new ArrayList<String>(2);
      if (yearClause != null) {
        whereClauses.add(yearClause);
      }
      if (cursor != null) {
        StringBuilder cursorClause = new StringBuilder("(").append(orderBy).append(") > (");
        for (int i = 0; i < cursor.size(); i++) {
          if (i > 0) {
            cursorClause.append(", ");
          }
          cursorClause.append(sqlLiteral(cursor.get(i)));
        }
        cursorClause.append(')');
        whereClauses.add(cursorClause.toString());
      }
      if (!whereClauses.isEmpty()) {
        sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
      }
      sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
      List<Map<String, Object>> batch = queryRows(conn, sql.toString());
      if (batch.isEmpty()) {
        break;
      }
      batchConsumer.accept(batch);
      fetched += batch.size();
      if (batch.size() < limit) {
        break;
      }
      Map<String, Object> last = batch.get(batch.size() - 1);
      cursor = new ArrayList<Object>(pkColumns.size());
      for (String pk : pkColumns) {
        cursor.add(last.get(pk));
      }
    }
  }

  private static String sqlLiteral(Object value) {
    if (value == null) {
      return "NULL";
    }
    if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    }
    return "'" + value.toString().replace("'", "''") + "'";
  }

  /** One row-concat source registry entry. */
  private static final class RowConcatSource {
    final String sourceSchema;
    final String sourceTable;
    /** One or more columns forming the source table's own primary key -- stringified_fk is
     *  their ':'-joined values, uniform for single-column and composite PKs alike. */
    final List<String> pkColumns;
    final List<String> stringColumns;
    /** Members of {@link #stringColumns} whose values are HTML: {@link #stripHtml} runs on them
     *  before they are joined, so markup is not embedded as text. Empty for almost every source. */
    final Set<String> htmlColumns;
    /** Wide FK columns, one per PK component, named as {sourceSchema}_{sourceTable}_{pkColumn}.
     *  Allows direct SQL joins back to source rows without string parsing. Automatically
     *  generated if wideFkColumn is null; list is non-empty to ensure every source supports joins. */
    final List<String> fkColumns;

    RowConcatSource(String sourceSchema, String sourceTable, List<String> pkColumns,
        List<String> stringColumns, String legacyWideFkColumn) {
      this(sourceSchema, sourceTable, pkColumns, stringColumns, legacyWideFkColumn,
          Collections.<String>emptySet());
    }

    RowConcatSource(String sourceSchema, String sourceTable, List<String> pkColumns,
        List<String> stringColumns, String legacyWideFkColumn, Set<String> htmlColumns) {
      this.sourceSchema = sourceSchema;
      this.sourceTable = sourceTable;
      this.pkColumns = pkColumns;
      this.stringColumns = stringColumns;
      this.htmlColumns = htmlColumns;
      // All sources generate FK columns: legacy callers pass a single wideFkColumn (used as-is),
      // but most pass null and we auto-generate from PK columns. Either way, fkColumns is
      // non-empty to support star-schema joins from vc_staging back to every source row.
      if (legacyWideFkColumn != null) {
        this.fkColumns = Arrays.asList(legacyWideFkColumn);
      } else {
        this.fkColumns = fkColumnsFrom(sourceSchema, sourceTable, pkColumns);
      }
    }
  }

  private static List<String> fkColumnsFrom(String sourceSchema, String sourceTable, List<String> pkColumns) {
    List<String> cols = new ArrayList<String>(pkColumns.size());
    for (String pk : pkColumns) {
      cols.add(sourceSchema + "_" + sourceTable + "_" + pk);
    }
    return cols;
  }

  /** One document-blob source registry entry. */
  private static final class DocumentBlobSource {
    final String sourceSchema;
    final String sourceTable;
    final List<String> pkColumns;
    final String blobColumn;
    final String sourceType;
    final List<String> fkColumns;
    /** Pluggable chunk-parsing function for this source's blob column -- see {@link
     *  ChunkFunction}'s javadoc for why this exists instead of every source sharing one
     *  hardcoded chunker. */
    final ChunkFunction chunker;

    DocumentBlobSource(String sourceSchema, String sourceTable, List<String> pkColumns,
        String blobColumn, String sourceType, String legacyWideFkColumn, ChunkFunction chunker) {
      this.sourceSchema = sourceSchema;
      this.sourceTable = sourceTable;
      this.pkColumns = pkColumns;
      this.blobColumn = blobColumn;
      this.sourceType = sourceType;
      // All sources generate FK columns: legacy callers pass a single wideFkColumn (used as-is),
      // but most pass null and we auto-generate from PK columns. Either way, fkColumns is
      // non-empty to support star-schema joins from vc_staging back to every source row.
      if (legacyWideFkColumn != null) {
        this.fkColumns = Arrays.asList(legacyWideFkColumn);
      } else {
        this.fkColumns = fkColumnsFrom(sourceSchema, sourceTable, pkColumns);
      }
      this.chunker = chunker;
    }
  }
}
