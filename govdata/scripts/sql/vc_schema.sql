-- vc_schema.sql — PostgreSQL compute-layer tables for ref.vectorized_chunks.
--
-- Lives in the same PG schema as pipeline_tracker/table_completion (derived from the parquet
-- bucket name via pg_ns_from_bucket, e.g. govdata_parquet_v1) so dq and prod stay isolated the
-- same way the tracker does. Applied via psql -v ns=<namespace> from vc_pg.sh.
--
-- Design (see conversation/memory for full rationale):
--   - The row key mirrors ref.vectorized_chunks' own Iceberg PK exactly:
--     (source_schema, source_table, stringified_fk, sequence). chunk_id is stored as the SAME
--     convenience string the Iceberg table stores ('<source_schema>:<source_table>:
--     <stringified_fk>:<sequence>'), not a separate identity.
--   - parent_hash is a content hash of the SOURCE ROW's raw pre-chunk text (the row-concat
--     string or document-blob column). Unchanged hash on a recompute pass means "skip, nothing
--     to do" — this IS the idempotency check.
--   - The old position/loop-counter orphaning bug (re-chunking producing a different chunk
--     COUNT for the same parent, stranding old sequence numbers) is solved structurally, not by
--     a content-addressed key: a parent_hash change ALWAYS tombstones every existing row for
--     that (source_schema, source_table, stringified_fk) — the whole parent's prior chunk set —
--     before inserting the freshly computed one. No partial chunk-level upsert ever happens, so
--     there is no scenario where some old sequence numbers survive a re-chunk while others don't.
--   - Deletes never happen directly against Iceberg. A removal (parent content changed, or an
--     explicit vc_remove.sh call) moves rows from vc_staging into vc_tombstones; the sync step
--     is the only thing that turns a tombstone into an Iceberg equality-delete. One mechanism,
--     one path to "data disappears from Iceberg" — never a partition overwrite.
--   - parent_unit exists for a future finer-grained parent (e.g. SEC per-section chunking, where
--     one filing's stringified_fk contains many independently-re-chunkable sections) but is
--     unused ('') by every source onboarded so far — ChunkOrganizer's row-concat and
--     document-blob sources are both one-parent-per-source-row. Not part of the PK today; adding
--     SEC means extending the PK, a deliberate future migration, not a silent scope change.

CREATE SCHEMA IF NOT EXISTS :"ns";

-- Current compute-layer state of every chunk destined for ref.vectorized_chunks.
CREATE TABLE IF NOT EXISTS :"ns".vc_staging (
  source_schema           VARCHAR NOT NULL,
  source_table            VARCHAR NOT NULL,
  stringified_fk          VARCHAR NOT NULL,
  sequence                BIGINT NOT NULL,
  parent_unit             VARCHAR NOT NULL DEFAULT '',
  chunk_id                VARCHAR NOT NULL,
  parent_hash             VARCHAR NOT NULL,
  source_type             VARCHAR NOT NULL,
  year                    INT,
  cik                     VARCHAR,
  accession_number        VARCHAR,
  filing_date             VARCHAR,
  section                 VARCHAR,
  subsection              VARCHAR,
  section_path            VARCHAR,
  paragraph_continuation  BOOLEAN,
  chunk_text              TEXT NOT NULL,
  enriched_text           TEXT,
  content_type            VARCHAR,
  financial_concepts      VARCHAR,
  exhibit_number          VARCHAR,
  speaker_name            VARCHAR,
  speaker_role            VARCHAR,
  paragraph_number        BIGINT,
  ref_naics_code          VARCHAR,
  fedregister_document_number VARCHAR,
  updated_at              BIGINT NOT NULL,
  PRIMARY KEY (source_schema, source_table, stringified_fk, sequence)
);

CREATE INDEX IF NOT EXISTS idx_vc_staging_updated_at
  ON :"ns".vc_staging (source_schema, source_table, updated_at);

-- Every removal — a parent_hash-changed re-chunk or an explicit vc_remove.sh call — lands here
-- first. applied_at stays NULL until the sync step commits the matching Iceberg equality-delete;
-- only then is the tombstone considered drained (retained for a while after for auditability,
-- not deleted immediately on apply).
CREATE TABLE IF NOT EXISTS :"ns".vc_tombstones (
  source_schema     VARCHAR NOT NULL,
  source_table      VARCHAR NOT NULL,
  stringified_fk    VARCHAR NOT NULL,
  sequence          BIGINT NOT NULL,
  chunk_id          VARCHAR NOT NULL,
  tombstoned_at     BIGINT NOT NULL,
  applied_at        BIGINT,
  PRIMARY KEY (source_schema, source_table, stringified_fk, sequence, tombstoned_at)
);

CREATE INDEX IF NOT EXISTS idx_vc_tombstones_pending
  ON :"ns".vc_tombstones (source_schema, source_table)
  WHERE applied_at IS NULL;

-- Per-(source_schema, source_table) watermarks, two independent ones:
--   last_swept_completed_at — the source table's OWN pipeline_tracker.table_completion.
--     completed_at as of the last sweep that actually rescanned it. Unchanged since last sweep
--     means "nothing in this source has changed at all" -- skip rescanning it entirely, the
--     coarse layer; parent_hash (in vc_staging) is the finer-grained layer underneath it that
--     still catches a per-row content change within a source whose completed_at DID advance.
--   last_synced_at — high-water mark for the (separate) sync step that drains vc_staging/
--     vc_tombstones into Iceberg: only looks at vc_staging rows with updated_at > this value,
--     and tombstones with applied_at IS NULL.
CREATE TABLE IF NOT EXISTS :"ns".vc_sync_state (
  source_schema             VARCHAR NOT NULL,
  source_table              VARCHAR NOT NULL,
  last_swept_completed_at   BIGINT NOT NULL DEFAULT 0,
  last_synced_at            BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (source_schema, source_table)
);
