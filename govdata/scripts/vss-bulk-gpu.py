#!/usr/bin/env python3
"""
VSS Bulk Embedding Pipeline (GPU)

Pipeline:
1. Read chunks with NULL embeddings from vectorized_chunks (Iceberg via PyIceberg)
2. Generate embeddings using sentence-transformers (GPU)
3. Write updated chunks back to Iceberg (proper Iceberg append via PyIceberg)
4. Build VSS index (local DuckDB with HNSW)
5. Upload VSS cache to S3

Uses snowflake-arctic-embed-xs (384 dimensions) - same model as quackformers,
so query-time embedding with quackformers is compatible.

Usage:
    # Process all chunks with NULL embeddings
    python vss-bulk-gpu.py --all

    # Dry run (count chunks only)
    python vss-bulk-gpu.py --all --dry-run

    # Rebuild VSS from existing vectorized_chunks (no new embeddings)
    python vss-bulk-gpu.py --rebuild-vss-only
"""

import argparse
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Constants
EMBEDDING_MODEL = "Snowflake/snowflake-arctic-embed-xs"
EMBEDDING_DIM = 384
BATCH_SIZE = 64

S3_BUCKET = "s3://govdata-parquet-v1"
ICEBERG_CHUNKS = f"{S3_BUCKET}/source=sec/SEC/vectorized_chunks"
VSS_CACHE_PATH = f"{S3_BUCKET}/cache/vss/chunks_vss.duckdb"
VSS_METADATA_PATH = f"{S3_BUCKET}/cache/vss/metadata.json"

# Iceberg table identifier
ICEBERG_TABLE = "vectorized_chunks"
ICEBERG_WAREHOUSE = f"{S3_BUCKET}/source=sec/SEC"


def check_gpu() -> str:
    """Check for GPU availability."""
    try:
        import torch
        if torch.cuda.is_available():
            device_name = torch.cuda.get_device_name(0)
            mem_gb = torch.cuda.get_device_properties(0).total_memory / 1e9
            print(f"GPU: {device_name} ({mem_gb:.1f} GB)")
            return "cuda"
        else:
            print("WARNING: No GPU detected, using CPU (will be slow)")
            return "cpu"
    except ImportError:
        print("WARNING: PyTorch not installed, using CPU")
        return "cpu"


def get_s3_config() -> dict:
    """Get S3/R2 configuration from environment."""
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_OVERRIDE"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"Error: Missing environment variables: {missing}")
        print("Source .env.prod first")
        sys.exit(1)

    return {
        "s3_region": "us-east-1",
        "s3_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3_endpoint": os.environ["AWS_ENDPOINT_OVERRIDE"].replace("https://", ""),
        "s3_endpoint_url": os.environ["AWS_ENDPOINT_OVERRIDE"],
    }


def create_duckdb_connection(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with Iceberg + S3/httpfs support."""
    print("  Creating DuckDB connection...")
    conn = duckdb.connect(db_path)
    print("  Installing extensions...")
    conn.execute("INSTALL iceberg; LOAD iceberg")
    conn.execute("INSTALL httpfs; LOAD httpfs")

    config = get_s3_config()
    print("  Configuring S3...")
    conn.execute(f"SET s3_region = '{config['s3_region']}'")
    conn.execute(f"SET s3_access_key_id = '{config['s3_access_key_id']}'")
    conn.execute(f"SET s3_secret_access_key = '{config['s3_secret_access_key']}'")
    conn.execute(f"SET s3_endpoint = '{config['s3_endpoint']}'")
    conn.execute("SET s3_use_ssl = true")
    conn.execute("SET s3_url_style = 'path'")
    conn.execute("SET unsafe_enable_version_guessing = true")
    print("  DuckDB ready!")

    return conn


def get_pyiceberg_s3_properties() -> dict:
    """Get S3 properties for PyIceberg file I/O."""
    config = get_s3_config()
    return {
        "s3.access-key-id": config["s3_access_key_id"],
        "s3.secret-access-key": config["s3_secret_access_key"],
        "s3.endpoint": config["s3_endpoint_url"],
        "s3.region": config["s3_region"],
    }


def load_iceberg_table():
    """Load the Iceberg table using PyIceberg in-memory catalog + register_table."""
    from pyiceberg.catalog import load_catalog

    props = get_pyiceberg_s3_properties()

    catalog = load_catalog(
        "default",
        **{
            "type": "in-memory",
            "warehouse": ICEBERG_WAREHOUSE.replace("s3://", "s3a://"),
            **props,
        },
    )

    # Create namespace if needed, then register existing table from metadata
    try:
        catalog.create_namespace("default")
    except Exception:
        pass

    metadata_location = f"{ICEBERG_CHUNKS}/metadata"

    # Read version-hint.text to find current metadata version
    import s3fs
    fs = s3fs.S3FileSystem(
        key=props["s3.access-key-id"],
        secret=props["s3.secret-access-key"],
        client_kwargs={"endpoint_url": props["s3.endpoint"]},
    )
    hint_path = f"{metadata_location}/version-hint.text".replace("s3://", "")
    version = int(fs.cat_file(hint_path).decode().strip())
    metadata_file = f"{metadata_location}/v{version}.metadata.json".replace("s3://", "s3://")

    table = catalog.register_table(
        ("default", ICEBERG_TABLE),
        metadata_file,
    )
    return table


def update_version_hint_after_append(table):
    """After PyIceberg append, copy UUID-named metadata to vN.metadata.json and update version-hint.text.

    PyIceberg writes metadata with UUID naming (00000-uuid.metadata.json) but
    DuckDB iceberg_scan expects sequential naming (vN.metadata.json) via version-hint.text.
    """
    import s3fs

    props = get_pyiceberg_s3_properties()
    fs = s3fs.S3FileSystem(
        key=props["s3.access-key-id"],
        secret=props["s3.secret-access-key"],
        client_kwargs={"endpoint_url": props["s3.endpoint"]},
    )

    metadata_dir = f"{ICEBERG_CHUNKS}/metadata".replace("s3://", "")
    hint_path = f"{metadata_dir}/version-hint.text"

    # Read current version
    current_version = int(fs.cat_file(hint_path).decode().strip())

    # Get the new metadata file path from the table object (set by PyIceberg after append)
    new_metadata_path = table.metadata_location.replace("s3a://", "").replace("s3://", "")

    new_version = current_version + 1
    new_name = f"{metadata_dir}/v{new_version}.metadata.json"

    print(f"  Promoting metadata: {new_metadata_path.split('/')[-1]} -> v{new_version}.metadata.json")

    # Copy the UUID metadata as vN.metadata.json
    content = fs.cat_file(new_metadata_path)
    fs.pipe_file(new_name, content)

    # Update version-hint.text
    fs.pipe_file(hint_path, str(new_version).encode())
    print(f"  Updated version-hint.text to v{new_version}")


def fetch_null_embedding_chunks(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Read chunks from vectorized_chunks that need embeddings.
    Uses DuckDB iceberg_scan with temp table workaround for filter pushdown bug.
    Deduplicates by (cik, chunk_id) to handle Iceberg append-only semantics:
    the Java materializer writes rows with NULL embeddings, and the GPU pipeline
    appends rows WITH embeddings — both coexist in the table.
    Only returns chunks that have NO embedded copy anywhere in the table.
    """
    print("Querying vectorized_chunks for NULL embeddings (iceberg_scan)...")
    start = time.time()

    # Materialize to temp table first (DuckDB iceberg filter pushdown bug workaround)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _all_chunks AS
        SELECT * FROM iceberg_scan('{ICEBERG_CHUNKS}')
    """)

    # Find chunks that need embeddings: have a NULL-embedding row but NO
    # embedded row (i.e., chunk_id+cik combos where no copy has a real embedding)
    result = conn.execute("""
        SELECT c.cik, c.accession_number, c.year, c.chunk_id,
               c.source_type, c.section, c."sequence", c.filing_date,
               c.chunk_text, c.enriched_text, c.content_type,
               c.financial_concepts, c.exhibit_number,
               c.speaker_name, c.speaker_role, c.paragraph_number
        FROM _all_chunks c
        WHERE c.chunk_text IS NOT NULL
          AND LENGTH(c.chunk_text) > 10
          AND (c.embedding IS NULL
               OR (array_length(c.embedding) = 1 AND c.embedding[1] = 0.0))
          AND NOT EXISTS (
              SELECT 1 FROM _all_chunks e
              WHERE e.cik = c.cik AND e.chunk_id = c.chunk_id
                AND e.embedding IS NOT NULL
                AND array_length(e.embedding) > 1
          )
    """).fetchall()

    elapsed = time.time() - start

    columns = [
        "cik", "accession_number", "year", "chunk_id",
        "source_type", "section", "sequence", "filing_date",
        "chunk_text", "enriched_text", "content_type",
        "financial_concepts", "exhibit_number",
        "speaker_name", "speaker_role", "paragraph_number"
    ]

    chunks = [dict(zip(columns, row)) for row in result]
    print(f"Found {len(chunks)} chunks needing embeddings ({elapsed:.1f}s)")
    return chunks


def generate_embeddings(chunks: list[dict], device: str) -> list[list[float]]:
    """Generate embeddings using sentence-transformers."""
    if not chunks:
        return []

    from sentence_transformers import SentenceTransformer

    print(f"  Loading model {EMBEDDING_MODEL} on {device}...")
    model = SentenceTransformer(EMBEDDING_MODEL, device=device)

    texts = [c["chunk_text"] for c in chunks]

    print(f"  Generating {len(texts)} embeddings...")
    start = time.time()

    embeddings = model.encode(
        texts,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,  # For cosine similarity
    )

    elapsed = time.time() - start
    rate = len(texts) / elapsed if elapsed > 0 else 0
    print(f"  Generated {len(embeddings)} embeddings in {elapsed:.1f}s ({rate:.1f}/sec)")

    return [emb.tolist() for emb in embeddings]


def write_to_iceberg(chunks: list[dict], embeddings: list[list[float]], year: int) -> int:
    """Write chunks with embeddings to vectorized_chunks Iceberg table via PyIceberg."""
    if not chunks:
        return 0

    print(f"  Writing {len(chunks)} chunks to Iceberg (year={year})...")

    table = load_iceberg_table()

    data = {
        "cik": [c["cik"] for c in chunks],
        "accession_number": [c["accession_number"] for c in chunks],
        "year": [c["year"] for c in chunks],
        "chunk_id": [c["chunk_id"] for c in chunks],
        "source_type": [c["source_type"] for c in chunks],
        "section": [c["section"] for c in chunks],
        "sequence": [c["sequence"] for c in chunks],
        "filing_date": [c["filing_date"] for c in chunks],
        "chunk_text": [c["chunk_text"] for c in chunks],
        "enriched_text": [c["enriched_text"] for c in chunks],
        "embedding": embeddings,
        "content_type": [c["content_type"] for c in chunks],
        "financial_concepts": [c["financial_concepts"] for c in chunks],
        "exhibit_number": [c["exhibit_number"] for c in chunks],
        "speaker_name": [c["speaker_name"] for c in chunks],
        "speaker_role": [c["speaker_role"] for c in chunks],
        "paragraph_number": [c["paragraph_number"] for c in chunks],
    }

    # Create Arrow table matching Iceberg schema
    arrow_schema = pa.schema([
        ("cik", pa.string()),
        ("accession_number", pa.string()),
        ("year", pa.int32()),
        ("chunk_id", pa.string()),
        ("source_type", pa.string()),
        ("section", pa.string()),
        ("sequence", pa.int32()),
        ("filing_date", pa.string()),
        ("chunk_text", pa.string()),
        ("enriched_text", pa.string()),
        ("embedding", pa.list_(pa.float32())),
        ("content_type", pa.string()),
        ("financial_concepts", pa.string()),
        ("exhibit_number", pa.string()),
        ("speaker_name", pa.string()),
        ("speaker_role", pa.string()),
        ("paragraph_number", pa.int32()),
    ])

    arrow_table = pa.table(data, schema=arrow_schema)
    table.append(arrow_table)

    # Update version-hint.text so DuckDB iceberg_scan can find the new data
    update_version_hint_after_append(table)

    print(f"  Committed {len(chunks)} rows to Iceberg (year={year})")
    return len(chunks)


def build_vss_database(conn: duckdb.DuckDBPyConnection, output_path: str) -> tuple:
    """Build VSS DuckDB database with HNSW index from vectorized_chunks via iceberg_scan."""
    print(f"\nBuilding VSS database: {output_path}")

    # Remove existing
    Path(output_path).unlink(missing_ok=True)
    Path(f"{output_path}.wal").unlink(missing_ok=True)

    vss_conn = duckdb.connect(output_path)
    vss_conn.execute("INSTALL vss; LOAD vss")
    vss_conn.execute("SET hnsw_enable_experimental_persistence = true")

    # Create table
    vss_conn.execute(f"""
        CREATE TABLE chunks (
            cik VARCHAR,
            accession_number VARCHAR,
            yr INTEGER,
            chunk_id VARCHAR,
            source_type VARCHAR,
            section VARCHAR,
            chunk_text VARCHAR,
            content_type VARCHAR,
            embedding FLOAT[{EMBEDDING_DIM}]
        )
    """)

    # Load from Iceberg via iceberg_scan
    total_chunks = 0

    print(f"  Loading all chunks with embeddings from Iceberg...")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = f.name

    try:
        # Materialize into temp table first to avoid DuckDB iceberg filter pushdown
        # bug with list columns (array_length filter fails with direct iceberg_scan)
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _iceberg_chunks AS
            SELECT * FROM iceberg_scan('{ICEBERG_CHUNKS}')
        """)
        # Deduplicate by (cik, chunk_id): Iceberg is append-only, so both the
        # original NULL-embedding row and the GPU-embedded row coexist.
        # Use ROW_NUMBER to pick the row with a real embedding (non-NULL, length > 1).
        conn.execute(f"""
            COPY (
                SELECT cik, accession_number, year as yr, chunk_id,
                       source_type, section, chunk_text, content_type, embedding
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY cik, chunk_id
                        ORDER BY CASE WHEN embedding IS NOT NULL
                                       AND array_length(embedding) > 1
                                      THEN 0 ELSE 1 END
                    ) as _rn
                    FROM _iceberg_chunks
                )
                WHERE _rn = 1
                  AND embedding IS NOT NULL
                  AND array_length(embedding) > 1
            ) TO '{temp_path}' (FORMAT PARQUET)
        """)

        # Load into VSS database
        vss_conn.execute(f"""
            INSERT INTO chunks
            SELECT cik, accession_number, yr, chunk_id, source_type, section,
                   chunk_text, content_type, embedding::FLOAT[{EMBEDDING_DIM}]
            FROM read_parquet('{temp_path}')
        """)

        total_chunks = vss_conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        print(f"  Loaded {total_chunks} chunks with embeddings")

    except Exception as e:
        print(f"  Error loading chunks: {e}")

    finally:
        Path(temp_path).unlink(missing_ok=True)

    if total_chunks == 0:
        print("  No chunks found!")
        vss_conn.close()
        return 0, 0

    # Build HNSW index
    print(f"  Building HNSW index on {total_chunks} chunks...")
    start = time.time()
    vss_conn.execute("CREATE INDEX chunks_hnsw ON chunks USING HNSW (embedding) WITH (metric = 'cosine')")
    index_time = time.time() - start
    print(f"  Index built in {index_time:.1f}s")

    vss_conn.close()

    size_mb = Path(output_path).stat().st_size / (1024 * 1024)
    print(f"  Database: {total_chunks} chunks, {size_mb:.1f} MB")

    return total_chunks, size_mb


def read_vss_metadata() -> dict:
    """Download and parse metadata.json from S3. Returns dict with at least {"version": 0}."""
    import subprocess

    endpoint = os.environ.get("AWS_ENDPOINT_OVERRIDE", "")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        temp_path = f.name

    try:
        cmd = ["aws", "s3", "cp", VSS_METADATA_PATH, temp_path, "--endpoint-url", endpoint]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"  No existing metadata.json on S3 (starting fresh)")
            return {"version": 0}

        with open(temp_path) as f:
            metadata = json.load(f)

        metadata.setdefault("version", 0)
        return metadata

    finally:
        Path(temp_path).unlink(missing_ok=True)


def upload_vss_to_s3(local_path: str, total_chunks: int, version: int = 1) -> bool:
    """Upload VSS database and metadata to S3."""
    import subprocess

    endpoint = os.environ.get("AWS_ENDPOINT_OVERRIDE", "")

    print(f"\nUploading VSS database to S3...")

    # Upload database
    cmd = ["aws", "s3", "cp", local_path, VSS_CACHE_PATH, "--endpoint-url", endpoint]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"  Upload failed: {result.stderr}")
        return False

    print(f"  Uploaded {VSS_CACHE_PATH}")

    # Upload metadata
    metadata = {
        "rebuilt": datetime.now(timezone.utc).isoformat(),
        "chunks": total_chunks,
        "years": "all",
        "embed_model": EMBEDDING_MODEL,
        "embed_dim": EMBEDDING_DIM,
        "version": version,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(metadata, f)
        temp_path = f.name

    try:
        cmd = ["aws", "s3", "cp", temp_path, VSS_METADATA_PATH, "--endpoint-url", endpoint]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"  Metadata upload failed: {result.stderr}")
            return False

        print(f"  Uploaded {VSS_METADATA_PATH}")

    finally:
        Path(temp_path).unlink(missing_ok=True)

    return True


def check_version_and_upload(local_path: str, total_chunks: int, start_version: int) -> bool:
    """Check for version conflicts or regression before uploading VSS cache.

    Returns True if upload succeeded, False if skipped.
    """
    current_meta = read_vss_metadata()
    current_version = current_meta.get("version", 0)
    current_chunks = current_meta.get("chunks", 0)

    if current_version != start_version:
        print(f"  Version changed (expected {start_version}, found {current_version}) "
              f"— another builder uploaded. Skipping.")
        return False

    if current_chunks > total_chunks:
        print(f"  Current cache has more chunks ({current_chunks}) than rebuild "
              f"({total_chunks}). Skipping.")
        return False

    return upload_vss_to_s3(local_path, total_chunks, start_version + 1)


def main():
    parser = argparse.ArgumentParser(description="VSS Bulk Embedding Pipeline (GPU)")
    parser.add_argument("--all", action="store_true", help="Process all chunks with NULL embeddings")
    parser.add_argument("--dry-run", action="store_true", help="Count chunks only")
    parser.add_argument("--rebuild-vss-only", action="store_true",
                        help="Skip embedding, rebuild VSS from existing vectorized_chunks")
    parser.add_argument("--skip-iceberg", action="store_true",
                        help="Don't write to Iceberg (generate embeddings only)")
    parser.add_argument("--output", type=str, default="chunks_vss.duckdb",
                        help="Output VSS database path")
    parser.add_argument("--upload", action="store_true", help="Upload VSS to S3 after completion")
    args = parser.parse_args()

    if not args.all and not args.rebuild_vss_only:
        parser.error("Specify --all or --rebuild-vss-only")

    print("=" * 60)
    print("VSS Bulk Embedding Pipeline")
    print("=" * 60)
    print(f"Model: {EMBEDDING_MODEL}")
    print(f"Dimensions: {EMBEDDING_DIM}")
    print()

    # Check GPU
    device = check_gpu()
    print()

    # Connect to Iceberg via DuckDB (for reads)
    print("Connecting to Iceberg...")
    conn = create_duckdb_connection()
    print()

    # Read current VSS metadata version for optimistic concurrency control
    start_meta = read_vss_metadata()
    start_version = start_meta.get("version", 0)
    print(f"VSS metadata version at start: {start_version}")
    print()

    # Rebuild VSS only mode (builds local VSS from existing embeddings)
    if args.rebuild_vss_only:
        total_chunks, size_mb = build_vss_database(conn, args.output)

        if args.upload and total_chunks > 0:
            check_version_and_upload(args.output, total_chunks, start_version)

        print("\nDone!")
        return

    # Full pipeline: Fetch all NULL-embedding chunks, Embed, Store
    chunks = fetch_null_embedding_chunks(conn)

    if not chunks:
        print("No chunks needing embeddings — nothing to do!")
        return

    if args.dry_run:
        print(f"[DRY RUN] Would process {len(chunks)} chunks")
        return

    # Generate embeddings
    embeddings = generate_embeddings(chunks, device)

    if not args.skip_iceberg:
        # Group chunks by year for writing
        chunks_by_year = {}
        for i, chunk in enumerate(chunks):
            yr = chunk["year"]
            if yr not in chunks_by_year:
                chunks_by_year[yr] = ([], [])
            chunks_by_year[yr][0].append(chunk)
            chunks_by_year[yr][1].append(embeddings[i])

        all_new_chunks = 0
        for yr in sorted(chunks_by_year.keys()):
            yr_chunks, yr_embeds = chunks_by_year[yr]
            written = write_to_iceberg(yr_chunks, yr_embeds, yr)
            all_new_chunks += written
    else:
        all_new_chunks = len(chunks)
        print("  [SKIP] Not writing to Iceberg")

    print("\n" + "=" * 60)
    print("Pipeline Complete")
    print("=" * 60)
    print(f"Chunks embedded: {all_new_chunks}")


if __name__ == "__main__":
    main()
