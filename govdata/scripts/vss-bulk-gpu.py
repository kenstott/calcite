#!/usr/bin/env python3
"""
VSS Bulk Embedding Pipeline (GPU)

Pipeline:
1. Acquire text from mda_sections (Iceberg)
2. Generate embeddings using sentence-transformers (GPU)
3. Store in vectorized_chunks (Iceberg)
4. Build VSS index (local DuckDB with HNSW)
5. Upload VSS cache to S3

Uses snowflake-arctic-embed-xs (384 dimensions) - same model as quackformers,
so query-time embedding with quackformers is compatible.

Usage:
    # Install dependencies
    pip install sentence-transformers duckdb pyarrow

    # Process specific years
    python vss-bulk-gpu.py --years 2024,2025

    # Process all years
    python vss-bulk-gpu.py --all

    # Dry run (count chunks only)
    python vss-bulk-gpu.py --years 2024 --dry-run

    # Skip Iceberg write, only rebuild VSS from existing vectorized_chunks
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

# Constants
EMBEDDING_MODEL = "Snowflake/snowflake-arctic-embed-xs"
EMBEDDING_DIM = 384
CHUNK_SIZE = 2000  # chars (~512 tokens)
CHUNK_OVERLAP = 200  # chars overlap between chunks
BATCH_SIZE = 256

S3_BUCKET = "s3://govdata-parquet-v1"
ICEBERG_MDA = f"{S3_BUCKET}/source=sec/SEC/mda_sections"
ICEBERG_CHUNKS = f"{S3_BUCKET}/source=sec/SEC/vectorized_chunks"
VSS_CACHE_PATH = f"{S3_BUCKET}/cache/vss/chunks_vss.duckdb"
VSS_METADATA_PATH = f"{S3_BUCKET}/cache/vss/metadata.json"


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
    }


def create_duckdb_connection(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with Iceberg/S3 support."""
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL iceberg; LOAD iceberg")
    conn.execute("INSTALL httpfs; LOAD httpfs")

    config = get_s3_config()
    conn.execute(f"SET s3_region = '{config['s3_region']}'")
    conn.execute(f"SET s3_access_key_id = '{config['s3_access_key_id']}'")
    conn.execute(f"SET s3_secret_access_key = '{config['s3_secret_access_key']}'")
    conn.execute(f"SET s3_endpoint = '{config['s3_endpoint']}'")
    conn.execute("SET s3_use_ssl = true")
    conn.execute("SET s3_url_style = 'path'")
    conn.execute("SET unsafe_enable_version_guessing = true")

    return conn


def get_available_years(conn: duckdb.DuckDBPyConnection) -> list[int]:
    """Get years with MDA sections available."""
    result = conn.execute(f"""
        SELECT DISTINCT "year"
        FROM iceberg_scan('{ICEBERG_MDA}')
        WHERE "year" IS NOT NULL
        ORDER BY "year" DESC
    """).fetchall()
    return [row[0] for row in result]


def get_existing_chunks(conn: duckdb.DuckDBPyConnection, year: int) -> set[str]:
    """Get chunk_ids already in vectorized_chunks for a year."""
    try:
        result = conn.execute(f"""
            SELECT DISTINCT chunk_id
            FROM iceberg_scan('{ICEBERG_CHUNKS}')
            WHERE "year" = {year}
              AND embedding IS NOT NULL
        """).fetchall()
        return {row[0] for row in result}
    except Exception:
        # Table might not exist yet
        return set()


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks."""
    if not text or len(text) < 50:
        return []

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()
        if len(chunk) >= 50:  # Minimum chunk size
            chunks.append(chunk)
        start = end - overlap

    return chunks


def extract_chunks_for_year(conn: duckdb.DuckDBPyConnection, year: int,
                            existing_chunks: set[str]) -> list[dict]:
    """
    Extract text from mda_sections and chunk it.
    Skip chunks that already exist in vectorized_chunks.
    """
    print(f"  Querying mda_sections for year {year}...")

    result = conn.execute(f"""
        SELECT cik, accession_number, "year", section, paragraph_text
        FROM iceberg_scan('{ICEBERG_MDA}')
        WHERE "year" = {year}
          AND paragraph_text IS NOT NULL
          AND LENGTH(paragraph_text) > 100
    """).fetchall()

    print(f"  Found {len(result)} MDA sections")

    chunks = []
    skipped = 0

    for row in result:
        cik, accession_number, yr, section, paragraph_text = row

        text_chunks = chunk_text(paragraph_text)

        for i, chunk_text_content in enumerate(text_chunks):
            chunk_id = f"{cik}_{accession_number}_{section}_{i}"

            # Skip if already processed
            if chunk_id in existing_chunks:
                skipped += 1
                continue

            chunks.append({
                "cik": cik,
                "accession_number": accession_number,
                "year": yr,
                "chunk_id": chunk_id,
                "section": section,
                "chunk_text": chunk_text_content,
            })

    print(f"  Extracted {len(chunks)} new chunks (skipped {skipped} existing)")
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


def write_to_iceberg(conn: duckdb.DuckDBPyConnection, chunks: list[dict],
                     embeddings: list[list[float]], year: int) -> int:
    """Write chunks with embeddings to vectorized_chunks Iceberg table."""
    if not chunks:
        return 0

    print(f"  Writing {len(chunks)} chunks to Iceberg...")

    # Create temporary parquet file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = f.name

    try:
        # Build data for parquet
        import pyarrow as pa
        import pyarrow.parquet as pq

        data = {
            "cik": [c["cik"] for c in chunks],
            "accession_number": [c["accession_number"] for c in chunks],
            "year": [c["year"] for c in chunks],
            "chunk_id": [c["chunk_id"] for c in chunks],
            "section": [c["section"] for c in chunks],
            "chunk_text": [c["chunk_text"] for c in chunks],
            "embedding": embeddings,
        }

        # Create Arrow table with correct types
        schema = pa.schema([
            ("cik", pa.string()),
            ("accession_number", pa.string()),
            ("year", pa.int32()),
            ("chunk_id", pa.string()),
            ("section", pa.string()),
            ("chunk_text", pa.string()),
            ("embedding", pa.list_(pa.float32(), EMBEDDING_DIM)),
        ])

        table = pa.table(data, schema=schema)
        pq.write_table(table, temp_path)

        # Upload to Iceberg via DuckDB
        # Note: DuckDB Iceberg extension doesn't support direct INSERT yet,
        # so we append parquet files to the Iceberg table location
        s3_dest = f"{ICEBERG_CHUNKS}/year={year}/chunks_{int(time.time())}.parquet"

        conn.execute(f"""
            COPY (SELECT * FROM read_parquet('{temp_path}'))
            TO '{s3_dest}' (FORMAT PARQUET)
        """)

        print(f"  Wrote to {s3_dest}")
        return len(chunks)

    finally:
        Path(temp_path).unlink(missing_ok=True)


def build_vss_database(conn: duckdb.DuckDBPyConnection, output_path: str,
                       years: list[int]) -> tuple[int, float]:
    """Build VSS DuckDB database with HNSW index from vectorized_chunks."""
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
            section VARCHAR,
            chunk_text VARCHAR,
            embedding FLOAT[{EMBEDDING_DIM}]
        )
    """)

    # Load from Iceberg for each year
    total_chunks = 0
    config = get_s3_config()

    for year in years:
        print(f"  Loading year {year} from Iceberg...")

        # Export to temp parquet first (workaround for DuckDB Iceberg issues)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name

        try:
            conn.execute(f"""
                COPY (
                    SELECT cik, accession_number, "year" as yr, chunk_id,
                           section, chunk_text, embedding
                    FROM iceberg_scan('{ICEBERG_CHUNKS}')
                    WHERE "year" = {year}
                      AND embedding IS NOT NULL
                ) TO '{temp_path}' (FORMAT PARQUET)
            """)

            # Load into VSS database
            result = vss_conn.execute(f"""
                INSERT INTO chunks
                SELECT cik, accession_number, yr, chunk_id, section, chunk_text,
                       embedding::FLOAT[{EMBEDDING_DIM}]
                FROM read_parquet('{temp_path}')
            """)

            year_count = vss_conn.execute(f"SELECT COUNT(*) FROM chunks WHERE yr = {year}").fetchone()[0]
            print(f"    Year {year}: {year_count} chunks")
            total_chunks += year_count

        except Exception as e:
            print(f"    Year {year}: Error - {e}")

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


def upload_vss_to_s3(local_path: str, total_chunks: int, years: list[int]) -> bool:
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
        "years": sorted(years),
        "embed_model": EMBEDDING_MODEL,
        "embed_dim": EMBEDDING_DIM,
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


def main():
    parser = argparse.ArgumentParser(description="VSS Bulk Embedding Pipeline (GPU)")
    parser.add_argument("--years", type=str, help="Comma-separated years (e.g., 2023,2024,2025)")
    parser.add_argument("--all", action="store_true", help="Process all available years")
    parser.add_argument("--dry-run", action="store_true", help="Count chunks only")
    parser.add_argument("--rebuild-vss-only", action="store_true",
                        help="Skip embedding, rebuild VSS from existing vectorized_chunks")
    parser.add_argument("--skip-iceberg", action="store_true",
                        help="Don't write to Iceberg, only build local VSS")
    parser.add_argument("--output", type=str, default="chunks_vss.duckdb",
                        help="Output VSS database path")
    parser.add_argument("--upload", action="store_true", help="Upload VSS to S3 after completion")
    args = parser.parse_args()

    if not args.years and not args.all and not args.rebuild_vss_only:
        parser.error("Specify --years, --all, or --rebuild-vss-only")

    print("=" * 60)
    print("VSS Bulk Embedding Pipeline")
    print("=" * 60)
    print(f"Model: {EMBEDDING_MODEL}")
    print(f"Dimensions: {EMBEDDING_DIM}")
    print()

    # Check GPU
    device = check_gpu()
    print()

    # Connect to Iceberg
    print("Connecting to Iceberg...")
    conn = create_duckdb_connection()

    # Get years
    if args.rebuild_vss_only or args.all:
        years = get_available_years(conn)
    else:
        years = [int(y.strip()) for y in args.years.split(",")]

    print(f"Years: {years}")
    print()

    # Rebuild VSS only mode
    if args.rebuild_vss_only:
        total_chunks, size_mb = build_vss_database(conn, args.output, years)

        if args.upload and total_chunks > 0:
            upload_vss_to_s3(args.output, total_chunks, years)

        print("\nDone!")
        return

    # Full pipeline: Extract, Embed, Store
    all_new_chunks = 0

    for year in years:
        print(f"\n{'=' * 60}")
        print(f"Processing year {year}")
        print("=" * 60)

        # Get existing chunks to avoid re-processing
        existing = get_existing_chunks(conn, year)
        print(f"  Existing chunks in Iceberg: {len(existing)}")

        # Extract new chunks from MDA sections
        chunks = extract_chunks_for_year(conn, year, existing)

        if not chunks:
            print(f"  No new chunks for year {year}")
            continue

        if args.dry_run:
            print(f"  [DRY RUN] Would process {len(chunks)} chunks")
            continue

        # Generate embeddings
        embeddings = generate_embeddings(chunks, device)

        if not args.skip_iceberg:
            # Write to Iceberg
            written = write_to_iceberg(conn, chunks, embeddings, year)
            all_new_chunks += written
        else:
            print("  [SKIP] Not writing to Iceberg")

    if args.dry_run:
        print("\nDry run complete")
        return

    # Build VSS database
    total_chunks, size_mb = build_vss_database(conn, args.output, years)

    # Upload to S3
    if args.upload and total_chunks > 0:
        upload_vss_to_s3(args.output, total_chunks, years)

    print("\n" + "=" * 60)
    print("Pipeline Complete")
    print("=" * 60)
    print(f"New chunks embedded: {all_new_chunks}")
    print(f"Total chunks in VSS: {total_chunks}")
    print(f"VSS database: {args.output} ({size_mb:.1f} MB)")
    print()
    print("Test query:")
    print(f"  duckdb {args.output} << 'EOF'")
    print("  LOAD vss; LOAD quackformers;")
    print("  SELECT chunk_text,")
    print("         array_cosine_similarity(embedding, embed('china tariffs')::FLOAT[384]) as score")
    print("  FROM chunks ORDER BY score DESC LIMIT 5;")
    print("  EOF")


if __name__ == "__main__":
    main()
