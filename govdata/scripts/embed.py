#!/usr/bin/env python3
"""
One consistent, local embedding generator for govdata semantic search.

Replaces the DuckDB quackformers extension (which has no build for DuckDB v1.4.4)
and the Vultr-GPU/R2 bulk pipeline (which can't reach the local MinIO). Runs on
CPU via fastembed (onnxruntime) with Snowflake/snowflake-arctic-embed-xs — the
SAME 384-dim model quackformers used, so previously stored vectors stay compatible.

Used in two places:
  * ETL fill:  embed-parquet <in.parquet> <id_col> <text_col> <out.parquet>
               Reads rows, emits a parquet of (<id_col>, embedding FLOAT[384]).
  * Search:    query "<text>"
               Prints the query's 384-d vector as a JSON array for
               array_cosine_similarity at runtime.

Model and dimensions are fixed so ETL-time and query-time embeddings always match.
"""
import argparse
import json
import os
import sys

# Must match vss-local.py's MODEL default exactly (same string, same case) — that script
# produces the stored codes, and a differently-cased or overridden model here would embed
# queries into a different space than the corpus.
MODEL_NAME = os.environ.get("VSS_EMBED_MODEL", "Snowflake/snowflake-arctic-embed-xs")
EMBEDDING_DIM = 384


class _SentenceTransformerEmbedder:
    """Adapter exposing fastembed's ``embed(texts) -> iterable of vectors`` shape.

    Kept so cmd_query / cmd_serve / cmd_embed_parquet are unchanged, while the vectors
    themselves come from the same library, model and normalization the PRODUCER uses.
    """

    def __init__(self):
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(MODEL_NAME, device="cpu")

    def embed(self, texts, batch_size=32):
        # normalize_embeddings=True mirrors vss-local.py exactly. The stored codes are
        # quantized from unit vectors, so an unnormalized query vector would be compared
        # against them on a different scale — the cosine rerank would still return rows,
        # just subtly mis-ranked, which is the worst kind of wrong here.
        return self._model.encode(
            list(texts), batch_size=batch_size, normalize_embeddings=True,
            convert_to_numpy=True, show_progress_bar=False)


def _model():
    # sentence-transformers, NOT fastembed: the corpus codes in
    # s3://govdata-parquet-v1/*/vectorized_chunk_codes/ are produced by vss-local.py via
    # SentenceTransformer, and query vectors must come from the same pipeline to live in
    # the same space. fastembed nominally serves the same model through its own ONNX
    # export with its own pooling defaults, so it is not guaranteed to agree — and it is
    # not installed in the embed venv that vss-embed-setup.sh provisions (torch +
    # sentence-transformers + duckdb + pyarrow), so importing it here failed outright.
    # Imported lazily so `--help` works without the dependency installed.
    return _SentenceTransformerEmbedder()


def cmd_query(args):
    model = _model()
    vec = list(model.embed([args.text]))[0]
    out = [float(x) for x in vec]
    if len(out) != EMBEDDING_DIM:
        sys.stderr.write(
            "ERROR: model returned %d dims, expected %d\n" % (len(out), EMBEDDING_DIM))
        return 1
    # Compact JSON array — consumable directly as a DuckDB FLOAT[384] literal.
    sys.stdout.write(json.dumps(out))
    sys.stdout.write("\n")
    return 0


def cmd_serve(args):
    # Persistent embedding server for the airgapped query side. Reads one JSON
    # request per line on stdin and writes one JSON response per line on stdout,
    # with the model loaded exactly once. Java owns the process lifecycle.
    #
    #   request:  {"text": "<query>"}            (or a bare JSON string)
    #   response: {"embedding": [<384 floats>]}  | {"error": "<message>"}
    #
    # Same model as ETL, so query vectors live in the stored vectors' space.
    model = _model()
    sys.stderr.write("embed: serve ready (%s, %d-d)\n" % (MODEL_NAME, EMBEDDING_DIM))
    sys.stderr.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            text = req["text"] if isinstance(req, dict) else str(req)
            vec = list(model.embed([text]))[0]
            out = [float(x) for x in vec]
            if len(out) != EMBEDDING_DIM:
                raise ValueError(
                    "model returned %d dims, expected %d" % (len(out), EMBEDDING_DIM))
            sys.stdout.write(json.dumps({"embedding": out}))
        except Exception as e:  # noqa: BLE001 - keep serving across bad requests
            sys.stdout.write(json.dumps({"error": str(e)}))
        sys.stdout.write("\n")
        sys.stdout.flush()
    return 0


def cmd_embed_parquet(args):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(args.input)
    if args.id_col not in table.column_names:
        sys.stderr.write("ERROR: id column '%s' not in %s\n" % (args.id_col, args.input))
        return 1
    if args.text_col not in table.column_names:
        sys.stderr.write("ERROR: text column '%s' not in %s\n" % (args.text_col, args.input))
        return 1

    ids = table.column(args.id_col).to_pylist()
    texts = table.column(args.text_col).to_pylist()
    # fastembed wants strings; map null/empty text to a single space so the row count
    # is preserved and the vector is deterministic.
    texts = [(t if isinstance(t, str) and t.strip() else " ") for t in texts]

    model = _model()
    rows = len(texts)
    sys.stderr.write("embed: %d rows via %s (%d-d)\n" % (rows, MODEL_NAME, EMBEDDING_DIM))
    vectors = []
    for i, vec in enumerate(model.embed(texts, batch_size=args.batch_size)):
        vectors.append([float(x) for x in vec])
        if (i + 1) % 5000 == 0:
            sys.stderr.write("  %d/%d\n" % (i + 1, rows))

    emb_type = pa.list_(pa.float32(), EMBEDDING_DIM)
    out = pa.table({
        args.id_col: pa.array(ids, type=table.column(args.id_col).type),
        "embedding": pa.array(vectors, type=emb_type),
    })
    pq.write_table(out, args.output)
    sys.stderr.write("embed: wrote %d (%s, embedding) rows to %s\n"
                     % (rows, args.id_col, args.output))
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("query", help="print the 384-d embedding of a text as JSON")
    q.add_argument("text")
    q.set_defaults(func=cmd_query)

    s = sub.add_parser("serve",
                       help="persistent server: JSON request per line on stdin -> JSON response per line on stdout")
    s.set_defaults(func=cmd_serve)

    e = sub.add_parser("embed-parquet",
                       help="embed a text column from a parquet, emit (id, embedding) parquet")
    e.add_argument("input")
    e.add_argument("id_col")
    e.add_argument("text_col")
    e.add_argument("output")
    e.add_argument("--batch-size", type=int, default=256)
    e.set_defaults(func=cmd_embed_parquet)

    args = p.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
