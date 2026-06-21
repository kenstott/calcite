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
import sys

MODEL_NAME = "snowflake/snowflake-arctic-embed-xs"
EMBEDDING_DIM = 384


def _model():
    # Imported lazily so `--help` works without the dependency installed.
    from fastembed import TextEmbedding
    return TextEmbedding(model_name=MODEL_NAME)


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
