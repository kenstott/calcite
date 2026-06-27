#!/bin/bash
#
# Builds the self-contained hugot embedding server binary.
#
# This is the AIRGAPPED, in-jar query-time embedder: a single static-ish Go binary
# that loads an ONNX sentence-embedding model and answers one JSON request per line
# on stdin with one JSON response per line on stdout — the protocol
# org.apache.calcite.adapter.file.similarity.EmbeddingService speaks.
#
#   request:  {"text": "<query>"}
#   response: {"embedding": [<floats>]}   | {"error": "<message>"}
#
# Why Go/hugot: it bundles tokenization + ONNX inference + pooling + normalization
# (matching the Python sentence-transformers/fastembed pipeline) with NO language
# runtime to vendor — unlike CPython (heavy) or ONNX-in-Java (must reimplement the
# tokenizer/pooling). knights-analytics/hugot's FeatureExtractionPipeline does it.
#
# ---------------------------------------------------------------------------
# Build dependencies (build host only; the airgapped runtime ships just the
# compiled binary + libonnxruntime.so + the model):
#   - Go >= 1.21
#   - a C compiler (cgo); hugot links onnxruntime + tokenizers via cgo
#   - libtokenizers.a  (prebuilt: github.com/daulet/tokenizers releases, matching
#                        the version hugot pins — currently v1.27.0, linux-amd64)
#   - ONNX Runtime is loaded at RUNTIME (libonnxruntime.so), not linked here.
#
# Env knobs:
#   TOKENIZERS_LIB_DIR  dir containing libtokenizers.a   (required)
#   CC                  C compiler (default: cc)
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

: "${TOKENIZERS_LIB_DIR:?set TOKENIZERS_LIB_DIR to the dir containing libtokenizers.a}"

export CGO_ENABLED=1
export CGO_LDFLAGS="-L${TOKENIZERS_LIB_DIR}"
export CC="${CC:-cc}"

OUT="${1:-hugot-embed}"

# -tags ORT enables hugot's onnxruntime backend (otherwise NewORTSession errors
# with "to enable ORT, run go build -tags ORT").
go build -tags ORT -o "$OUT" .

echo "built $OUT"
echo
echo "RUN (note: ORT_LIB_PATH is the DIRECTORY containing libonnxruntime.so, NOT"
echo "the .so file itself — hugot's WithOnnxLibraryPath wants a directory):"
echo "  ORT_LIB_PATH=/path/to/dir_with_libonnxruntime_so \\"
echo "  EMBED_MODEL_PATH=/path/to/model_dir \\"
echo "  LD_LIBRARY_PATH=/path/to/onnxruntime_libs \\"
echo "  ./$OUT"
echo
echo "The model dir needs model.onnx + tokenizer.json + config.json (a mean-pooled"
echo "sentence-transformers model, e.g. all-MiniLM-L6-v2; hugot mean-pools + L2-"
echo "normalizes — it does NOT do CLS pooling, so CLS models like arctic-embed-xs"
echo "will NOT match their reference vectors)."
