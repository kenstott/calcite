#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# vss-embed-setup.sh — one-time setup of the LOCAL CPU embedding venv used by
# vss-local.py. Replaces the remote-Vultr embedding stack: embeddings are now
# generated on the ETL box's CPU (the model, snowflake-arctic-embed-xs, is tiny).
#
# Uses python3.12 (torch / sentence-transformers ship CPU wheels for it; the box's
# default python3 is 3.14, which currently has no torch wheels). Idempotent: skips
# creation if the venv already imports the required packages.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
VENV="${VSS_EMBED_VENV:-$GOVDATA_HOME/build/.venv-embed}"
PYBIN="${VSS_EMBED_PYTHON:-python3.12}"

if [ -f "$VENV/bin/python" ] && "$VENV/bin/python" - <<'PY' >/dev/null 2>&1
import torch, sentence_transformers, duckdb, pyarrow  # noqa
PY
then
  echo "Embed venv already provisioned: $VENV"
  "$VENV/bin/python" - <<'PY'
import torch, sentence_transformers, duckdb
print("  torch", torch.__version__, "| sentence-transformers", sentence_transformers.__version__, "| duckdb", duckdb.__version__)
PY
  exit 0
fi

command -v "$PYBIN" >/dev/null || { echo "ERROR: $PYBIN not found"; exit 1; }
echo "Creating embed venv at $VENV using $PYBIN ($($PYBIN --version 2>&1))"
mkdir -p "$(dirname "$VENV")"
"$PYBIN" -m venv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install -q --upgrade pip
echo "Installing torch (CPU-only wheel)..."
pip install -q torch --index-url https://download.pytorch.org/whl/cpu
echo "Installing sentence-transformers, duckdb, pyarrow..."
pip install -q sentence-transformers "duckdb>=1.3,<1.4" pyarrow

echo "Verifying imports..."
python - <<'PY'
import torch, sentence_transformers, duckdb, pyarrow
print("OK: torch", torch.__version__, "| sentence-transformers", sentence_transformers.__version__, "| duckdb", duckdb.__version__)
print("CUDA available:", torch.cuda.is_available(), "(expected False on the CPU ETL box)")
PY
echo "Embed venv ready: $VENV"
