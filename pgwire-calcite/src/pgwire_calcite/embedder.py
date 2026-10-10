# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Query-time text embedding, shared across every client connected to this server.

The point of running this here rather than in each MCP client process: a Claude
Desktop install with two client processes (or a hundred, on a shared multi-user
server) shares exactly ONE loaded embedding model, not one per process — the same
reasoning that put the DuckDB catalog itself behind this shared server
(kenstott/calcite#364).

What this embedder serves, and what it does not: only ``EMBED('literal')`` in SQL, which
transforms.py rewrites to a vector before the statement reaches Calcite. ``SEMANTIC_SEARCH``
and the Java ``EMBED`` function embed inside this server's JVM, through the file adapter's
OnnxClsEmbedder (CLS pooling, L2-normalized, the int8 model carried in the file adapter's
jar), which was built to reproduce the corpus vector space. The AskAmerica engine carries no
embedder of its own: it sends both to this server as SQL.

The corpus vectors are produced by govdata/scripts/vss-local.py with sentence-transformers
(snowflake/snowflake-arctic-embed-xs, 384-d, L2-normalized); govdata/scripts/embed.py uses the
same pipeline. This module uses fastembed (onnxruntime-backed, no torch) with the same
model, because torch is not something every client machine should have to install.
fastembed runs its own ONNX export of that model with its own pooling defaults and is not
guaranteed to numerically match the corpus pipeline; that agreement has not been measured.

Fully offline at runtime: ``local_files_only=True`` makes fastembed refuse to touch
the network at all, using only what is already present in EMBED_MODEL_CACHE_DIR
(populated once, with network access, when the pgwire-govdata release bundle is
built — see pgwire-adapters-release.yml). Confirmed live: without
local_files_only, fastembed still attempts a HuggingFace Hub reachability check
even with a fully warm cache and HF_HUB_OFFLINE=1 set, and hangs for ~40s across
three retries before failing outright — unacceptable for an airgapped client.
"""

from __future__ import annotations

import os
from typing import List, Optional

_model = None


def _cache_dir() -> Optional[str]:
    return os.environ.get("EMBED_MODEL_CACHE_DIR")


def _model_name() -> str:
    return os.environ.get("VSS_EMBED_MODEL", "snowflake/snowflake-arctic-embed-xs")


def is_available() -> bool:
    """Whether a bundled model cache is configured — checked before ever importing
    fastembed, so a server without the bundle (file/splunk/sharepoint/cloudops
    variants, which have no need for it) pays no import cost."""
    cache_dir = _cache_dir()
    return cache_dir is not None and os.path.isdir(cache_dir)


def _get_model():
    global _model
    if _model is None:
        cache_dir = _cache_dir()
        if not cache_dir:
            raise RuntimeError(
                "EMBED_MODEL_CACHE_DIR is not set -- no bundled embedder available "
                "on this server"
            )
        from fastembed import TextEmbedding

        _model = TextEmbedding(
            model_name=_model_name(), cache_dir=cache_dir, local_files_only=True
        )
    return _model


def embed_text(text: str) -> List[float]:
    """One 384-d, L2-normalized embedding vector for ``text``."""
    model = _get_model()
    vec = next(iter(model.embed([text])))
    return [float(x) for x in vec]
