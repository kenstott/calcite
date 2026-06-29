#!/usr/bin/env python3
"""Link tests + flip status to complete for a batch of file-adapter requirements.

Reads a JSON map {id: [test-refs]} from docs/testing/_links.json and patches docs/requirements/file.yaml
in place, format-preserving (block-based line patching): sets `status: complete`, replaces the `tests:`
block with the given refs, and adds `since: 2026-06` if absent. Idempotent on already-linked reqs.

Usage:
  echo '{"FILE-120": ["file/StatisticsRequirementsTest"]}' > docs/testing/_links.json
  python3 docs/testing/link.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs" / "requirements" / "file.yaml"
LINKS = json.loads((ROOT / "docs" / "testing" / "_links.json").read_text(encoding="utf-8"))

lines = LEDGER.read_text(encoding="utf-8").split("\n")
out: list[str] = []
i, n = 0, len(lines)
linked = []
while i < n:
    m = re.match(r"^- id: (FILE-\d+)\s*$", lines[i])
    if not m or m.group(1) not in LINKS:
        out.append(lines[i]); i += 1; continue
    rid = m.group(1)
    block = [lines[i]]; i += 1
    while i < n and not lines[i].startswith("- id: ") and not lines[i].startswith("#"):
        block.append(lines[i]); i += 1
    has_since = any(re.match(r"^  since:", b) for b in block)
    patched: list[str] = []
    j = 0
    while j < len(block):
        b = block[j]
        if re.match(r"^  status:", b):
            patched.append("  status: complete"); j += 1; continue
        if re.match(r"^  tests:", b):
            j += 1
            while j < len(block) and re.match(r"^    - ", block[j]):
                j += 1
            patched.append("  tests:")
            for t in LINKS[rid]:
                patched.append("    - " + t)
            if not has_since:
                patched.append("  since: 2026-06")
            continue
        patched.append(b); j += 1
    out.extend(patched)
    linked.append(rid)

LEDGER.write_text("\n".join(out), encoding="utf-8")
print("linked + completed:", ", ".join(linked))
