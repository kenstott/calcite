#!/usr/bin/env bash
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
# Exercises the lead-only and CI gates of fix-branch.sh merge in a throwaway repo with a stub gh.
set -uo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/fix-branch.sh"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
pass=0; fail=0
ok()  { echo "PASS $1"; pass=$((pass+1)); }
bad() { echo "FAIL $1"; fail=$((fail+1)); }

setup() {   # fresh origin + main tree + branch fix/x with one commit, DQ recorded
  rm -rf "$T/o.git" "$T/calcite" "$T/calcite-fixes"
  git init -q --bare "$T/o.git"
  git init -q -b main "$T/calcite"; mkdir -p "$T/calcite/govdata/scripts" "$T/calcite/.claude/lead"
  cp "$SRC" "$T/calcite/govdata/scripts/fix-branch.sh"
  git -C "$T/calcite" config user.email t@t; git -C "$T/calcite" config user.name t
  git -C "$T/calcite" remote add origin "$T/o.git"
  echo base > "$T/calcite/f"; git -C "$T/calcite" add -A; git -C "$T/calcite" commit -qm base
  git -C "$T/calcite" push -q origin main
  FB="$T/calcite/govdata/scripts/fix-branch.sh"
  WT="$(bash "$FB" start x 2>/dev/null)"
  echo change > "$WT/g"; git -C "$WT" add g; git -C "$WT" commit -qm change
  bash "$FB" dq-passed x >/dev/null
}
stub_gh() {  # each argument is one "name<TAB>conclusion" line the stub prints
  mkdir -p "$T/bin"
  { echo '#!/bin/sh'; local l; for l in "$@"; do printf "printf '%%s\\\\n' '%s'\n" "$l"; done; } > "$T/bin/gh"
  chmod +x "$T/bin/gh"
}
J21="Linux (JDK 21)"
merge() { PATH="$T/bin:$PATH" bash "$FB" merge x 2>&1; }

P="git -C \"\$WT\" push -q origin fix/x"
pushit() { git -C "$WT" push -q origin fix/x; }
enf() { echo enforce > "$T/calcite/.claude/lead/mode"; }

setup; enf; stub_gh "$J21	success"
out="$(merge)"; echo "$out" | grep -q "not pushed" && ok "refuses an unpushed branch" || bad "unpushed: $out"

setup; enf; pushit; stub_gh "Validate Gradle Wrapper	success"
out="$(merge)"; echo "$out" | grep -q "required CI check is not on" && ok "refuses when only a light check ran (no PR CI)" || bad "light: $out"

setup; enf; pushit; stub_gh "$J21	pending"
out="$(merge)"; echo "$out" | grep -q "not green" && ok "refuses while the check is still running" || bad "pending: $out"

setup; enf; pushit; stub_gh "$J21	success" "Windows (JDK 17)	failure"
out="$(merge)"; echo "$out" | grep -q "not green" && ok "refuses when any check failed" || bad "failed: $out"

setup; enf; pushit; stub_gh "$J21	success" "Windows (JDK 17)	skipped"
out="$(merge)"; echo "$out" | grep -q "merged fix/x" && ok "merges when CI is green" || bad "green: $out"

setup; enf; pushit; stub_gh "$J21	success"
out="$(CLAUDE_ROLE=worker merge)"; echo "$out" | grep -q "lead's step" && ok "refuses a worker" || bad "worker: $out"

setup; enf; pushit; stub_gh "$J21	success"
echo more > "$T/calcite/h"; git -C "$T/calcite" add h; git -C "$T/calcite" commit -qm moved
out="$(merge)"; echo "$out" | grep -q "changed the commit CI validated" && ok "refuses when the rebase changes the validated commit" || bad "rebase: $out"

setup; echo shadow > "$T/calcite/.claude/lead/mode"
out="$(merge)"; echo "$out" | grep -q "merged fix/x" && echo "$out" | grep -q "WARN (shadow" && ok "shadow mode warns and still merges" || bad "shadow: $out"

echo "$pass passed, $fail failed"; [ "$fail" -eq 0 ]
