#!/bin/bash
# Test: fix-branch.sh merge after a rebase removes branch, worktree and DQ mark, and prunes the remote
# branch only once origin/main contains it. Runs entirely in a temp repo with a temp bare origin.
set -u
D="$(cd "$(dirname "$0")" && pwd)"; SRC="$D/../fix-branch.sh"
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
fail=0; ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
git init -q --bare "$T/origin.git"
R="$T/repo/calcite"; mkdir -p "$R/govdata/scripts"; git init -q -b main "$R"
cp "$SRC" "$R/govdata/scripts/fix-branch.sh"
cd "$R" && echo a > a && git add -A && git commit -qm base && git remote add origin "$T/origin.git" && git push -q origin main
S="$R/govdata/scripts/fix-branch.sh"
"$S" start t1 >/dev/null 2>&1; WT="$T/repo/calcite-fixes/t1"
echo f > "$WT/f" && git -C "$WT" add f && git -C "$WT" commit -qm fix && git -C "$WT" push -q origin fix/t1
echo b > b && git add b && git commit -qm advance   # main moves, so the script must rebase
"$S" dq-passed t1 >/dev/null
out=$("$S" merge t1 2>&1); rc=$?
ck "merge exits 0 after a rebase"                    '[ $rc -eq 0 ]'
ck "merged line printed"                             'echo "$out" | grep -q "^merged fix/t1 into main"'
ck "local branch deleted"                            '! git rev-parse -q --verify fix/t1 >/dev/null'
ck "worktree removed"                                '[ ! -d "$WT" ]'
ck "DQ mark removed"                                 '[ ! -e "$(git rev-parse --git-common-dir)/fix-branch-dq/t1" ]'
ck "remote kept while origin/main lacks the commits" 'git ls-remote --exit-code --heads origin fix/t1 >/dev/null'
git push -q origin main; git fetch -q origin
"$S" prune-remote t1 >/dev/null 2>&1
ck "remote deleted once in origin/main"              '! git ls-remote --exit-code --heads origin fix/t1 >/dev/null'
exit $fail
