# file/test-archive — strangler mining backlog

This directory sits **outside any Gradle source set**, so nothing here is compiled or run. It is the
holding area for the test-rebuild's strangler migration (see
[../../docs/testing/strategy.md](../../docs/testing/strategy.md) §7).

## How it works

1. A weak/`RECODE` test (see [coverage-map.yaml](../../docs/testing/coverage-map.yaml)) is **rewritten**
   to the golden standard as a new `@Tag("FILE-NNN")` class under `file/src/test/java/...`, linked in
   the ledger.
2. The weak original is **moved here** (a reversible `git mv`, not a delete) — its scenario and
   fixtures stay available for mining, and the active suite stops running the weak/flaky version.
3. **Deletion is matrix-gated:** a file may be removed from the archive only once every requirement
   that `supersedes` it is `complete` with a green golden. Until then it is preserved.

This preserves the hard-won domain knowledge in the legacy tests while keeping the active suite
trustworthy, with no coverage gap at any point.

## Status

Empty — populated as `RECODE` requirements are rebuilt (the next phase after the WRITE gaps close).
