# Airgapped desktop packaging (Phase 6, PGW-025–031)

Delivery is **per-OS installers** (Windows / macOS / Linux) — not Docker (PGW-025)
— that bundle a standalone CPython + a locked wheelhouse + the JVM + the Calcite
jars, and provision offline on first run (PGW-026/027). Nothing is fetched from
the network at install time (PGW-029).

## Primary delivery: package managers + tarballs (NO signing needed)

This is a **driver/CLI-server tool for technical users**, so the primary delivery
is package managers + tarballs, which are **not Gatekeeper/SmartScreen quarantined**
and therefore need **no OS code-signing** (PGW-030/031 revised):

- **macOS / Linux — Homebrew tap:** `brew install kenstott/tap/pgwire-file`. Brew-installed
  CLIs are not Gatekeeper-quarantined — no Apple Developer account / notarization required.
- **Windows — Scoop bucket:** `scoop install pgwire-file`. Scoop needs no Authenticode
  signing and is not SmartScreen-quarantined.
- Both ship one runtime formula (`pgwire-calcite-runtime`) plus one formula per pack
  (`pgwire-<adapter>`) that depends on it, generated at release by
  `runtime_packs.py formulas <out> <version> <sums-dir>` from `adapters.json`, `platforms.json`
  and the release's `.sha256` assets (see the layout section).
- **All — per-OS/arch tarball** GitHub release (`packaging/pack.sh`) + a one-line
  `curl | sh` install. Terminal installs aren't quarantined either.

Signed double-clickable **DMG/MSI is an OPTIONAL add-on** for non-technical
browser-download users only; its (credentialed) runbook is at the end.

## CI does the per-OS builds — no hardware of yours needed

Per-OS assembly runs on **GitHub-hosted runners** (macOS + Windows + Linux), so
you don't need those machines:

- **`.github/workflows/pgwire-calcite-ci.yml`** — runs the full test suite (builds
  the Calcite classpath + UDF jar, installs, `pytest`).
- **`.github/workflows/pgwire-calcite-release.yml`** — builds every adapter's jars once
  (pure Java, cross-platform) and keeps the ones they all hold, then a **matrix** job per
  OS/arch stages a standalone CPython (`uv python`, = python-build-standalone), installs the
  server into a venv (airgap-ready), jlinks a JRE, and packs the runtime.
- **`.github/workflows/pgwire-adapters-release.yml`** — calls the runtime build, then builds
  each adapter's pack against it, starts the file pack from the runtime on every platform, and
  attaches the runtime and the packs to the release.

The only step still gated on secrets is the **optional** signed DMG/MSI (Apple /
Windows signing identities) — not needed for the package-manager / `curl|sh`
delivery above.

> **What this repo provides vs. what needs credentials.**
> Buildable + committed: fail-loud airgap config (`airgap.py`, tested), wheelhouse
> builder (`build-wheelhouse.sh`) + pinned `requirements.lock`, tarball packer
> (`pack.sh`), Homebrew/Scoop manifests, and the **GitHub Actions** that build the
> per-OS bundles. Needs credentials ONLY for optional signed DMG/MSI.

## Layout: one runtime, one pack per adapter (REQ-2254)

Each engine release ships, per OS/arch, one **runtime** and one **pack** per adapter. The runtime
holds what every adapter needs; a pack holds what only its adapter needs and names the runtime
**line** it requires: an ABI number (`runtime-line`), bumped when the runtime's jar set or the
runtime/pack contract changes. A pack starts the newest runtime installed on its line, so a runtime
release on the same line ships with the packs unchanged. Each tarball holds its part of one tree, so extracting a runtime and a pack
into the same directory installs them (`src/pgwire_calcite/packs.py` documents the layout):

```
<root>/
├── runtimes/<version>/              pgwire-calcite-runtime-<version>-<os>-<arch>.tar.gz
│   ├── runtime.json                 {"name": "pgwire-calcite-runtime", "line", "version", "variant"}
│   ├── jre/                         jlinked JRE (every JDK module)
│   ├── python/ cpython/             python-build-standalone + a venv with pgwire_calcite (PGW-028)
│   ├── jars/                        the jars every adapter's classpath holds (calcite-core, avatica, ...)
│   ├── pgwire-launcher.jar          Java entry point (PGW-025)
│   └── bin/pgwire-calcite-runtime   starts a pack: --pack <pack-dir> [server args]
└── packs/<adapter>/<version>/       pgwire-<adapter>-pack-<version>-<os>-<arch>.tar.gz
    ├── pack.json                    {"name", "version", "runtime_line", "variant", "args"}
    ├── jars/                        the adapter's jars the runtime does not hold
    ├── model/model.json             the adapter's Calcite model (+ catalog cache, embedder model)
    ├── site-packages/               optional: Python packages only this adapter uses (govdata)
    └── bin/pgwire-<adapter>         starts the newest runtimes/<version> on its line; refuses to
                                     start, naming the line, when none is installed
```

The classpath is the runtime's `jars/` then the pack's `jars/`
(`PGWIRE_CALCITE_CLASSPATH_DIRS`); a jar in both, by name or as two versions of one artifact, is
refused at start, and `pack-jars` refuses the same at pack build time. The Python path is the runtime's
venv plus the pack's `site-packages/`. Each source still runs in its own server process. A
Python-only change ships as a new runtime; the packs are unchanged.

`runtime_packs.py` lays both out (`common-jars`, `pack-jars`, `runtime`, `pack`, `pack-extras`)
and writes the package-manager formulas (`formulas`);
`pgwire-calcite-release.yml` builds the runtime and `pgwire-adapters-release.yml` calls it and builds
the packs against it. `build-bundle.sh <adapter> <variant> <out-dir>` builds the same runtime and
one pack from a checkout.

Homebrew and Scoop install each formula in its own prefix. A pack formula's wrapper sets
`PGWIRE_CALCITE_RUNTIMES` to the runtime formula's stable directory
(`opt/pgwire-calcite-runtime/libexec/runtimes`, `apps\pgwire-calcite-runtime\current\runtimes`),
where the pack's launcher looks for the newest runtime on its line instead of `runtimes/` beside
the packs; upgrading the runtime formula leaves the pack formulas as they are.

Jars ship **exploded** (a `jars/` dir), not a fat jar (operational: per-jar Calcite pinning, clean
native signing, no shade/ServiceLoader fragility).

## Build steps

1. **Lock deps** (once, or when deps change):
   ```
   uv pip compile pyproject.toml --python-version 3.12 -o packaging/requirements.lock
   ```
2. **Fetch cross-platform wheelhouses** (build machine, needs network):
   ```
   packaging/build-wheelhouse.sh dist/wheelhouse
   ```
   Produces `dist/wheelhouse/<variant>/`. The vendored `buenavista` is built
   locally (`uv build vendor/buenavista`) and copied into each variant.
3. **Fetch standalone CPython** per variant from python-build-standalone, pinned to
   the same version/ABI as the wheelhouse (PGW-028).
4. **Assemble the Calcite `jars/`** via the classpath task
   (`scripts/print-calcite-classpath.gradle`) → copy the resolved jars +
   `vendor/jars/arrow-jdbc-17.0.0.jar` into `jars/`.
5. **First-run provisioning** (offline, at install/first-launch): the launcher
   installs the wheelhouse into the bundled CPython with **no index**:
   ```
   uv pip install --no-index --find-links wheelhouse/<variant> -r requirements.lock
   ```
6. **Offline verification (CI, PGW-029)** — on a network-disconnected runner,
   provision from the bundle and run the suite **including rarely-used paths**;
   assert `pgwire_calcite.airgap.assert_offline_ready` on the catalog DuckDB.

## Signing / notarization — NON-GOAL (PGW-031)

There is no OS code-signing, notarization, or signed DMG/MSI step. Delivery is via
package manager / `curl | sh` / tarball, which is **not** Gatekeeper/SmartScreen
quarantined, so no signing identities are required. The only platform concern that
remains: Linux wheels must match the glibc baseline (manylinux2014; musllinux if
Alpine is a target).

## Design note: Java supervisor vs. the current Python supervisor

PGW-025/033 envision the **top-level launcher as a Java app** that provisions
CPython and supervises the children. Phases 0–5 implemented a **Python** supervisor
(`supervisor.py`) + Python pgwire + a Calcite JVM child. Both satisfy "OS-agnostic,
Calcite in its own recyclable child." To match PGW-025 exactly, the installer's
`bin/pgwire-calcite` can be a thin **Java** launcher that (a) provisions CPython
on first run and (b) starts the Python supervisor, which in turn manages the
pgwire + Calcite-child processes. That thin Java shim is the remaining Phase-6
code (small); the supervision logic already exists in Python.
