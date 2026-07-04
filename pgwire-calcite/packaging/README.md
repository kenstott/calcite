# Airgapped desktop packaging (Phase 6, PGW-025–031)

Delivery is **per-OS installers** (Windows / macOS / Linux) — not Docker (PGW-025)
— that bundle a standalone CPython + a locked wheelhouse + the JVM + the Calcite
jars, and provision offline on first run (PGW-026/027). Nothing is fetched from
the network at install time (PGW-029).

## Primary delivery: package managers + tarballs (NO signing needed)

This is a **driver/CLI-server tool for technical users**, so the primary delivery
is package managers + tarballs, which are **not Gatekeeper/SmartScreen quarantined**
and therefore need **no OS code-signing** (PGW-030/031 revised):

- **macOS / Linux — Homebrew tap:** `brew install kenstott/tap/pgwire-calcite`
  (formula: `packaging/homebrew/pgwire-calcite.rb`). Brew-installed CLIs are not
  Gatekeeper-quarantined — no Apple Developer account / notarization required.
- **Windows — Scoop bucket:** `scoop install pgwire-calcite`
  (manifest: `packaging/scoop/pgwire-calcite.json`). Scoop needs no Authenticode
  signing and is not SmartScreen-quarantined.
- **All — per-OS/arch tarball** GitHub release (`packaging/pack.sh`) + a one-line
  `curl | sh` install. Terminal installs aren't quarantined either.

Signed double-clickable **DMG/MSI is an OPTIONAL add-on** for non-technical
browser-download users only; its (credentialed) runbook is at the end.

## CI does the per-OS builds — no hardware of yours needed

Per-OS assembly runs on **GitHub-hosted runners** (macOS + Windows + Linux), so
you don't need those machines:

- **`.github/workflows/pgwire-calcite-ci.yml`** — runs the full test suite (builds
  the Calcite classpath + UDF jar, installs, `pytest`).
- **`.github/workflows/pgwire-calcite-release.yml`** — builds the Calcite jars once
  (pure Java, cross-platform), then a **matrix** job per OS/arch stages a
  standalone CPython (`uv python`, = python-build-standalone), installs the app +
  deps into a bundled venv (airgap-ready), jlinks a minimal JRE, assembles the
  bundle, tars + sha256s it, uploads artifacts, and attaches them to the release.

The only step still gated on secrets is the **optional** signed DMG/MSI (Apple /
Windows signing identities) — not needed for the package-manager / `curl|sh`
delivery above.

> **What this repo provides vs. what needs credentials.**
> Buildable + committed: fail-loud airgap config (`airgap.py`, tested), wheelhouse
> builder (`build-wheelhouse.sh`) + pinned `requirements.lock`, tarball packer
> (`pack.sh`), Homebrew/Scoop manifests, and the **GitHub Actions** that build the
> per-OS bundles. Needs credentials ONLY for optional signed DMG/MSI.

## Layout of a shipped artifact (per OS/arch variant)

```
pgwire-calcite-<os>-<arch>/
├── jre/                     # bundled JRE (OS-agnostic Java app, PGW-025)
├── cpython/                 # python-build-standalone, pinned version/ABI (PGW-028)
├── wheelhouse/              # locked platform wheels for THIS variant (PGW-027)
├── jars/                    # Calcite deps/ (exploded, not a fat jar — see design note)
│   └── arrow-jdbc-17.0.0.jar
├── natives/                 # signed native libs (duckdb, pyarrow, arrow) (PGW-031)
├── model/                   # Calcite model(s): file / govdata / … presets
└── bin/pgwire-calcite       # launcher: provisions on first run, then serves
```

Per PGW-030 the preferred form is **per-OS installers (~150 MB each, own variant
only)** over a universal ~600 MB–1 GB artifact; ~1 GB total is the accepted airgap
tax. Jars ship **exploded** (a `jars/` dir), not a fat jar — see the fat-jar-vs-dir
design note (operational: per-jar Calcite pinning, clean native signing, no
shade/ServiceLoader fragility).

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

## Signing / notarization — REQUIRES YOUR CREDENTIALS (PGW-031)

These commands are the runbook; they must run on your machines with your identities.

- **macOS** (Developer ID + notarization):
  ```
  codesign --deep --force --options runtime \
    --sign "Developer ID Application: <YOU>" pgwire-calcite-macos-arm64/natives/*.dylib
  xcrun notarytool submit pgwire-calcite-macos-arm64.zip \
    --apple-id <APPLE_ID> --team-id <TEAM> --password <APP_SPECIFIC_PW> --wait
  xcrun stapler staple pgwire-calcite-macos-arm64.pkg
  ```
- **Windows** (Authenticode, Gatekeeper/SmartScreen):
  ```
  signtool sign /fd SHA256 /tr <RFC3161_TSA> /td SHA256 \
    /f <YOUR.pfx> /p <PW> pgwire-calcite-windows-x86_64\bin\pgwire-calcite.exe
  ```
- **Linux**: no OS signing; ensure wheels match the glibc baseline (manylinux2014;
  musllinux if Alpine is a target) (PGW-031).

## Design note: Java supervisor vs. the current Python supervisor

PGW-025/033 envision the **top-level launcher as a Java app** that provisions
CPython and supervises the children. Phases 0–5 implemented a **Python** supervisor
(`supervisor.py`) + Python pgwire + a Calcite JVM child. Both satisfy "OS-agnostic,
Calcite in its own recyclable child." To match PGW-025 exactly, the installer's
`bin/pgwire-calcite` can be a thin **Java** launcher that (a) provisions CPython
on first run and (b) starts the Python supervisor, which in turn manages the
pgwire + Calcite-child processes. That thin Java shim is the remaining Phase-6
code (small); the supervision logic already exists in Python.
