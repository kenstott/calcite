# AskAmerica Engine — Distribution Design

Status: design, pre-implementation. Captures the packaging decisions for getting
the engine into the hands of two very different audiences without an installer the
target user can't run or a download their network won't allow.

## The problem in one line

The engine is a 722 MB shaded fat jar today. Enterprise developers on managed
workstations generally **cannot run native installers** (`.dmg`/`.msi`/`.exe` need
admin rights and software-approval) but **can pull packages** through their
sanctioned dev channels (Artifactory-proxied Maven / PyPI). So the deliverable must
be a *dependency*, not something you *install* — and it must fit the size and trust
constraints of those channels.

## Two audiences, two doors

Distribution splits cleanly by which JVM the code runs in:

| Door | Artifact | Experience | We provide |
|---|---|---|---|
| **pip** (PyPI) | unshaded jar-set → wheels | **MCP-first** (+ programmatic SQL) | guided first-run, MCP config generation, key flow |
| **Maven Central** | shaded fat jar | JDBC driver | a coordinate + 4 connection facts, in docs |

The distinction is not arbitrary — it follows from *why the fat jar is shaded*:

- **Shading is only required when the classes drop into someone else's JVM** —
  the JDBC-driver-in-Tableau/DBeaver/Spring/Trino case. There, relocation/merge keep
  your Calcite/Guava/Jackson from colliding with the host app. Maven Central is that
  audience's native channel.
- **The pip / MCP path runs its own dedicated JVM via JPype.** There is no host
  classpath to collide with, so shading buys nothing — and we can ship plain,
  unshaded jars (see "Wheel packaging"). The fat jar is an artifact of the JDBC
  distribution that was inherited by the Python one for no real reason.

### "JDBC" means two different things

- **JDBC-style querying *from Python*** (JPype loads the driver into Python's own JVM;
  `connect()` → run SQL). The wheel-set already carries the driver classes, so pip
  covers this for free. Keep it — it's the natural notebook use case.
- **A JDBC driver *JAR* for a foreign JVM** (BI tools, Spring, Trino). This is the
  Maven fat jar. pip does **not** try to deliver it; the pip first-run "connecting a
  BI tool?" option is a *link to the Maven coordinate*, not an install action.

Guided-onboarding effort is spent in one place — pip → MCP-first. The Maven/JDBC path
is *documented*, not shepherded: the BI persona is defined by already knowing how to
consume a driver. Hand them the Maven coordinate, driver class name, JDBC URL format,
and creds properties (API key or `AWS_*`) — that is the entire, standard integration
surface.

## Current artifact (as-built)

- Shadow (fat) jar, primarily a JDBC driver (`jdbc:askamerica:` →
  `AskAmericaDriver` → `GovDataDriver`), secondary MCP server entry point.
- Coordinates `ai.askamerica:askamerica-engine`; **Business Source License 1.1**
  (not Apache). Publishes on tag `engine-v*` to Maven Central, GitHub Packages, and
  GitHub Releases (`.github/workflows/askamerica-engine.yml`).
- Python package `askamerica` lives in a **separate repo** (`kenstott/askamerica`),
  published to PyPI. Today `pip install 'askamerica[engine]'` uses JPype and
  **downloads the engine JAR from the GitHub release at runtime** into
  `~/.askamerica/engine/` — see "Known gaps".
- jpackage native installers (`.pkg`/`.deb`/`.msi`) exist
  (`build.gradle.kts:276-328`); the installer's postinstall *also* downloads the
  engine JAR from the GitHub release. These target the own-machine consumer, **not**
  the locked-down enterprise dev.

## Runtime data access (independent of install)

Solving the install channel does **not** solve runtime data reach. There are three
distinct egress endpoints, not one:

1. **Artifact source** — Maven Central / PyPI (proxyable) *or* github.com release CDN
   (the current Python/installer path; the release asset serves from a different host
   than github.com and is a common allowlist miss).
2. **Credential broker** — default creds are fetched from
   `askamerica.ai/v1/catalog/credentials` via `ASKAMERICA_API_KEY`
   (`R2CredentialProvider.java:50-141`). A proxy can open S3 and still block this,
   which surfaces as a mysterious auth failure.
3. **Data** — default `s3://govdata-parquet-v1` (Cloudflare R2), read directly by
   DuckDB `httpfs` (`GovDataDriver.java:400-409`).

The storage location and creds are **already first-class operands**, so no refactor
is needed to support enterprises:

- `AWS_ENDPOINT_OVERRIDE` + `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
  (`GovDataDriver.java:355-386`) → self-hosted S3/MinIO, and **bypasses the
  askamerica.ai broker entirely**.
- `GOVDATA_PARQUET_DIR` or a `directory` operand → local filesystem.

**Action:** document a **"bring-your-own-bucket" enterprise profile**
(`AWS_*` + `GOVDATA_PARQUET_DIR`) that sidesteps both the broker and the public
bucket in one move. Watch for **TLS interception** — corporate MITM proxies re-sign
certs; DuckDB `httpfs` / the S3 client rejects them unless the corporate CA is
trusted.

## Fat jar slimming

The 722 MB is mostly dead weight, and it hurts *every* channel (Maven Central,
GitHub release, and any wheel). Slim first, regardless of packaging.

Investigated breakdown (on-disk / compressed MB — what actually shrinks the file):

| Item | On-disk | Where it enters | Notes |
|---|---|---|---|
| AWS SDK v1 classes `com/amazonaws/**` | 192 | `hadoop-aws:3.3.6` → `aws-java-sdk-bundle:1.12.367` (`file/build.gradle.kts:70`, `govdata/build.gradle.kts:133`) + direct `aws-java-sdk-s3` | duplicate of AWS v2 |
| Arrow Gandiva natives `gandiva_jni/**` | 118 | `arrow-gandiva` (`file/build.gradle.kts:56`) | verify used on read path |
| Orphaned ONNX model `models/all-MiniLM-L6-v2/**` | 79 | DJL tokenizers; **consuming code already excluded** (`build.gradle.kts:110`) | pure orphan |
| DuckDB natives (4 platforms) | 77 | `duckdb_jdbc:1.4.4.0` (transitive via govdata) | all platforms in one jar |
| AWS SDK v1 codegen JSON `models/*-{intermediate,model}.json` | 68 | aws-java-sdk-bundle | never loaded at runtime |
| Orphaned tokenizer natives `native/lib/**` | 16 | DJL tokenizers | orphaned with ONNX |
| AWS SDK v2 `software/amazon/awssdk/**` | 6 | `iceberg-aws` | **keep** — Iceberg needs it |
| PostgreSQL driver | 2 | `file/build.gradle.kts:37` (write-path tracker) | removable for read-only |

Hypotheses tested: **no** multiple DB drivers/embedded DBs (clean except 2 MB
Postgres); **yes** massive Hadoop/AWS write-path bloat; **partial** duplication (AWS
v1 *and* v2 both shaded).

Removal tiers:

| Tier | What | Reclaim | Jar → | Risk |
|---|---|---|---|---|
| **1 — do now** | orphaned ONNX + tokenizer natives + AWS-v1 codegen JSON | 163 MB | ~560 MB | zero — consuming code already excluded / JSON never loaded |
| **2 — blocked** | drop AWS SDK v1 classes | +192 MB | ~370 MB | **BLOCKED** — the read path uses hadoop `s3a` (AWS v1) via `IcebergTable`/`HadoopTables`; needs an `S3FileIO` (v2) migration first (see below) |
| **2a — safe now** | AWS-v1 codegen JSON only (`models/*-{intermediate,model}.json`) | +68 MB | ~490 MB | none — build artifacts, never loaded at runtime; the v1 classes stay |
| **3 — applied** | Arrow Gandiva (natives + classes) | +118 MB | ~441 MB | none on the read path — reached only via `FileSchema.createArrowTable()`, which is `.arrow`-file-only and loads the arrow adapter by **reflection** ("to avoid hard dependency"); govdata has no `.arrow` sources |
| **4** | strip non-target-platform natives | +~130 MB | ~120–150 MB | single-platform deploy only |

**Tier-2 verification result (blocked).** The Iceberg *read* path uses hadoop `s3a`
(AWS SDK v1), not `S3FileIO` (v2). Confirmed at `IcebergTable.java:105-112` (sets
`fs.s3a.impl = S3AFileSystem` + `SimpleAWSCredentialsProvider` at query time),
`IcebergCatalogManager.java:67,157` (`HadoopTables`/`HadoopCatalog`), and
`FileSchema.java:5537` (`table.newScan().planFiles()` plans metadata through hadoop
FS). DuckDB httpfs reads the parquet *data*, but Iceberg *metadata* planning runs on
s3a, so dropping `com.amazonaws` wholesale breaks reads. Prerequisite: migrate
`IcebergTable` + `IcebergCatalogManager` from `HadoopFileIO`/`HadoopTables` (s3a/v1)
to Iceberg `S3FileIO` (v2). Until then only the codegen JSON (Tier-2a) is reclaimable
— those `models/*.json` files are AWS SDK build artifacts never loaded at runtime, so
they drop via a shadowJar `exclude` while the v1 classes stay intact.

## Wheel packaging (pip door)

The pip audience runs a dedicated JVM, so **drop the fat jar and ship the unshaded
jars as a set of wheels**. This is a verified clean drop-in — no mechanism makes
side-by-side unshaded jars behave differently from the shaded fat jar in the JPype
path:

- Driver registration is `Class.forName`/static-init self-registration
  (`AskAmericaDriver.java:38`, `GovDataDriver.java:62`), not merged service files.
  The one `ServiceLoader` consumer (`McpServerLauncher.java:54`) unions across the
  classpath natively via `getResources()`, so `mergeServiceFiles()` is a
  single-artifact convenience, not a correctness requirement. No first-wins
  single-resource reads of any service file exist.
- **No** package relocation anywhere in the build.
- **No** `append`/`transform`/`reference.conf`/`Log4j2Plugins.dat`/`spring.*` content
  that needs cross-jar merging.
- **No** split packages across the engine's own modules (disjoint package roots).
- Shading *strips* jar signatures (`exclude META-INF/*.SF/.RSA/.DSA`); the unshaded
  set *preserves* them — better provenance.

### Why not one universal wheel, or platform-stripping

- The platform-independent JVM core is ~113 MB *after* slimming — already over public
  PyPI's 100 MB per-file limit **before adding any native**. So platform-stripping
  alone (which only touches natives) cannot get a single wheel under 100 MB.
- Opaque byte-split-and-reassemble is rejected: a reconstituted-then-executed blob
  defeats the SCA scanning that is half the reason to use PyPI, and reads as a
  supply-chain smell.

### Partition plan

Slim (drop AWS v1 + orphans, ~355 MB) → emit the resolved `runtimeClasspath` jars
unshaded → partition the ~150 MB legitimate remainder:

- `askamerica-engine-core` (universal `py3-none-any`) — calcite + iceberg +
  arrow-java + engine glue (~65 MB)
- `askamerica-engine-runtime` (universal) — netty/jetty/jackson/guava/commons/
  hadoop-common/aws-v2 (~55 MB)
- `askamerica-engine-duckdb-<platform>` (platform-tagged) — one native (~18 MB each)

The top wheel pins exact versions of the others (`core==X` requires `runtime==X`);
pip resolves the graph and pulls the right platform native by wheel tag; a Python
launcher globs every installed jar onto the JPype classpath. Universal wheels build
**once**; only the small native wheels are per-platform — cleaner than rebuilding a
stripped fat jar per OS. Each file is under 100 MB → **no PyPI limit-increase
request, no opaque bins**.

Note: sizes above are estimates apportioned from 4-platform compressed totals (±~20
MB). Enterprise Artifactory does not enforce 100 MB, so the *enterprise* tier can
alternatively take a single slimmed universal wheel; the partition is what the
*community* public-PyPI tier needs.

## First-run flow (pip)

Gate on **creds present, not specifically the free key** — otherwise the BYO-bucket
enterprise user (who has `AWS_*` and no askamerica key) is locked out at the funnel.

- **No creds** → offer *both* routes: "Get a free key at askamerica.ai" **and**
  "Already have your own bucket? Set `AWS_ENDPOINT_OVERRIDE` + `AWS_*`."
- **Creds present** (askamerica key *or* `AWS_*`) → show the intent menu, framed by
  what the user is doing, not by protocol, and additive (do one now, others later):
  - **Use with your AI assistant (MCP)** — hero; generate the client config entry.
  - **Query from Python directly (SQL)** — a 3-line snippet for the current REPL.
  - **Connect a BI tool (JDBC)** — a *link* to the Maven coordinate + URL, not an
    install.

Mechanics:

- Do **not** hang a blocking wizard off `import` (breaks notebooks/scripts). Make
  setup a dedicated command (`askamerica setup`); on import emit at most a one-line
  hint if unconfigured.
- Detect context: interactive TTY → prompt; notebook/REPL → rich HTML/print with
  copy-paste snippets.
- "Install as MCP" generates (and optionally writes, with confirmation) the MCP
  client config — which only works if the launch command is fully self-contained
  (the wheel-set, no runtime JAR download).

## Website download page (post-key)

Structure **experience-first → persona → OS**, not a flat `dmg/exe/deb/pip` list
(three of those are the same persona, one is different):

- **Hero: Connect to your AI assistant (MCP)** — this is the primary experience;
  don't bury it inside an installer.
- **Desktop app** — OS-auto-detected installer for the own-machine consumer who *can*
  run installers (it auto-configures MCP for them).
- **Developer / `pip`** — for the Artifactory crowd who can't run an installer.

This page is the **community/self-serve funnel**. The BYO-bucket / no-free-key
enterprise user does not come through here; that path (self-hosted, procured) lives
elsewhere and never requires the free key.

## Licensing / tiering

The engine is **BSL 1.1**. Enterprise OSS-review boards scrutinize (some reject) BSL
by policy. If a target account blocks BSL at OSS intake, the frictionless
`pip/mvn install` motion is the *wrong door* there regardless of packaging — that
account comes through **commercial license + procurement** (which BSL is designed
for). Implication: a two-tier model —

- **Community tier** (permissive) → the self-serve PyPI funnel + rich first-run.
- **Enterprise tier** (BSL/commercial) → procured artifact (internal Artifactory
  feed, or an internal MCP registry entry) delivered post-contract.

MCP fits both doors: `uvx`/self-serve for community, internal MCP registry for
procured enterprise.

## Implementation status (this pass)

Landed and validated locally:

- **Slimming Tier-1 + Tier-2a + Tier-3** in `askamerica-engine/build.gradle.kts`: 722 MB
  → **~441 MB**. Excludes the orphaned ONNX model + tokenizer natives, the AWS-v1
  codegen JSON (`models/*.json`, flat pattern — the govdata `config/djia-wiki-model.json`
  is preserved), and Arrow Gandiva (natives + classes; reached only via the reflective
  `.arrow`-only path govdata never takes). AWS-v1 classes stay (read path needs s3a).
- **Driver-load smoke test PASSED** on the slimmed fat jar (jshell: `AskAmericaDriver`
  + `GovDataDriver` load and register, `acceptsURL=true`, no `NoClassDefFoundError`).
- **`stageEngineRuntime` gradle task**: emits the unshaded runtime jar-set (244 jars),
  which **boots** — the same smoke test passes with all 244 jars side by side on the
  classpath, confirming the no-shading-needed analysis empirically.
- **Python package** (`kenstott/askamerica`): `engine.py` now assembles the classpath
  from a bundled jar-set (`askamerica/engine_jars/*.jar`) with the single-jar download
  kept as a backward-compatible fallback; `pyproject.toml` ships `engine_jars/` as a
  wheel artifact.
- **Website**: pip instructions corrected (no `install-engine` download step); download
  links already resolve to `releases/latest`.
- **CI guards** (`.github/workflows/askamerica-engine.yml`): fail the build if the AWS
  codegen JSON / orphaned ML resources reappear, if the govdata config resource is lost
  to an over-broad exclude, or if the jar exceeds a 650 MB ceiling; plus a
  `stageEngineRuntime` smoke step.

Follow-up (not in this pass):

- **Jar-set now truly unshaded (~604 MB, boots).** Root cause of the earlier 1.15 GB
  bloat: `:file` targets Java 11 but its consumers inherit Calcite's Java 8 target, so
  Gradle rejected file's plain (Java 11) `calcite-file` variant as incompatible and fell
  back to file's shadow jar (`sih-aperio`, ~641 MB). Fixed **scoped to askamerica**: an
  `engineWheelClasspath` config requesting `jvm.version=11` + `bundling=external` makes
  `stageEngineRuntime` resolve plain `calcite-file` + its individual dependency jars.
  Result: `sih-aperio` gone, deps individual, `arrow-gandiva` dropped by the prefix
  filter, no Gandiva natives anywhere, set boots (driver + GovDataDriver resolve). No
  other consumer changed (govdata/Trino still resolve as before).
- **The `<100 MB` partition is blocked by exactly one jar.** In the 604 MB set, the
  **only** file over 100 MB is `aws-java-sdk-bundle-1.12.367.jar` (297 MB); every other
  jar (calcite-file 82 MB, duckdb 77 MB, …) bins into wheels trivially. The bundle
  cannot be split (a jar is atomic) and cannot be safely swapped for the tiny split
  `aws-java-sdk-s3/core/kms` (~3 MB) — the split ships 3 services vs the bundle's 329,
  and hadoop-aws's S3A needs many of them (sts, dynamodb, …); a blind swap risks a
  runtime `NoClassDefFoundError` on the s3a read path, unverifiable without live creds.
  So there are two real ways past it: (1) **Path 2 — migrate the read path to Iceberg
  `S3FileIO` (v2)**, which drops hadoop-aws + the 297 MB bundle entirely (and also frees
  the fat jar's 192 MB); needs a live Iceberg read to validate. (2) **PyPI file-size
  increase** — ship the bundle in its own wheel (works on Artifactory today, community
  PyPI with a granted bump). Path 1 is the clean permanent fix.
- **Tier-2** (drop the 192 MB AWS-v1 classes) needs the Iceberg `S3FileIO` migration.

## Known gaps / verification still needed

1. **Python wheel downloads the JAR at runtime** (`README` in `kenstott/askamerica`):
   JPype fetches the engine JAR from the GitHub release into `~/.askamerica/engine/`.
   This reintroduces a github.com egress hop and the fetch-a-binary governance smell
   at first run. Partially addressed: the package now *prefers* a bundled jar-set and
   only downloads as a fallback; the wheel-partition CI (follow-up) makes bundling the
   default.
2. **Iceberg FileIO — RESOLVED: read path is on hadoop `s3a` (AWS v1).** The 192 MB
   Tier-2 cut is blocked until `IcebergTable` + `IcebergCatalogManager` are migrated
   to Iceberg `S3FileIO` (v2). Tracked as a follow-up; Tier-2a (68 MB codegen JSON) is
   applied now.
3. **Gandiva usage — RESOLVED: not on the read path.** Reached only via
   `FileSchema.createArrowTable()`, which is `.arrow`-file-only and loads the arrow
   adapter by reflection; govdata has no `.arrow` sources. Removed in Tier-3.
4. **Exact per-platform native sizes** — build a linux-only stripped jar and measure
   to replace the ±20 MB estimates.

## Next implementation steps

1. Apply Tier-1 excludes (zero-risk, banks 163 MB).
2. Apply Tier-2a (exclude the 68 MB AWS-v1 codegen JSON). Tier-2 (drop the 192 MB of
   AWS-v1 classes) stays blocked until the Iceberg `S3FileIO` migration lands.
3. **Build and validate the slimmed fat jar** (the Maven/JDBC deliverable): rebuild
   after each tier, assert it still loads as a driver, connects, and reads Iceberg
   from S3 with no `NoClassDefFoundError` from a cut class. See "Testing".
4. Add a gradle task emitting the slimmed, unshaded `runtimeClasspath` jars,
   partitioned into wheels by size/platform.
5. Write the classpath-assembly launcher on the Python side.
6. Wire the first-run flow (creds-gated, MCP-first) and the config generator.
7. Document the BYO-bucket enterprise profile and the Maven/JDBC integration facts.

## Testing

Nothing here ships on assertion alone — every cut and every packaging change is
proven by an executable check. The excludes are aggressive, so the failure mode to
hunt is a runtime `NoClassDefFoundError`/`ClassNotFoundException` from something the
static analysis said was unused.

### Slimmed fat jar (Maven / JDBC deliverable)

- **Rebuild after each removal tier** and run the existing read-path integration test
  (`askamerica-engine/src/test/java/.../McpServerIntegrationTest.java`) against the
  slimmed jar, not the fat one.
- **Driver smoke:** `DriverManager.getConnection("jdbc:askamerica:...")` →
  `SELECT`-and-fetch against (a) the R2 bucket, (b) a BYO S3/MinIO, (c) a local
  `GOVDATA_PARQUET_DIR`.
- **Iceberg read after AWS v1 removal** — the Tier-2 gate. A passing Iceberg
  metadata + data read on the slimmed jar is what confirms `S3FileIO` (v2) carries the
  read path and hadoop-aws/v1 was genuinely dead. If this fails, Tier 2 is blocked
  until Iceberg is moved to `S3FileIO`.
- **CI regression guards** (fail the build, not a reviewer's memory):
  - assert `com/amazonaws/**` is **absent** from the slimmed artifacts (stops AWS v1
    creeping back via a transitive),
  - assert the slimmed fat jar is under a size ceiling (e.g. < 400 MB).

### Unshaded wheel-set (pip / MCP)

- **Boot side-by-side:** put the partitioned jars on a JPype classpath and confirm the
  driver self-registers, connects, and queries — the practical confirmation of the
  "no shading needed" analysis.
- **Resolution + platform tags:** on linux/mac/win, `pip install` resolves
  `core + runtime + the matching `duckdb-<platform>`` wheel and no other native.
- **MCP path:** server starts; `list_schemas` / `describe_table` / `query` respond.
- **Programmatic SQL path:** the notebook `connect()` → SQL flow.
- **Self-contained:** assert first run performs **no** network fetch of the JAR
  (nothing hits github) — the whole point of bundling the jars in the wheels.
- **Per-wheel size guard:** CI asserts every wheel file < 100 MB (public-PyPI limit).

### Runtime data access matrix

| Scenario | Env | Expect |
|---|---|---|
| Default | `ASKAMERICA_API_KEY` | broker fetch → R2 read |
| BYO bucket | `AWS_ENDPOINT_OVERRIDE` + `AWS_*` | MinIO read, **broker not called** |
| Local | `GOVDATA_PARQUET_DIR` | filesystem read |
| No creds | none | actionable guidance error, not a crash |

Assert the BYO path makes **zero** calls to `askamerica.ai` (the broker-bypass
promise).

### First-run flow

- Creds-gated branching: no-creds / free-key / `AWS_*` each land on the right screen.
- `import` does not block; notebook renders rich output, TTY prompts.
- MCP config generation writes a valid client entry that launches the self-contained
  wheel (no runtime download).
