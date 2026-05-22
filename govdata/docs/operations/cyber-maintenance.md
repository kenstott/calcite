# Cyber Data Maintenance Runbook

## Quick Reference

| Slot | When to run |
|---|---|
| `cyber_vuln:initial` | First-time setup — full NVD + KEV + CWE catalog |
| `cyber_threat:initial` | First-time setup — NIST/CIS/OWASP standards + OTX backfill |
| `cyber_vuln:daily` | Daily — NVD delta + KEV refresh |
| `cyber_vuln:weekly` | Weekly — CWE, OSV, cross-refs, advisories |
| `cyber_threat:weekly` | Weekly — ATT&CK techniques + NIST mappings |
| `cyber_threat:hourly` | Hourly — live IOC feeds |
| `cyber_threat:static` | On-demand — re-run static standards after framework update |

```bash
cd scripts/parallel

# First-time setup — cyber runs as part of the full historical load
./run-pool.sh historical
# — or cyber initial only —
./run-pool.sh --schema cyber_threat historical
./run-pool.sh --schema cyber_vuln historical

# Recurring — cyber workers run automatically as part of the daily pool
./run-pool.sh daily
# — or cyber only —
./run-pool.sh --schema cyber_threat daily
./run-pool.sh --schema cyber_vuln daily

# On-demand: re-run static standards after a NIST/CIS/OWASP update
./run-pool.sh --schema cyber_threat historical

# Force all sub-runs regardless of release window (backfill / manual refresh)
# Via run-pool (preferred — memory management, logging, pool coordination)
./run-pool.sh --force --schema cyber_threat daily
./run-pool.sh --force --schema cyber_vuln daily
# Direct worker invocation (bypasses pool; useful for isolated testing)
./worker-cyber.sh weekly cyber_vuln --force
./worker-cyber.sh daily cyber_vuln --force
```

---

## Prerequisites

### Environment Variables

Set these in `.env.prod` (or the environment used by your scheduler):

Cyber schemas use `GOVDATA_PARQUET_DIR` and `GOVDATA_CACHE_DIR` — set these globally, no schema-specific path overrides needed.

```bash
# Optional but recommended
export CYBER_NVD_API_KEY=your-nvd-api-key     # register at nvd.nist.gov/developers
export CYBER_OTX_API_KEY=your-otx-key         # register at otx.alienvault.com
export CYBER_THREATFOX_API_KEY=your-tf-key    # register at threatfox.abuse.ch
export CYBER_GITHUB_TOKEN=ghp_xxx             # GitHub Settings → Developer tokens
export CYBER_OSV_ECOSYSTEMS=PyPI,npm,Go,Maven # choose ecosystems you care about
```

### JAR

The worker script requires the govdata shadow JAR. Build it with:

```bash
./gradlew :govdata:shadowJar
```

---

## Modes in Detail

### `initial` — Run once on first setup

Downloads the full datasets that are too large or slow for delta loads.

For `cyber_vuln:initial`:

1. **Full NVD CVE catalog** (~350k CVEs via paginated API; takes 15–60 min depending on rate limit)
2. **CISA KEV catalog** and **CWE catalog**

For `cyber_threat:initial`:

1. **Static standards**: NIST 800-53 Rev 5, NIST CSF 2.0, CIS Controls v8, OWASP Top 10, ATT&CK→NIST mappings
2. **OTX full backfill** (only if `CYBER_OTX_API_KEY` is set; cursor-paginates all available pulses)

After initial completes, switch to the recurring cadence workers. Do not re-run `initial` routinely —
it fetches the entire NVD catalog each time.

```bash
# Runs both schemas; for schema-specific:
./scripts/parallel/worker-cyber.sh initial cyber_vuln
./scripts/parallel/worker-cyber.sh initial cyber_threat
```

---

### `daily` — Incremental CVE and KEV refresh

Downloads only CVEs modified or published in the last 24 hours (NVD's `lastModStartDate` filter),
plus a fresh copy of the CISA KEV list. Applies to `cyber_vuln`.

**When to run:** Once per day. A common schedule is 06:00 UTC.

```bash
./scripts/parallel/worker-cyber.sh daily cyber_vuln
```

---

### `weekly` — Broader vulnerability ecosystem refresh

Refreshes sources that change less frequently.

For `cyber_vuln:weekly`:

- **CWE catalog** (MITRE publishes updates periodically)
- **OSV vulnerabilities** (per-ecosystem advisories; requires `CYBER_OSV_ECOSYSTEMS`)
- Cross-references and advisories

For `cyber_threat:weekly`:

- **MITRE ATT&CK techniques** (major releases twice per year; minor updates more often)
- **ATT&CK→NIST mappings** (CISA publishes updates with each ATT&CK release)

**When to run:** Weekly. A common schedule is Sunday 02:00 UTC.

```bash
./scripts/parallel/worker-cyber.sh weekly cyber_vuln --force
./scripts/parallel/worker-cyber.sh weekly cyber_threat --force
```

---

### `hourly` — Live IOC and threat intelligence feeds

Applies to `cyber_threat`. Fetches the most current threat intelligence from live sources:

| Table | Source | Notes |
|---|---|---|
| `ioc_urls` | URLhaus | Downloads ZIP+CSV of malicious URLs |
| `ioc_hashes` | MalwareBazaar | Downloads ZIP+CSV of malware hashes |
| `ioc_ips` | Feodo Tracker | JSON feed of botnet C2 IPs |
| `ioc_mixed` | ThreatFox | POST API; requires `CYBER_THREATFOX_API_KEY` |
| `threat_pulses` | AlienVault OTX | Cursor pagination; requires `CYBER_OTX_API_KEY`; uses `CYBER_OTX_DELTA_DAYS=1` for delta |

**When to run:** Every 1–4 hours depending on your threat intelligence freshness requirement.
The IOC sources (URLhaus, MalwareBazaar, Feodo) are public and rate-limit-free.
ThreatFox and OTX require API keys.

```bash
# Set delta window for OTX before running
export CYBER_OTX_DELTA_DAYS=1
./scripts/parallel/worker-cyber.sh hourly cyber_threat
```

---

### `static` — Re-run static standards

Applies to `cyber_threat`. Refreshes NIST 800-53, NIST CSF 2.0, CIS Controls, OWASP Top 10,
and ATT&CK→NIST mappings. These sources only change when frameworks publish new versions
(roughly annually).

**When to run:** After a framework publishes a new version, or when you add new ecosystems.
Not needed in routine schedules.

```bash
./scripts/parallel/worker-cyber.sh static cyber_threat
```

---

## Cron Schedule

Cyber workers run as part of `run-scheduled.sh` (the perpetual runner) or `./run-pool.sh daily`. No separate cron entry is needed. See the main operations guide for systemd setup.

To run cyber workers in isolation (e.g. after a manual NVD backfill):

```bash
./run-pool.sh --schema cyber_threat daily
./run-pool.sh --schema cyber_vuln daily
```

---

## Release-Window Checks

Cyber workers gate their sub-runs to avoid running when no new data is expected.
Each check exits in milliseconds — no network I/O, no model file written.

| Mode | Window | Mechanism | Notes |
|---|---|---|---|
| `daily` | Every day | No gate — NVD updates continuously | Always runs |
| `weekly` | Sunday only (DOW 0) | `within_release_dow` | CWE/ATT&CK/OSV update weekly or less |
| `hourly` | Every run | No gate — live IOC feeds | Always runs |
| `static` | On-demand | No gate — manual trigger | Run after framework version releases |

To bypass: `./run-pool.sh --force --schema cyber_threat daily` (preferred) or `./worker-cyber.sh weekly cyber_vuln --force` (direct).
`--force` skips window checks only — year bounds are unchanged.

---

## Troubleshooting

### NVD rate limiting

Without an API key, NVD allows 1 request per 6 seconds. A full catalog download takes ~60 minutes.
With an API key, the limit is 5 requests per 30 seconds (~15 minutes for a full catalog).

Register for a free API key at [nvd.nist.gov/developers](https://nvd.nist.gov/developers/request-an-api-key).

### OTX 429 errors

The OTX transformer automatically sleeps 60 seconds and retries on HTTP 429. If this happens
frequently, increase `CYBER_OTX_DELTA_DAYS` in the hourly job to reduce the number of pages fetched.

### Missing tables

Tables guarded by API keys (`ioc_mixed`, `threat_pulses`) silently produce 0 rows when their key
is absent — they do not error. Check that the corresponding env var is set and non-empty.

### Log location

Logs are written to `scripts/parallel/runs/worker-{schema}-{mode}/etl_{timestamp}.log`.

```bash
tail -f scripts/parallel/runs/worker-cyber_threat-hourly/etl_*.log
tail -f scripts/parallel/runs/worker-cyber_vuln-initial/etl_*.log
tail -f scripts/parallel/runs/worker-cyber_threat-initial/etl_*.log
```
