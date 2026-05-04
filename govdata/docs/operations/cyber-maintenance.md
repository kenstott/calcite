# Cyber Data Maintenance Runbook

## Quick Reference

| Worker | Mode | Workers |
|---|---|---|
| worker-62 | initial/backfill | included in `./run-pool.sh historical` |
| worker-63–65 | recurring | included in `./run-pool.sh daily` |
| worker-66 | static standards refresh | `./run-pool.sh 66` (on-demand only) |

```bash
cd scripts/parallel

# First-time setup — cyber runs as part of the full historical load
./run-pool.sh historical
# — or cyber initial only —
./run-pool.sh --schema cyber historical

# Recurring — cyber workers run automatically as part of the daily pool
./run-pool.sh daily
# — or cyber only —
./run-pool.sh --schema cyber daily

# On-demand: re-run static standards after a NIST/CIS/OWASP update
./run-pool.sh 66

# Force all sub-runs regardless of release window (backfill / manual refresh)
./worker-cyber.sh weekly --force
./worker-cyber.sh daily --force
```

---

## Prerequisites

### Environment Variables

Set these in `.env.prod` (or the environment used by your cron/scheduler):

```bash
# Required
export CYBER_PARQUET_DIR=/data/cyber          # or s3://your-bucket/cyber
export CYBER_CACHE_DIR=/data/cyber-cache      # or s3://your-bucket/cyber-cache

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

Downloads the full datasets that are too large or slow for delta loads:

1. **Full NVD CVE catalog** (~350k CVEs via paginated API; takes 15–60 min depending on rate limit)
2. **CISA KEV catalog** and **CWE catalog**
3. **Static standards**: NIST 800-53 Rev 5, NIST CSF 2.0, CIS Controls v8, OWASP Top 10, ATT&CK→NIST mappings
4. **OTX full backfill** (only if `CYBER_OTX_API_KEY` is set; cursor-paginates all available pulses)

After initial completes, switch to the recurring cadence workers. Do not re-run `initial` routinely —
it fetches the entire NVD catalog each time.

```bash
./scripts/parallel/worker-cyber.sh initial
```

---

### `daily` — Incremental CVE and KEV refresh

Downloads only CVEs modified or published in the last 24 hours (NVD's `lastModStartDate` filter),
plus a fresh copy of the CISA KEV list.

**When to run:** Once per day. A common schedule is 06:00 UTC.

```bash
./scripts/parallel/worker-cyber.sh daily
```

Cron example:
```
0 6 * * * /path/to/govdata/scripts/parallel/worker-cyber.sh daily
```

---

### `weekly` — Broader vulnerability ecosystem refresh

Refreshes sources that change less frequently:

- **CWE catalog** (MITRE publishes updates periodically)
- **OSV vulnerabilities** (per-ecosystem advisories; requires `CYBER_OSV_ECOSYSTEMS`)
- **MITRE ATT&CK techniques** (major releases twice per year; minor updates more often)
- **ATT&CK→NIST mappings** (CISA publishes updates with each ATT&CK release)

**When to run:** Weekly. A common schedule is Sunday 02:00 UTC.

```bash
./scripts/parallel/worker-cyber.sh weekly
```

Cron example:
```
0 2 * * 0 /path/to/govdata/scripts/parallel/worker-cyber.sh weekly
```

---

### `hourly` — Live IOC and threat intelligence feeds

Fetches the most current threat intelligence from live sources:

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
./scripts/parallel/worker-cyber.sh hourly
```

Cron example (every 2 hours):
```
0 */2 * * * CYBER_OTX_DELTA_DAYS=1 /path/to/govdata/scripts/parallel/worker-cyber.sh hourly
```

---

### `static` — Re-run static standards

Refreshes NIST 800-53, NIST CSF 2.0, CIS Controls, OWASP Top 10, and ATT&CK→NIST mappings.
These sources only change when frameworks publish new versions (roughly annually).

**When to run:** After a framework publishes a new version, or when you add new ecosystems.
Not needed in routine schedules.

```bash
./scripts/parallel/worker-cyber.sh static
```

---

## Cron Schedule

Cyber workers run as part of `./run-pool.sh daily` — no separate cron entry needed.

```cron
# All recurring workers including cyber — run once per day
0 6 * * *   cd /path/to/govdata/scripts/parallel && ./run-pool.sh daily
```

To run cyber workers in isolation (e.g. after a manual NVD backfill):

```bash
./run-pool.sh --schema cyber daily
```

---

## Release-Window Checks

Workers 63–64 gate their sub-runs to avoid running when no new data is expected.
Each check exits in milliseconds — no network I/O, no model file written.

| Mode | Window | Mechanism | Notes |
|---|---|---|---|
| `daily` | Every day | No gate — NVD updates continuously | Always runs |
| `weekly` | Sunday only (DOW 0) | `within_release_dow` | CWE/ATT&CK/OSV update weekly or less |
| `hourly` | Every run | No gate — live IOC feeds | Always runs |
| `static` | On-demand | No gate — manual trigger | Run after framework version releases |

To bypass: `./worker-cyber.sh weekly --force`

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

Logs are written to `scripts/parallel/runs/worker-cyber-<mode>/etl_<timestamp>.log`.

```bash
tail -f scripts/parallel/runs/worker-cyber-hourly/etl_*.log
```
