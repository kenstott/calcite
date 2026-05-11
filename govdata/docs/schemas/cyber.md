# Cyber Schema Documentation

## Overview

The `cyber_vuln` and `cyber_threat` schemas provide access to cybersecurity data including
vulnerability databases, threat intelligence feeds, and security standards frameworks. Both schemas
are served by `CyberSchemaFactory` via `GovDataSchemaFactory` and are driven by YAML-defined tables
with configurable download automation.

---

## Sub-schemas

| Schema | `dataSource` value | Description |
|---|---|---|
| `cyber_vuln` | `cyber_vuln` | Vulnerability databases: NVD CVEs, CISA KEV, CWE catalog, OSV, advisories |
| `cyber_threat` | `cyber_threat` | Threat intelligence: IOC feeds, ATT&CK, NIST/CIS/OWASP standards, OTX pulses |

---

## Tables

### cyber_vuln

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `vulnerabilities` | NVD CVE 2.0 catalog with CVSS scores and references | NVD API | Daily delta |
| `kev_catalog` | CISA Known Exploited Vulnerabilities | CISA KEV JSON | Daily |
| `cwe_catalog` | CWE weakness taxonomy | MITRE CWE XML | Weekly |
| `osv_vulnerabilities` | OSV ecosystem vulnerabilities | OSV API | Weekly |
| `vuln_cross_refs` | Cross-references between CVE, CWE, CPE, CNA | Derived | Weekly |
| `advisories` | Vendor security advisories | GitHub advisory DB | Weekly |

### cyber_threat

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `ioc_urls` | Malicious URL indicators | URLhaus | Hourly |
| `ioc_hashes` | Malware file hashes | MalwareBazaar | Hourly |
| `ioc_ips` | Malicious IP indicators | Feodo Tracker | Hourly |
| `ioc_mixed` | Mixed IOCs (URLs, hashes, IPs) | ThreatFox | Hourly |
| `threat_pulses` | AlienVault OTX threat pulses | OTX API | Hourly |
| `attack_techniques` | MITRE ATT&CK techniques | MITRE STIX bundle | Weekly |
| `attack_to_nist_mappings` | ATT&CK technique → NIST 800-53 control mappings | CISA mappings-explorer | Weekly |
| `nist_controls` | NIST SP 800-53 Rev 5 control catalog | NIST OSCAL JSON | Static |
| `nist_csf_functions` | NIST CSF 2.0 functions, categories, and subcategories | NIST OSCAL JSON | Static |
| `cis_controls` | CIS Controls v8 safeguards | CIS GitHub RST | Static |
| `owasp_top10` | OWASP Top 10 (2021) categories with CWE mappings | OWASP GitHub MD | Static |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `CYBER_PARQUET_DIR` | Root directory for Parquet output (local path or S3 URI) |
| `CYBER_CACHE_DIR` | Root directory for raw download cache |

### Optional — data source credentials

| Variable | Description | Affected tables |
|---|---|---|
| `CYBER_NVD_API_KEY` | NVD API key for higher rate limits (5 req/30s vs 1/6s without) | `vulnerabilities` |
| `CYBER_OTX_API_KEY` | AlienVault OTX API key | `threat_pulses` |
| `CYBER_THREATFOX_API_KEY` | ThreatFox API key | `ioc_mixed` |
| `CYBER_GITHUB_TOKEN` | GitHub personal access token | `osv_vulnerabilities`, `advisories` |
| `CYBER_OSV_ECOSYSTEMS` | Comma-separated OSV ecosystems (e.g. `PyPI,npm,Go`) | `osv_vulnerabilities` |

### Optional — behavior

| Variable | Description |
|---|---|
| `CYBER_OTX_DELTA_DAYS` | If set to a positive integer, OTX fetches only pulses modified in the last N days. Unset or 0 = full backfill. |

---

## Maintenance: worker-cyber.sh

All cyber data maintenance is handled by a single parameterized script:

```
scripts/parallel/worker-cyber.sh <mode>
```

See [Cyber Data Maintenance](../operations/cyber-maintenance.md) for the full runbook.

---

## Sample Queries

```sql
-- Top 10 critical CVEs exploited in the wild
SELECT v.cve_id, v.description, v.cvss_v3_score, k.date_added
FROM cyber_vuln.vulnerabilities v
JOIN cyber_vuln.kev_catalog k ON v.cve_id = k.cve_id
WHERE v.cvss_v3_score >= 9.0
ORDER BY k.date_added DESC
LIMIT 10;

-- IOCs seen in the last 24 hours
SELECT source, ioc_type, ioc_value, malware_name, first_seen
FROM cyber_threat.ioc_mixed
WHERE first_seen >= CURRENT_DATE - INTERVAL '1' DAY
ORDER BY first_seen DESC;

-- ATT&CK techniques mapped to NIST controls
SELECT a.technique_id, a.name, m.control_id, n.title
FROM cyber_threat.attack_techniques a
JOIN cyber_threat.attack_to_nist_mappings m ON a.technique_id = m.technique_id
JOIN cyber_threat.nist_controls n ON m.control_id = n.control_id
ORDER BY a.technique_id;
```
