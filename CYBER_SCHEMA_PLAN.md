# Cyber Schema Plan

## Overview

Two new schemas implemented as sub-domains within the `govdata` module, following the same
metadata-driven YAML + DuckDB/Parquet ETL pattern as `sec`, `crime`, and `census`.

| Schema | Focus | Refresh Cadence |
|--------|-------|-----------------|
| `cyber_vuln` | Vulnerability & advisory data | Daily to weekly |
| `cyber_threat` | Threat intel, exploits, IOCs, standards | Hourly (feeds) to static (standards) |

Both live under `govdata/src/main/java/.../govdata/cyber/` and
`govdata/src/main/resources/cyber/`.

---

## Confirmed Design Decisions

### 1. Model / Script Pattern

Follows the existing ETL model pattern (see `govdata/src/main/resources/models/etl/`). A single
global model (e.g., `cyber-vuln-all.json`) defines the full schema; individual worker scripts are
subsets of that model with different operand parameters — date ranges, enabled sources, cadence
tier. Example split:

```
models/etl/
  cyber-vuln-daily.json      # NVD delta, CISA KEV — runs daily
  cyber-vuln-weekly.json     # OSV ecosystems, GitHub SA, MITRE CVE — runs weekly
  cyber-threat-hourly.json   # URLhaus, MalwareBazaar, ThreatFox, Feodo — runs hourly
  cyber-threat-weekly.json   # ATT&CK, CAPEC, D3FEND — runs weekly
  cyber-threat-static.json   # NIST 800-53, CSF, CIS Controls, OWASP — runs on version bump
```

Each model passes operand params to `CyberSchemaFactory` to control which sources are active and
what date window to process.

### 2. TTL — YAML-driven (needs implementation)

`ttl_days` will be a per-table YAML config field, read by `CyberSchemaFactory` and passed to
`DuckDBCacheStore`. **Note:** `current_year_ttl_days` already exists in `econ-schema.yaml` but is
not wired up anywhere in Java — it is dead config. Cyber will implement the full loop:
YAML field → `SchemaConfigReader` → `CacheManifest` → `DuckDBCacheStore.ttlDays` parameter.
This should also be back-ported to econ once the pattern is proven in cyber.

### 3. STIX2 Parsing

Java `Stix2ResponseTransformer`. Uses Jackson streaming API (not `readTree()`) due to ~100MB
bundle size. Two-pass approach: first pass builds an object map by STIX ID, second pass resolves
`relationship` objects into array columns on primary tables.

### 4. PDF Advisory Extraction

Apache PDFBox from Phase 1. PDFBox is already present in the Calcite dependency tree. Used for
FBI/CISA joint advisories and Verizon DBIR. HTML advisories are preferred when available; PDF is
the fallback, not the primary path.

### 5. OSV Storage Access

Plain HTTPS download from `https://osv-vulnerabilities.storage.googleapis.com/`. No GCS SDK
needed. Ecosystems controlled by `CYBER_OSV_ECOSYSTEMS` env var.

### 6. CWE Catalog Ownership

`cyber_vuln.cwe_catalog` is the canonical CWE dimension table (sourced from
`https://cwe.mitre.org/data/xml/cwec_latest.xml.zip`). `cyber_threat` references it via
cross-schema join on `cwe_id`. Neither schema duplicates CWE descriptions inline.

### 7. SchemaFactory

One shared `CyberSchemaFactory` handles both `cyber_vuln` and `cyber_threat`. The `dataSource`
operand in the model JSON (`"dataSource": "cyber_vuln"` or `"dataSource": "cyber_threat"`)
determines which YAML schema file is loaded and which sub-domain downloaders are activated.

---

## Schema: `cyber_vuln`

Vulnerability records, advisory metadata, and cross-source identifier mappings.

### Data Sources

| Source | Type | API / URL | Notes |
|--------|------|-----------|-------|
| NVD (NIST) | REST API | `https://services.nvd.nist.gov/rest/json/cves/2.0` | Paginated; API key for higher rate limits; ~240k CVEs |
| CISA KEV | JSON feed | `https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json` | ~1k records; single file; very stable schema |
| MITRE CVE | JSON bulk | `https://github.com/CVEProject/cvelistV5` | Git repo; daily delta ZIPs available |
| GitHub Security Advisories | GraphQL | `https://api.github.com/graphql` | GHSA IDs; ecosystem-specific; token required |
| OSV | JSON bulk | `https://osv.dev/docs/#section/Data` | GCS bucket `gs://osv-vulnerabilities`; ecosystem ZIPs |
| VulnDB | Commercial | N/A | Requires license; skip for now |

### Proposed Tables

Field names are confirmed from live API responses.

| Table | Grain | Confirmed Field Names |
|-------|-------|-------------|
| `vulnerabilities` | 1 row per CVE | `cve_id` (NVD: `.cve.id`), `published` (ISO-8601), `last_modified` (NVD: `.cve.lastModified`), `vuln_status` (NVD: `.cve.vulnStatus`), `description_en`, `cvss_v2_score`, `cvss_v2_vector`, `cvss_v31_score`, `cvss_v31_vector`, `cvss_v31_severity`, `cwe_ids[]`, `source` |
| `cve_references` | 1 row per reference URL per CVE | `cve_id`, `url` (NVD: `.references[].url`), `source` (NVD: `.references[].source`), `feed_source` |
| `affected_products` | 1 row per CPE match per CVE | `cve_id`, `cpe_uri`, `vulnerable`, `version_start_including`, `version_end_excluding`, `feed_source` |
| `kev_catalog` | 1 row per KEV entry | `cve_id` (KEV: `cveID`), `vendor_project` (KEV: `vendorProject`), `product`, `vulnerability_name` (KEV: `vulnerabilityName`), `date_added` (KEV: `dateAdded`, YYYY-MM-DD), `short_description` (KEV: `shortDescription`), `required_action` (KEV: `requiredAction`), `due_date` (KEV: `dueDate`), `known_ransomware_use` (KEV: `knownRansomwareCampaignUse` — string `"Known"`/`"Unknown"`), `notes_urls[]` (split KEV `notes` on `;`), `cwes[]` (KEV: `cwes` array) |
| `osv_vulnerabilities` | 1 row per OSV entry | `osv_id` (OSV: `id`), `modified` (required), `published`, `withdrawn`, `aliases[]`, `summary`, `details`, `severity_type`, `severity_score`, `ecosystem`, `package_name`, `package_purl`, `schema_version`, `database_specific` (JSON blob) |
| `advisories` | 1 row per advisory | `advisory_id`, `source`, `title`, `published_date`, `severity`, `cve_ids[]`, `affected_systems`, `url` |
| `vuln_cross_refs` | CVE ↔ GHSA/OSV/NVD/MITRE ID mappings | `cve_id`, `external_id`, `external_source`, `url` |

**NVD parsing notes:**
- Response envelope: `vulnerabilities[n].cve` — one extra nesting level
- CVSS versions `V2`, `V30`, `V31`, `V40` can all coexist on one CVE; store V31 preferentially, fall back to V2
- Pagination: offset-based, `startIndex` + `resultsPerPage` (max 2000); ~347k total CVEs
- Rate limit: 5 req/30s without key, 50 req/30s with `CYBER_NVD_API_KEY`

**CISA KEV parsing notes:**
- Single static file, 1,585 entries; no delta endpoint — diff on `catalogVersion` (format: `YYYY.MM.DD`)
- `notes` is a free-text semicolon-delimited URL string, not an array — must split on `;` at ingest
- `cwes` array may be empty on older entries

**OSV parsing notes:**
- Only `id` and `modified` are guaranteed present; all other fields nullable
- `affected` can be null (untriaged); handle gracefully
- Version ranges use sequential `introduced`/`fixed` event pairs within `ranges[].events`
- 47 ecosystems; filter via `CYBER_OSV_ECOSYSTEMS` env var

### Refresh Strategy

- NVD: daily delta via `lastModStartDate`/`lastModEndDate` params; full refresh weekly
- CISA KEV: daily; single JSON file, diff on `catalogVersion` field (no ETag delta available)
- MITRE CVE: daily delta ZIPs from CVEProject GitHub releases
- GitHub SA: daily GraphQL cursor-based pagination
- OSV: daily; per-ecosystem ZIPs from HTTPS (`https://osv-vulnerabilities.storage.googleapis.com/`) — no GCS SDK needed

---

## Schema: `cyber_threat`

Threat intelligence, exploit data, IOC feeds, ATT&CK taxonomy, and standards/compliance mappings.

### Data Sources

#### Threat Taxonomy (MITRE)

| Source | Type | URL | Notes |
|--------|------|-----|-------|
| MITRE ATT&CK | STIX2 JSON | `https://github.com/mitre-attack/attack-stix-data` | Enterprise, Mobile, ICS domains; versioned releases |
| MITRE CAPEC | XML | `https://capec.mitre.org/data/xml/capec_latest.xml` | Attack pattern taxonomy; maps to ATT&CK |
| MITRE D3FEND | OWL/JSON-LD | `https://d3fend.mitre.org/ontologies/d3fend.json` | Defensive countermeasures; maps to ATT&CK |

#### Government Advisories

| Source | Type | URL | Notes |
|--------|------|-----|-------|
| CISA Advisories | HTML/JSON | `https://www.cisa.gov/cybersecurity-advisories` | ICS-CERT, AA-series joint advisories |
| US-CERT Alerts | RSS/JSON | `https://www.cisa.gov/uscert/ncas/alerts` | Current activity, alerts, tips |
| FBI/CISA Joint Advisories | PDF/HTML | `https://www.cisa.gov/resources-tools/resources/joint-advisories` | AA-series; parse PDF for IOC extraction |

#### Exploit & Weaponization Data

| Source | Type | URL | Notes |
|--------|------|-----|-------|
| Exploit-DB | CSV + files | `https://gitlab.com/exploit-database/exploitdb` | Git repo; `files_exploits.csv` index; daily sync |
| Metasploit Modules | Git index | `https://github.com/rapid7/metasploit-framework` | Mine `modules/` for CVE refs; no full clone needed |
| Packet Storm | HTML scrape | `https://packetstormsecurity.com` | Rate-limited scrape; lower priority |

#### IOC / Malware Feeds

| Source | Type | URL | Auth | Volume |
|--------|------|-----|------|--------|
| MalwareBazaar | CSV (plain) | `https://bazaar.abuse.ch/export/csv/recent/` | None | ~5k new samples/day |
| URLhaus | ZIP → CSV inside | `https://urlhaus.abuse.ch/downloads/csv/` | None | ~50k URLs, full refresh; **must decompress ZIP** |
| ThreatFox | JSON (POST) | `https://threatfox-api.abuse.ch/api/v1/` | **`Auth-Key` header required** (free) | ~10k IOCs/day; anonymous access returns 401 |
| AlienVault OTX | REST | `https://otx.alienvault.com/api/v1/` | Free API key | Pulses; rate-limited |
| Abuse.ch (Feodo) | JSON | `https://feodotracker.abuse.ch/downloads/ipblocklist.json` | None | C2 IP blocklist (~5 active entries) |

#### Standards & Compliance

| Source | Type | URL | Notes |
|--------|------|-----|-------|
| NIST CSF 2.0 | JSON | `https://csrc.nist.gov/extensions/nudp/services/json/csf/download?olirID=...` | Static; version-pinned |
| NIST 800-53 Rev 5 | XML/JSON | `https://csrc.nist.gov/projects/risk-management/sp800-53-controls/release-search` | Stable; annual update cycle |
| CIS Benchmarks | JSON/XLSX | `https://www.cisecurity.org/cis-benchmarks` | Some require registration; start with CIS Controls v8 |
| OWASP Top 10 | Markdown/JSON | `https://github.com/OWASP/Top10` | Static; version-pinned |
| MITRE → NIST Mappings | CSV | `https://github.com/center-for-threat-informed-defense/mappings-explorer` | ATT&CK ↔ NIST 800-53/CSF cross-walk |

#### Industry Data

| Source | Type | URL | Notes |
|--------|------|-----|-------|
| Verizon DBIR | PDF + xlsx | `https://www.verizon.com/business/resources/reports/dbir/` | Annual; extract aggregate stats |
| SEC Breach Filings | EDGAR (via `cyber_vuln` SEC integration) | Existing SEC sub-domain | 8-K Item 1.05 cybersecurity disclosures; already in scope |
| State AG Breach Notices | HTML scrape | Various (CA, NY, TX, WA lead in volume) | Low priority; inconsistent format |

### Proposed Tables

#### ATT&CK Taxonomy

Field names confirmed from live STIX2 bundle (enterprise-attack-14.1, 20,770 objects).

| Table | Grain | Confirmed Field Names |
|-------|-------|-------------|
| `attack_techniques` | 1 row per technique/sub-technique | `technique_id` (from `external_references[source_name=="mitre-attack"].external_id`), `stix_id`, `name`, `description`, `tactic_short_names[]` (from `kill_chain_phases[].phase_name`), `platforms[]` (`x_mitre_platforms`), `data_sources[]` (`x_mitre_data_sources`), `is_subtechnique` (`x_mitre_is_subtechnique`), `parent_technique_id` (via `subtechnique-of` relationship), `domain` (`x_mitre_domains[0]`), `detection`, `mitre_version` (`x_mitre_version`) |
| `attack_tactics` | 1 row per tactic (14 in enterprise) | `tactic_id` (from `external_references`), `stix_id`, `name`, `short_name` (`x_mitre_shortname`), `description`, `domain` |
| `attack_mitigations` | 1 row per mitigation (284 in enterprise) | `mitigation_id` (from `external_references`), `stix_id`, `name`, `description`, `technique_ids[]` (via `mitigates` relationships) |
| `attack_groups` | 1 row per intrusion-set (158 in enterprise) | `group_id` (from `external_references`), `stix_id`, `name`, `aliases[]`, `description`, `technique_ids[]` (via `uses` relationships to attack-pattern), `software_ids[]` (via `uses` relationships to malware/tool) |
| `attack_software` | 1 row per malware or tool (653 combined) | `software_id` (from `external_references`), `stix_id`, `name`, `software_type` (STIX `type`: `malware` or `tool`), `aliases[]`, `platforms[]`, `technique_ids[]` (via `uses` relationships) |
| `attack_campaigns` | 1 row per campaign (23 in enterprise) | `campaign_id`, `stix_id`, `name`, `aliases[]`, `description`, `first_seen`, `last_seen` |
| `capec_patterns` | 1 row per attack pattern | `capec_id`, `name`, `description`, `likelihood`, `severity`, `attack_technique_ids[]`, `cwe_ids[]` |
| `d3fend_techniques` | 1 row per defensive technique | `d3fend_id`, `name`, `definition`, `attack_technique_ids[]`, `artifact_ids[]` |

**STIX2 parsing notes:**
- ATT&CK technique ID (e.g. `T1055.011`) is **not** a top-level field — extract from `external_references` where `source_name == "mitre-attack"`
- Graph edges (group→technique, mitigation→technique, etc.) are encoded as `relationship` objects (18,719 of them); must be resolved to populate array columns on primary tables
- Sub-technique parent resolved via `relationship_type == "subtechnique-of"`
- `x-mitre-collection` object (index 0) has `x_mitre_contents[]` for incremental sync — use this instead of full re-parse on updates
- Bundle is ~100MB; requires streaming parse (e.g. Jackson streaming API), not `readTree()`

#### Exploit Data

Field names confirmed from live Exploit-DB CSV.

| Table | Grain | Confirmed Field Names |
|-------|-------|-------------|
| `exploits` | 1 row per exploit | `exploit_id` (EDB: `id`), `file_path` (EDB: `file`), `description`, `date_published`, `author`, `exploit_type` (EDB: `type`: dos/remote/local/webapps/shellcode), `platform`, `port`, `date_added`, `date_updated`, `verified` (0/1), `cve_ids[]` (split EDB `codes` on `;`, filter `CVE-` prefix), `tags[]`, `source_url`, `source` |
| `metasploit_modules` | 1 row per module | `module_path`, `name`, `rank`, `cve_ids[]`, `platform`, `arch`, `type` |

**Exploit-DB parsing notes:**
- `codes` field is semicolon-delimited and mixes CVE IDs with OSVDB IDs (e.g. `CVE-2009-3699;OSVDB-58726`) — filter on `CVE-` prefix when populating `cve_ids[]`

#### IOC / Threat Feeds

Field names confirmed from live feed downloads.

| Table | Grain | Confirmed Field Names |
|-------|-------|-------------|
| `ioc_urls` | 1 row per malicious URL | `url_id` (URLhaus: `id`), `url`, `url_status` (`"online"`/`"offline"`), `date_added`, `last_online`, `threat` (URLhaus: `threat`), `tags[]` (split on `,`), `urlhaus_link`, `reporter`, `source` |
| `ioc_hashes` | 1 row per malware sample | `sha256` (MB: `sha256_hash`), `md5` (MB: `md5_hash`), `sha1` (MB: `sha1_hash`), `first_seen_utc`, `reporter`, `file_name`, `file_type` (MB: `file_type_guess`), `mime_type`, `signature`, `clamav`, `vt_percent` (MB: `vtpercent`), `imphash`, `ssdeep`, `tlsh`, `source` |
| `ioc_ips` | 1 row per C2 IP | `ip_address` (Feodo: `ip_address`), `port`, `status`, `hostname`, `as_number`, `as_name`, `country`, `first_seen`, `last_online`, `malware_family` (Feodo: `malware`), `source` |
| `ioc_mixed` | 1 row per ThreatFox IOC | `ioc_id`, `ioc_value`, `ioc_type` (`"ip:port"`/`"domain"`/`"url"`/`"md5_hash"`/`"sha256_hash"`), `threat_type`, `malware_key` (TF: `fk_malware`), `malware_printable`, `malware_aliases[]`, `first_seen_utc`, `last_seen_utc`, `confidence_level`, `is_compromised`, `reference`, `tags[]`, `reporter`, `anonymous`, `source` |
| `threat_pulses` | 1 row per OTX pulse | `pulse_id`, `name`, `author`, `tags[]`, `targeted_countries[]`, `malware_families[]`, `attack_ids[]`, `ioc_count`, `created`, `modified` |

**IOC feed parsing notes:**
- URLhaus: delivered as a ZIP archive — must decompress before CSV parsing; comment lines prefixed with `#`
- ThreatFox: `Auth-Key` header is **required** even for free tier; anonymous POST returns `{"error": "Unauthorized"}`
- Feodo Tracker: live endpoint is `/downloads/ipblocklist.json` (not `botnet_ip_blacklist.json`)
- MalwareBazaar: comment lines prefixed with `#`; `vtpercent`, `imphash` may be string `"n/a"`

#### Advisories

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `threat_advisories` | 1 row per advisory | `advisory_id`, `source`, `title`, `published_date`, `severity`, `cve_ids[]`, `affected_systems[]`, `ioc_count`, `url`, `tlp` |
| `advisory_iocs` | 1 row per IOC per advisory | `advisory_id`, `ioc_type`, `ioc_value`, `context` |

#### Standards / Compliance

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `nist_controls` | 1 row per control | `control_id`, `family`, `title`, `description`, `priority`, `baseline_impact[]`, `framework`, `version` |
| `nist_csf_functions` | 1 row per CSF function/category/subcategory | `function_id`, `function_name`, `category_id`, `category_name`, `subcategory_id`, `subcategory_name`, `framework_version` |
| `cis_controls` | 1 row per CIS control/safeguard | `control_id`, `safeguard_id`, `title`, `description`, `ig_group`, `asset_type`, `security_function`, `version` |
| `owasp_top10` | 1 row per entry per year | `entry_id`, `rank`, `name`, `description`, `cwe_ids[]`, `year` |
| `attack_to_nist_mappings` | 1 row per mapping | `technique_id`, `nist_control_id`, `mapping_type`, `score`, `comments`, `source_version` |

---

## Cross-Schema Joins

### Within cyber_vuln ↔ cyber_threat

```sql
-- Which KEV entries have public exploit code?
SELECT k.cve_id, k.vulnerability_name, v.cvss_v3_score, e.source, e.title, e.url
FROM cyber_vuln.kev_catalog k
JOIN cyber_vuln.vulnerabilities v ON k.cve_id = v.cve_id
JOIN cyber_threat.exploits e ON k.cve_id = ANY(e.cve_ids)
ORDER BY v.cvss_v3_score DESC;

-- Which KEV CVEs have ATT&CK technique coverage (via CWE cross-walk)?
SELECT k.cve_id, k.vulnerability_name, c.capec_id, t.technique_id, t.name AS technique
FROM cyber_vuln.kev_catalog k
JOIN cyber_vuln.vulnerabilities v ON k.cve_id = v.cve_id
JOIN cyber_threat.capec_patterns c ON v.cwe_ids && c.cwe_ids
JOIN cyber_threat.attack_techniques t ON c.attack_technique_ids && ARRAY[t.technique_id]
ORDER BY k.date_added DESC;

-- IOC count per threat actor group
SELECT g.name, COUNT(DISTINCT i.sha256) AS ioc_count
FROM cyber_threat.attack_groups g
JOIN cyber_threat.threat_pulses p ON g.technique_ids && p.attack_ids
JOIN cyber_threat.ioc_hashes i ON p.pulse_id = i.source
GROUP BY g.name
ORDER BY ioc_count DESC;
```

---

### cyber_vuln + sec (strongest cross-domain join)

Join key: `cyber_vuln.kev_catalog.vendor_project` ↔ `sec.filing_metadata.company_name`
and `sec.filing_metadata.sic_code` for sector-level analysis.

```sql
-- Which publicly traded companies are running actively exploited software?
SELECT s.company_name, s.ticker, s.sic_code, k.cve_id, k.vulnerability_name,
       k.date_added, k.due_date, v.cvss_v3_score
FROM cyber_vuln.kev_catalog k
JOIN cyber_vuln.vulnerabilities v ON k.cve_id = v.cve_id
JOIN sec.filing_metadata s
  ON LOWER(s.company_name) LIKE '%' || LOWER(k.vendor_project) || '%'
WHERE s.year = YEAR(CURRENT_DATE)
ORDER BY v.cvss_v3_score DESC;

-- Breach disclosures (8-K Item 1.05) cross-referenced with KEV
-- Note: SEC adapter owns the 8-K ingestion; cyber_vuln references it via view
SELECT f.cik, s.company_name, s.ticker, f.filing_date, k.cve_id, k.vulnerability_name
FROM sec.filings f
JOIN sec.filing_metadata s ON f.cik = s.cik
JOIN cyber_vuln.kev_catalog k
  ON f.full_text ILIKE '%' || k.cve_id || '%'
WHERE f.filing_type = '8-K'
  AND f.full_text ILIKE '%Item 1.05%';

-- Active KEV exploitation rate by SIC sector
SELECT s.sic_code, COUNT(DISTINCT k.cve_id) AS kev_cve_count,
       COUNT(DISTINCT s.cik) AS affected_companies
FROM cyber_vuln.kev_catalog k
JOIN sec.filing_metadata s
  ON LOWER(s.company_name) LIKE '%' || LOWER(k.vendor_project) || '%'
GROUP BY s.sic_code
ORDER BY kev_cve_count DESC;
```

---

### ref + sec + cyber_vuln (three-way: legal entity → registrant → breach)

Join chain: `ref.gleif_cik_mapping.cik` → `sec.filing_metadata.cik` → CVE cross-reference.
Enables: *CVE → exploited vendor → GLEIF legal entity → SEC registrant → breach filing*.

```sql
-- Full chain: vulnerability → legal entity → SEC breach disclosure
SELECT g.legal_name_language AS legal_entity, g.headquarters_country,
       s.ticker, k.cve_id, k.vulnerability_name, k.date_added
FROM ref.gleif_entities g
JOIN ref.gleif_cik_mapping m ON g.registration_authority_entity_id = m.registration_authority_entity_id
JOIN sec.filing_metadata s ON m.cik = s.cik
JOIN cyber_vuln.kev_catalog k
  ON LOWER(s.company_name) LIKE '%' || LOWER(k.vendor_project) || '%'
WHERE g.headquarters_country = 'US'
ORDER BY k.date_added DESC;
```

---

### fedregister + cyber_threat (regulatory response lag)

Join key: `fedregister.fr_agencies` filtered to CISA/NSA/FBI, date proximity to
`cyber_threat.threat_advisories.published_date`.

```sql
-- How long after a major advisory does a Federal Register action appear?
SELECT a.advisory_id, a.title AS advisory_title, a.published_date AS advisory_date,
       d.document_number, d.title AS fr_title, d.publication_date AS fr_date,
       DATEDIFF('day', a.published_date, d.publication_date) AS lag_days
FROM cyber_threat.threat_advisories a
JOIN fedregister.fr_documents d
  ON d.publication_date BETWEEN a.published_date AND a.published_date + INTERVAL '180 days'
JOIN fedregister.fr_agencies fa ON d.document_number = fa.document_number
WHERE fa.agency_name IN ('Cybersecurity and Infrastructure Security Agency',
                         'National Security Agency', 'Federal Bureau of Investigation')
ORDER BY lag_days;

-- Federal Register cybersecurity rules that reference specific CVEs
SELECT d.document_number, d.title, d.publication_date, fa.agency_name
FROM fedregister.fr_documents d
JOIN fedregister.fr_agencies fa ON d.document_number = fa.document_number
WHERE d.full_text ILIKE '%CVE-%'
  AND fa.agency_name ILIKE '%security%'
ORDER BY d.publication_date DESC;
```

---

### crime + cyber_threat (state-level context — strengthens when IC3 data added)

Join key: `crime.cde_estimates.state_abbr` ↔ state references in advisory geographic targeting.
Currently limited to state-level aggregation; becomes much stronger if FBI IC3 cybercrime
complaint data is added to the `crime` sub-domain.

```sql
-- States with high cybercrime advisory targeting vs. overall crime rates
SELECT cr.state_abbr, cr.violent_crime_rate, cr.property_crime_rate,
       COUNT(DISTINCT a.advisory_id) AS targeted_advisories
FROM crime.cde_estimates cr
LEFT JOIN cyber_threat.threat_advisories a
  ON a.affected_systems ILIKE '%' || cr.state_abbr || '%'
WHERE cr.year = 2023
GROUP BY cr.state_abbr, cr.violent_crime_rate, cr.property_crime_rate
ORDER BY targeted_advisories DESC;
```

---

### econ + cyber_vuln + sec (sector stress vs. exploitation frequency)

Join key: BLS sector codes mapped to SIC via a reference crosswalk. Answers whether
economically stressed sectors attract more active exploitation attempts.

```sql
-- Employment trend by sector vs. KEV exploitation count
SELECT e.series AS bls_series, e.value AS employment_index,
       s.sic_code, COUNT(DISTINCT k.cve_id) AS active_kev_count
FROM econ.employment_statistics e
JOIN sec.filing_metadata s ON /* SIC-to-BLS crosswalk — needs ref table */
JOIN cyber_vuln.kev_catalog k
  ON LOWER(s.company_name) LIKE '%' || LOWER(k.vendor_project) || '%'
WHERE e.year = 2024
GROUP BY e.series, e.value, s.sic_code
ORDER BY active_kev_count DESC;
```

Note: a SIC-to-NAICS/BLS crosswalk table would strengthen this join significantly. The
`ref` schema is the natural home for it.

---

## Environment Variables

Follows the same naming convention as existing govdata adapters (`FRED_API_KEY`, `BLS_API_KEY`, etc.).

### Authentication — Required for full functionality

| Variable | Source | Required? | Notes |
|----------|--------|-----------|-------|
| `CYBER_NVD_API_KEY` | NVD (NIST) | Recommended | Rate: 50 req/30s with key vs. 5 without. Register free at nvd.nist.gov |
| `CYBER_GITHUB_TOKEN` | GitHub | Required | GraphQL API requires auth. Any personal token with `public_repo` scope |
| `CYBER_THREATFOX_API_KEY` | ThreatFox (abuse.ch) | Required | Free registration at threatfox.abuse.ch |
| `CYBER_OTX_API_KEY` | AlienVault OTX | Required | Free registration at otx.alienvault.com |

### Authentication — Optional / commercial

| Variable | Source | Required? | Notes |
|----------|--------|-----------|-------|
| `CYBER_VULNDB_API_KEY` | VulnDB | Optional | Commercial license; skip in Phase 1 |
| `CYBER_VULNDB_API_SECRET` | VulnDB | Optional | OAuth secret paired with above |

### Storage and caching (mirrors existing GOVDATA_ pattern)

| Variable | Default | Notes |
|----------|---------|-------|
| `CYBER_CACHE_DIR` | `${GOVDATA_CACHE_DIR}/cyber` | Raw downloaded files (JSON, XML, STIX2, CSV) |
| `CYBER_PARQUET_DIR` | `${GOVDATA_PARQUET_DIR}/source=cyber` | Materialized Parquet output |

### Behavior tuning

| Variable | Default | Notes |
|----------|---------|-------|
| `CYBER_IOC_TTL_DAYS` | `90` | Days to retain IOC rows before eviction from hot partition |
| `CYBER_NVD_START_DATE` | `2002-01-01` | Earliest CVE publish date to ingest on full refresh |
| `CYBER_NVD_BATCH_DAYS` | `120` | Date window size per NVD API request (max 120 per NVD docs) |
| `CYBER_ATTACK_DOMAIN` | `enterprise,ics,mobile` | Comma-separated ATT&CK domains to ingest |
| `CYBER_OSV_ECOSYSTEMS` | `PyPI,npm,Maven,Go,RubyGems,NuGet,crates.io` | Ecosystems to pull from OSV GCS bucket |
| `CYBER_OTX_LOOKBACK_DAYS` | `30` | How many days back to pull OTX pulses on incremental refresh |
| `CYBER_EXPLOITDB_VERIFIED_ONLY` | `false` | When `true`, skip unverified Exploit-DB entries |
| `CYBER_INTEGRATION_TESTS` | `false` | Set `true` to enable integration tests (requires API keys) |

### Full env block for local development

```bash
# Auth
export CYBER_NVD_API_KEY=your_nvd_key
export CYBER_GITHUB_TOKEN=ghp_xxxx
export CYBER_THREATFOX_API_KEY=your_threatfox_key
export CYBER_OTX_API_KEY=your_otx_key

# Storage (inherit govdata dirs or override)
export CYBER_CACHE_DIR=${GOVDATA_CACHE_DIR}/cyber
export CYBER_PARQUET_DIR=${GOVDATA_PARQUET_DIR}/source=cyber

# Tuning
export CYBER_IOC_TTL_DAYS=90
export CYBER_NVD_START_DATE=2020-01-01
export CYBER_ATTACK_DOMAIN=enterprise
export CYBER_OSV_ECOSYSTEMS=PyPI,npm,Maven
export CYBER_INTEGRATION_TESTS=true
```

---

## Development Considerations

### IOC Feed Volume and TTL

IOC feeds (URLhaus, ThreatFox, MalwareBazaar) produce millions of rows. Unlike static vulnerability
records, IOCs have a useful lifetime — a C2 IP or phishing URL active in 2022 is noise today.

- Add a `ttl_days` config field to the YAML schema per table (e.g., URLhaus rows older than 90 days
  can be dropped from the hot Parquet partition)
- Partition IOC tables by `first_seen` date for efficient pruning
- Keep a `status` column (`active`, `inactive`, `unknown`) to filter without deleting history

### Refresh Cadence Tiers

The existing govdata `refreshInterval` (ISO 8601 duration) is sufficient but needs per-table
granularity for this schema:

| Tier | Cadence | Tables |
|------|---------|--------|
| Near-real-time | Hourly | `ioc_urls`, `ioc_hashes`, `ioc_ips`, `ioc_domains` |
| Daily | 24h | `kev_catalog`, `vulnerabilities`, `exploits`, `advisories` |
| Weekly | 7d | `attack_techniques`, `attack_groups`, `attack_software`, `capec_patterns` |
| Static (versioned) | On release | `nist_controls`, `nist_csf_functions`, `cis_controls`, `owasp_top10` |

### API Keys Required

| Source | Auth | Cost |
|--------|------|------|
| NVD | API key (free) | Rate: 5 req/s with key vs. 0.5 without |
| GitHub SA | Personal token (free) | GraphQL requires auth |
| ThreatFox | API key (free registration) | |
| AlienVault OTX | API key (free registration) | |

Env vars to add to the adapter config:
- `CYBER_NVD_API_KEY`
- `CYBER_GITHUB_TOKEN`
- `CYBER_THREATFOX_API_KEY`
- `CYBER_OTX_API_KEY`

### STIX2 Parsing

ATT&CK and D3FEND ship as STIX2 JSON bundles. The existing adapters don't handle STIX2. Options:

1. Parse manually (STIX2 is just JSON with typed objects + `relationship` objects for edges)
2. Use `mitre-stix2` Python pre-processing script to flatten to CSV before Java ingestion
3. Implement a `Stix2ResponseTransformer` in Java — preferred for consistency with existing pattern

STIX2 `relationship` objects encode the graph edges (technique → tactic, group → technique, etc.)
and need to be denormalized into the array columns on primary tables.

### CWE as a Shared Dimension

CWE IDs appear in NVD vulnerabilities, CAPEC patterns, OSV entries, and OWASP Top 10. Consider a
`cyber_vuln.cwe_catalog` table (sourced from `https://cwe.mitre.org/data/xml/cwec_latest.xml.zip`)
as a shared dimension that both schemas reference. Keeps CWE descriptions out of every table.

### PDF Extraction for Advisories

FBI/CISA joint advisories and Verizon DBIR ship as PDFs. The existing SEC adapter already has
HTML parsing; PDF is a new dependency. Options:

- Apache PDFBox (already in the Calcite dependency tree via other modules — verify)
- Pre-process to text with `pdftotext` (requires system binary; less portable)
- Accept HTML-only advisories for now and add PDF support in a later phase

### Deduplication Strategy

CVE IDs appear across NVD, MITRE CVE list, CISA KEV, GitHub SA, and OSV simultaneously. The
`vulnerabilities` table should be NVD-canonical (NVD as source of truth for CVSS scores and CPE
data) with other sources contributing via `vuln_cross_refs`. Dedup key: `cve_id` (normalized,
uppercase, e.g., `CVE-2021-44228`).

### SEC 8-K Item 1.05 Integration

Since July 2023, public companies must disclose material cybersecurity incidents in 8-K Item 1.05
filings. The existing `sec` sub-domain already ingests 8-K filings. Rather than duplicating that
ingestion in `cyber_vuln`, expose a view or cross-schema reference:

```sql
-- In cyber_vuln schema config: reference sec.filings where form_type = '8-K' and item = '1.05'
```

This avoids re-downloading SEC filings and keeps the SEC adapter as the single owner of that data.

---

## Implementation Phases

### Phase 0 — Shared infrastructure (prerequisite for all phases)

Must land before any downloader is written. Nothing else can start until this is done.

1. `CyberSchemaFactory` — single factory, `dataSource` operand selects `cyber_vuln` or
   `cyber_threat`; loads the corresponding YAML via `SchemaConfigReader`
2. `ttl_days` wiring — extend `SchemaConfigReader` to parse `ttl_days` from per-table YAML config;
   pass through `CyberCacheManifest` → `DuckDBCacheStore`. This is the missing link that
   `current_year_ttl_days` in `econ-schema.yaml` was intended to use but never implemented.
3. `CyberVulnCacheManifest` + `CyberThreatCacheManifest` — skeleton classes wiring factory →
   manifest → store
4. Model JSON files for all cadence tiers:
   - `cyber-vuln-daily.json` (NVD delta, CISA KEV)
   - `cyber-vuln-weekly.json` (OSV, GitHub SA, MITRE CVE)
   - `cyber-threat-hourly.json` (URLhaus, MalwareBazaar, ThreatFox, Feodo)
   - `cyber-threat-weekly.json` (ATT&CK, CAPEC, D3FEND)
   - `cyber-threat-static.json` (NIST 800-53, CSF, CIS Controls, OWASP)
5. `CYBER_INTEGRATION_TESTS` gate in test harness (mirrors `CALCITE_FILE_ENGINE_TYPE` pattern)
6. Compile-clean build with empty downloaders — no functionality yet, but the module wires up

---

### Phase 1 — cyber_vuln foundation (highest signal, cleanest APIs)

Delivers the two tables with the most immediate analytical value: every known CVE and the
active exploitation catalog. No cross-source complexity yet.

1. `cyber-vuln-schema.yaml` — `vulnerabilities`, `kev_catalog`, `affected_products`,
   `cwe_catalog` tables with confirmed field names from live API research
2. `CweDownloader` — XML from `cwec_latest.xml.zip`; establishes the canonical CWE dimension
   before any other table references it
3. `NvdDownloader` + `NvdResponseTransformer` — offset-based pagination (max 2000/page);
   delta mode via `lastModStartDate`/`lastModEndDate`; CVSS V31 preferred, V2 fallback;
   unwrap `vulnerabilities[n].cve` nesting
4. `CisaKevDownloader` — single JSON file; diff on `catalogVersion` (not ETag); split `notes`
   on `;`; handle empty `cwes` array on older entries
5. `PdfAdvisoryExtractor` — PDFBox utility class shared by later phases; extract text and
   basic IOC patterns (CVE IDs, IPs, domains) from PDF advisories
6. Integration tests: NVD with a 7-day date window, KEV full catalog, CWE full catalog

---

### Phase 2 — cyber_vuln cross-source enrichment

Adds the remaining vulnerability sources and cross-reference linkage.

1. `OsvDownloader` — HTTPS download from `osv-vulnerabilities.storage.googleapis.com`;
   per-ecosystem ZIPs controlled by `CYBER_OSV_ECOSYSTEMS`; handle nullable `affected`
2. `GithubSaDownloader` — GraphQL cursor pagination; `public_repo` scoped token
3. `MitreCveDownloader` — daily delta ZIPs from CVEProject GitHub releases
4. `vuln_cross_refs` table — populated by all three downloaders above; dedup key is
   normalized `CVE-YYYY-NNNNN` form
5. `advisories` table — CISA HTML feed; PDF fallback via `PdfAdvisoryExtractor` from Phase 1
6. `osv_vulnerabilities` table — OSV-native records (not CVE-keyed) for ecosystem-specific
   advisories with no CVE assignment

---

### Phase 3 — cyber_threat foundation + ATT&CK taxonomy

Establishes `cyber_threat` schema and delivers the ATT&CK taxonomy, which is the join target
for most subsequent threat tables.

1. `cyber-threat-schema.yaml` — `attack_techniques`, `attack_tactics`, `attack_mitigations`,
   `attack_groups`, `attack_software`, `attack_campaigns` tables with confirmed STIX2 field names
2. `Stix2ResponseTransformer` — Jackson streaming parser; two-pass: object map by STIX ID,
   then relationship resolution into array columns; use `x-mitre-collection.x_mitre_contents`
   for incremental sync; technique ID extracted from `external_references[source_name=="mitre-attack"]`
3. `AttackDownloader` — downloads versioned bundle JSON; domain controlled by
   `CYBER_ATTACK_DOMAIN` env var; streams ~100MB file
4. `CapecDownloader` — XML from `capec_latest.xml`; populates `capec_patterns` with
   `cwe_ids[]` cross-reference
5. `D3fendDownloader` — JSON-LD; populates `d3fend_techniques` with `attack_technique_ids[]`
6. `attack_to_nist_mappings` — CSV from CTID mappings-explorer repo

---

### Phase 4 — cyber_threat IOC feeds

TTL wiring from Phase 0 is a hard prerequisite — IOC tables are the primary consumer of
per-table `ttl_days`. All feeds partition by `first_seen` date.

1. `UrlhausDownloader` — decompress ZIP before CSV parse; comment lines prefixed `#`;
   split `tags` on `,`
2. `MalwareBazaarDownloader` — plain CSV; comment lines prefixed `#`; `vtpercent`/`imphash`
   may be string `"n/a"` — coerce to null
3. `ThreatFoxDownloader` — POST with `Auth-Key` header (mandatory); `ioc_mixed` table covers
   all IOC types (`ip:port`, `domain`, `url`, `md5_hash`, `sha256_hash`)
4. `FeodoDownloader` — JSON from `/downloads/ipblocklist.json` (not `botnet_ip_blacklist.json`)
5. `OtxDownloader` — pulse-based; lookback window controlled by `CYBER_OTX_LOOKBACK_DAYS`
6. TTL partition pruning — eviction job reads `ttl_days` from YAML (wired in Phase 0),
   drops Parquet partitions older than TTL from `ioc_*` tables

---

### Phase 5 — Standards, exploits, and integration

Completes the schema and validates cross-schema joins end-to-end.

1. `NistDownloader` — NIST 800-53 Rev 5 XML/JSON + CSF 2.0 JSON; populates
   `nist_controls` and `nist_csf_functions`
2. `CisControlsDownloader` — CIS Controls v8; populates `cis_controls`
3. `OwaspDownloader` — OWASP Top 10 from GitHub; version-pinned; populates `owasp_top10`
4. `ExploitDbDownloader` — `files_exploits.csv` from GitLab; split `codes` on `;`,
   filter `CVE-` prefix for `cve_ids[]`; `verified` is int 0/1
5. Cross-schema join integration tests — SEC + cyber_vuln (KEV/company name),
   ref + sec + cyber_vuln (GLEIF chain), fedregister + cyber_threat (advisory lag)
6. SEC 8-K Item 1.05 view — read-only reference into `sec.filings` filtered to
   `filing_type = '8-K'`; no re-ingestion of SEC data

---

## File Layout

```
govdata/
  src/main/java/.../govdata/cyber/
    vuln/
      CyberVulnCacheManifest.java
      NvdDownloader.java
      NvdResponseTransformer.java
      CisaKevDownloader.java
      OsvDownloader.java
      GithubSaDownloader.java
      MitreCveDownloader.java
      CweDownloader.java
    threat/
      CyberThreatCacheManifest.java
      AttackDownloader.java
      Stix2ResponseTransformer.java
      CapecDownloader.java
      D3fendDownloader.java
      ExploitDbDownloader.java
      MalwareBazaarDownloader.java
      UrlhausDownloader.java
      ThreatFoxDownloader.java
      OtxDownloader.java
      NistDownloader.java
  src/main/resources/cyber/
    cyber-vuln-schema.yaml
    cyber-threat-schema.yaml
    cyber-vuln-schema-factory.defaults.json
    cyber-threat-schema-factory.defaults.json
```
