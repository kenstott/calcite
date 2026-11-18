# Judge — news q9 theregister-cve-count-passes-100k-2026

**Story:** "This year's bug count just passed 100K" (The Register, 2026-11-13). Claim of interest: 2026 CVEs passed 100,000 on Friday, more than double 2025's total, with AI-driven bug hunting as the cause.

Rank: [expert, askamerica, everyman]

Judged 2026-11-17 on three answers produced the same day (this resume run).

## Why
- **expert (1st):** 1 Pinocchio (borderline 2), 29 claims. It counted NVD directly: 100,416 CVEs published Jan 1-Nov 13 against 49,972 in 2025 (2.01x), confirming the headline from the public registry rather than the vendor. It found errors askamerica graded true: Microsoft's October 1,474 is unsupported (coverage gives 1,407-1,416), not all 14 Copeland flaws are "make-me-root", and BOD 26-04 replaced KEV-driven deadlines, not CVSS-only ones. It added framing points: median days-to-exploit is measured only on CVEs already exploited, with an immature 2026 cohort; VulnCheck's 1.4% exploited share compares with CISA KEV's 0.30%; CrowdStrike says the speed pattern predates frontier AI.
- **askamerica (2nd):** engine rating 0 Pinocchios, 17 claims, with Bressers's quotes as `subject_claims` and two `fidelity` claims. It found the "literally minutes later" quote partly false (1h40m, from VulnCheck's own post), that the exploited ratio fell from 2.7% to 1.4% rather than staying "consistent", and that the Iranian attribution covers France only. It graded the AI-majority and second-half claims unsupported, and flagged the single-vendor reliance. Its warehouse query undercut it: `cyber_vuln.vulnerabilities` returned 75,986 for 2026, so it wrote that "NVD lags VulnCheck's broader count", when NVD itself had passed 100,000. It graded Microsoft's 1,474 and the BOD 26-04 description true.
- **everyman (3rd):** 1 Pinocchio. It worked from search results and secondary coverage only, and opened no primary source. It supported the headline figures, correctly noted that the 120-to-80 figure is time to KEV listing and that BOD 26-04 binds only federal civilian agencies, and graded most specific figures not checkable.

What would change the ranking: with a complete NVD table, askamerica would have confirmed the 100K headline from the registry as expert did. Expert's three extra factual catches would still keep it ahead. askamerica is clearly ahead of everyman.

## Recipe check
No recipe change. The method expert used (count the public registry directly instead of trusting the vendor's live count) is what askamerica tried. The warehouse table returned the wrong number.

## Resolution
Data defect, filed: govdata-ops#1301. Verified live against the NVD CVE 2.0 API on 2026-11-17: 101,138 CVEs published 2026-01-01..11-17 (22,140 + 37,096 + 41,902 across three windows), against 75,986 in the warehouse. 2025 matches (49,972).

## Shape
Reviewed. 11 `author_claims`, 4 `subject_claims` (speaker Josh Bressers), 2 `fidelity` (VulnCheck and CrowdStrike findings relayed). Compliant.

## Severity invariant
Not triggered.
