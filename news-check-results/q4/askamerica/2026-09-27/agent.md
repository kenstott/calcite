# Fact-Check: "Your Home Sale Could Freeze on September 30" (Call The Local, NFIP guide)

**Article checked:** https://www.callthelocal.com/guides/nfip-flood-insurance-september-30-2026-deadline-home-closings

**Report saved to:** `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-09-27/report.html` (also published in-session; the http://127.0.0.1 link is not durable past this process).

## Split Pinocchios rating (as required — several graded claims are attributed to NAR, a named third party)

- **Fidelity: 2/4** — the article accurately, often verbatim, transcribes 8 of 9 statistics it attributes to NAR. Its state-by-state breakdown (13,460 FL / 3,140 TX / 1,840 CA monthly NFIP-dependent closings) does **not** match NAR's actual published figures (14,870 FL / 3,590 TX / 1,680 CA — confirmed via direct `web_fetch` of NAR's Economists' Outlook, March 11, 2025). FL and TX are understated, CA overstated — not a rounding artifact.
- **Claims accuracy: 3/4** — every NAR figure the article transcribed *correctly* is genuinely accurate. But the article's central premise — "put one date on your calendar in bold: September 30, 2026" — was superseded. **Congress already extended NFIP's authorization past September 30, 2026 to December 11, 2026 on September 2, 2026** (P.L. 119-103 / H.R.6500), confirmed independently by NAHB (published Sept 4, 2026: "National Flood Insurance Program Extended Through Dec. 11") and by CRS's own IN10835 report as revised September 11, 2026 ("The NFIP is currently authorized until December 11, 2026"). This is a headline claim the most recent primary-source data runs directly against — the count=3 case on the WaPo scale.

## This is a new, more consequential finding than the prior same-day check

The earlier run today caught the state-breakdown misquote (a fidelity problem) but used a single blended Pinocchio count and did not catch that the article's entire deadline framing had already been overtaken by events 25 days before this check. That is the more serious issue: a reader today planning a home closing around "September 30" is acting on a deadline that no longer applies — the actual authorization now runs through December 11, 2026.

## Claim-by-claim (11 claims checked, all against primary sources fetched directly this session)

| Claim | Attribution | Verdict |
|---|---|---|
| "Sept 30, 2026" is THE deadline to plan around | Article's own central framing | **FALSE/STALE** — extended to Dec 11, 2026 on Sept 2, 2026 |
| H.R.7148 (Sept 30, 2026 extension) signed by Trump Feb 3, 2026 | Article's own assertion | TRUE (accurate as a historical statement) |
| Oct 2025 shutdown lapse → reauthorized → lapsed again → H.R.7148 fixed it; dozens of extensions since 2017 | Article's own assertion | TRUE |
| ~1,360/day, ~41,300/month NFIP-dependent closings | Attributed to NAR | TRUE — verbatim |
| Grace period, claims still paid during lapse | Attributed to NAR/CRS | TRUE |
| Mandatory-purchase requirement suspended, not eliminated | Article's own assertion | TRUE |
| Risk Rating 2.0 caps: 18%/25% | Article's own assertion | TRUE |
| $1.3T / 4.7M policyholders / 23,000 communities | Attributed to NAR | TRUE — verbatim |
| ~500K sales/yr, ~1M jobs, ~$70B economic activity | Attributed to NAR (McGahn quote) | TRUE — verbatim |
| **State breakdown: 13,460 FL / 3,140 TX / 1,840 CA** | Attributed to NAR | **FALSE** — real NAR figures: 14,870/3,590/1,680 |
| Standard homeowners insurance excludes flood damage | Article's own assertion | TRUE |

## AskAmerica data-coverage finding

`disasters.nfip_policies` was queried live this session (`SELECT year, SUM(policy_count), SUM(coverage) ... GROUP BY year`) — a partial OpenFEMA extract (2,478,000 rows across 2010–2026, gaps present; only 23,945 rows for all of 2025). It cannot corroborate FEMA's true ~4.7M-policyholder national scale; national and state totals were instead verified against NAR's, CRS's, and NAHB's own published text via direct `web_fetch`.

## Materially misleading beyond the individual facts?

Yes — the stale core premise, independent of any single wrong number. The article's own site published other content dated September 11 and 13, 2026 (after the extension was public), yet this piece's headline framing was never updated. A reader relying on it today would over-prepare for a deadline that already passed without incident.

## Sources fetched directly this session (6 distinct primary/independent sources)
- NAR Economists' Outlook (Mar 11, 2025)
- NAR Realtor Magazine, "NFIP by the Numbers" (Oct 7, 2025)
- CRS IN10835 via EveryCRSReport (revised Sept 11, 2026)
- GovTrack H.R.7148 status page
- NAHB, "NFIP Extended Through Dec. 11" (Sept 4, 2026)
- The article itself
