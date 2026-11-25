# Judge — Farm Policy News, "Thailand trade deal will include $2.6 billion in US ag purchases, pork market access, Trump says", 2026-11-25

**Question/claim:** Fact-check the Farm Policy News article (Nov 2026): Trump says a US-Thailand deal includes about $2.6B in US ag purchases and pork market access. Grade it on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep of the everyman and expert answers for "askamerica" found no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only; USTR fact sheet, Census figures quoted | Warehouse (econ.trade_exports/imports), FAS page read directly, DTN, Nation, Investing.com, AgroLatam | Web plus shell; USTR fact sheet, Census, FAS, Nation |
| Vintage | 2025 trade, Oct 2025 framework | 2025 trade (warehouse ends 2025), Nov 2026 reporting | 2025 trade, Oct 2025 framework |
| Headline figures | $19.6B / $91.3B; $2.6B/yr dates to Oct 2025 | $19.59B / $91.26B, +45.0%; FAS $1.34B | $19.6B / $91.3B; FAS $1.34B; pork gains $10-25M |
| Pinocchio grade | 1 | 1 | 1 |
| Claims graded | 9 (3 "not checkable here", 1 unretrieved FAS) | 6 in claims.json (4 article, 1 Trump, 1 headline) | about 4 in summary |
| Central claim verdict | Accurate attribution; "will include" overstates, $2.6B not new | Headline "mostly true" but framing misleading; Trump's tariff claim "partially false" | Accurate relay; "additional" is framing, $2.6B/yr is Oct 2025 estimate |
| Visual | none | dashboard.png + report | agent-dashboard.png + report |

## Verification

Central claim: the article reports what Trump said. A web search today (The Hill, Yahoo/Investing.com, Thai Examiner via search summary) confirms: Trump announced about $2.6B in extra US farm purchases on Nov 17; a joint statement was signed, not a final agreement; the $2.6B figure dates to the Oct 2025 framework; Thai officials (Commerce Minister Suphajee, ex-PM Abhisit) dispute or soften it; sources split on one-time versus annual. This supports all three 1-Pinocchio grades. It supports askamerica most on Thai pushback, and everyman and expert on the Oct 2025 origin.

Numbers: Census exports 19,588.5M (everyman, expert) vs warehouse 19.59B (askamerica) match. Imports 91,255.3M vs 91.26B match. FAS 1.34B appears in all three. The $1.69B 2021 peak is DTN-only; askamerica flagged that and noted a different Census definition. No fabrications found.

Gaps by arm:
- everyman marked FAS "not retrieved" and the Rollins and LeGrand quotes "not checkable here". Calibrated, but the thinnest evidence. It missed Thai pushback.
- expert missed Thai Commerce Minister pushback and the "Nearly Double" inconsistency. Strong on the USTR origin and pork (USMEF).
- askamerica did not state that $2.6B originates in the Oct 2025 framework, which both others did. It cited the Oct 2025 framework only for the 99% figure.

askamerica claims.json vs report.html vs agent.md: report.html shows author honesty 88 ("mostly honest", 4 claims graded) and Trump 50 / 1 claim graded; agent.md states 1 Pinocchio. No mismatch: 88 maps to 1 Pinocchio. Minor note: the headline claim is graded "mostly true" while the prose calls the framing materially misleading; consistent enough. Open item: the SQL numbers (19.59B, 91.26B, 62.96B) come from the warehouse and were not re-queried by me; they match the Census figures the other arms retrieved.

## Ranking rationale

1. askamerica: most claims checked against primary data (warehouse Census tables, FAS page read directly). Surfaced Thai officials' dispute, the headline-versus-body gap, pork unresolved, and the "Nearly Double" versus "additional" conflict. States what it could not verify (403 pages). Weakness: did not trace $2.6B to Oct 2025.
2. expert: tight, correct, traced $2.6B to USTR and added pork economics. Fewer claims, no Thai pushback, quotes left unchecked (stated).
3. everyman: correct grade and caught the Oct 2025 origin, but left FAS and quotes unchecked and had the least independent evidence.

What would change the ranking: if the Oct 2025 origin of the $2.6B is judged the key misleading point, expert moves ahead of askamerica.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

askamerica is #1. Existing recipe `surface-known-dissent-on-contested-claims` covers the Thai pushback it found. Expert and everyman traced the figure to the original USTR framework; a broadened "check whether a headline figure is a re-announced earlier estimate" method may merit a recipe line, via the warehouse-free path of the cited primary document. No warehouse table exists for FAS agricultural trade by country (askamerica stated this).

Proposed resolution: d — askamerica ranked first; the only gap (not tracing $2.6B to Oct 2025) is minor.

## Corpus gaps stated

- "The warehouse has no 2026 trade data and no table for the FAS agricultural series."
- "Census HS 01-24 ag exports ... is a narrower definition than FAS." (definition mismatch, not a gap claim)
- $1.69B 2021 peak: "I did not read the FAS annual series" (graded mostly true on one source).
- Bloomberg and AgWeb 403, Farms.com empty: quotes not re-verified at primaries.
