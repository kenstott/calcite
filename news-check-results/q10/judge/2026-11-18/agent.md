# Judge — news q10 agoptimus-lean-hog-record-fund-net-short-cot

**Story:** "Grain & Livestock COT Report, November 17, 2026" (Ag Optimus / Optimus Futures). Claims of interest: lean hogs managed-money net short 51,084, a record in the futures-and-options combined series since June 2006 (prior 50,492); corn net short 62,311; "lowest since" and "largest since" qualifiers across six markets; corn harvest progress.

Rank: [expert, askamerica, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **expert (1st):** 0 Pinocchios. Matched all six net positions to the CFTC combined report and checked the hog record against all 1,066 weekly readings since June 2006. Confirmed the soybean and live-cattle qualifiers. Found one imprecision: corn shorts rose 42,439 while the net change was 39,412, against the article's "added 39,411 contracts of short". Noted that the record depends on the report basis (futures-only: hogs −62,236).
- **askamerica (2nd):** engine 1 Pinocchio. Confirmed the central hog figure exactly from CFTC's combined data (79,486 long − 130,570 short; 50,492 the prior week). From the warehouse (`cftc.cot_disaggregated_futures`) it found the futures-only hog net short of −62,236, also a 2006–2026 record. It confirmed corn harvest progress (81%/88%, Iowa 72%/84%) against USDA crop progress in the warehouse, which no other arm checked. Several "lowest since" dates did not reconcile because the warehouse holds futures-only, and it did not confirm the combined series before 2022.
- **everyman (3rd):** 1 Pinocchio. Matched all six positions to the CFTC report but could not check any record or "since" qualifier.

What would change the ranking: askamerica's harvest-progress check widens its coverage beyond expert's. If the "lowest since" mismatches were weighted lightly, the two would be close to tied. Expert's full-history check of the headline record is the decisive difference.

## Recipe check
No recipe. The mismatch is basis (combined vs futures-only), and the warehouse lacks the combined series.

## Resolution
Filed kenstott/govdata-ops#1329 (`type:sourcing`, `kind:gap`, `schema:cftc`, `status:open`): CFTC Disaggregated Futures-and-Options Combined (Socrata kh3c-gbw2, verified live). The cftc schema loads only futures-only 72hh-3qpy, and no prior issue exists.

## Shape
claims.json: 6 `author_claims`; the piece is an analyst's own write-up with no named third-party quotes (expert also found none). Correct. Extraction was sparse at 6 claims for a dense piece, though the agent's prose covered the rest.

## Severity invariant
Not triggered.
