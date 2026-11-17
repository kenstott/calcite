# Judge — news q8 247wallst-six-fdic-bank-failures-past-year

**Story:** "Six FDIC-insured banks failed in the past year; here's each one ranked by size, led by Irvine's Nano Banc" (24/7 Wall St., 2026-11-15). Claim of interest: six failures between Nov 14, 2025 and Nov 14, 2026; Nano Banc ($736M) was larger than the other five combined ($712.1M).

Rank: [askamerica, expert, everyman]

Judged 2026-11-17 on three answers produced the same day (this resume run).

## Why
- **askamerica (1st):** engine rating 1 Pinocchio, 12 claims (11 true, 1 mostly false). Read the piece in full. It found the piece's one real error: West Georgia's $97.3M is called the "largest published" cost estimate, but the piece's own table and the FDIC's Sept 25 release give Nano Banc $114M. West Georgia leads only relative to assets (about 32% vs 15.5%). It confirmed that the count does not depend on the window (no failures between June 27, 2025 and Jan 30, 2026), and that "pending" mislabels estimates the FDIC announced at closing. It noted that the state regulator put Nano's assets at about $690M at closing. It checked the delinquency figures against `econ.fred_indicators` (DRCCLACBS: 2.85% Q2 2026, 3.08% Q4 2024), and the trust and SIPC coverage rules against FDIC and SIPC pages. Weakness: it read the failures from the FDIC website and API, never from the warehouse's own `banking.failures` table (its one search_catalog call was for delinquency).
- **expert (2nd):** 1 Pinocchio. Found the same "largest estimate" error, the 2.85% / 3.08% match, the $19.6M vs $19.7M Metropolitan rounding, and the two asset figures for West Georgia ($288M at 12/31/2025 vs $305.7M). It read the piece through a summarising fetch, so its wording checks are medium confidence, and it did not re-fetch the coverage rules.
- **everyman (3rd):** 1 Pinocchio. Confirmed the list, names and dates, and found the same two minor figure discrepancies. It missed the "largest published estimate" error, the piece's one material mistake. It could not find the 3.08% figure, which the other two confirmed from the Fed series.

What would change the ranking: askamerica vs expert is close; both found the main error. askamerica's full-text read, window check and regulator figure put it narrowly ahead. If expert's $305.7M vs $288M point were an error in the piece, it would count for expert, but the $305.7M is the closing-date figure, so it is not.

## Recipe check
No recipe change. askamerica skipping `banking.failures` did not cost it a finding here, because the FDIC's own API carried the same data. If this recurs on a question where the warehouse table adds history (failure counts by year, cost-to-assets across all failures since 1934), it is worth a trigger on the fact-check recipe.

## Resolution
Not needed (askamerica ranked first). No gaps found: the figures are in FDIC and FRED data, both in the warehouse.

## Shape
Reviewed. 12 `author_claims`, no speakers. The piece has no quotes, and its figures are its own reading of FDIC releases. Compliant. The engine's report headline repeats "are mostly accurate" ("...with one self-contradicting cost claim (1 Pinocchio) are mostly accurate"), a cosmetic defect in the composed title.

## Severity invariant
Not triggered.
