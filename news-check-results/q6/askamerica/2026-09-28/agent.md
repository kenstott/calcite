## Verdict: Mostly false, as attributed

**Claim under test:** American Farm Bureau Federation (AFBF), in a July 16, 2026 publication, claims combined farm losses of $34.6 billion.

**Finding:** $34.6 billion is a real AFBF number, but it does not appear in AFBF's July 16, 2026 publication. It comes from AFBF's earlier Market Intel piece, **"Farmers Urgently Need Economic Assistance" (Nov 24, 2025)**, where it is the projected national-average *returns-over-total-cost* shortfall for **nine principal row crops** (corn, soybeans, wheat, cotton, rice, barley, oats, peanuts, sorghum) in the **2025/26 crop year**, before crop insurance indemnities or any government support: "combined annual returns below total costs ... from the 2023/24 to 2025/26 crop years at -$20.2 billion, -$34.8 billion and -$34.6 billion, respectively."

AFBF's actual **July 16, 2026** publication — the news release "Farmer Losses Projected to Deepen" and the underlying Market Intel "Persistent Losses Leave Farmers Needing Economic Support" (both fetched and read in full) — headlines **$31 billion** projected loss for 2026 and **$32 billion** for 2027 (same nine-crop, returns-over-total-cost metric), plus "over $7 billion" in 2025 specialty-crop losses and "$41.4 billion" combined nine-crop losses projected for 2027. AFBF revised its own estimate for the comparable crop year down from $34.6B to $31B between the two reports. $34.6 billion does not appear anywhere in the July 16, 2026 materials.

Attribution: across all three AFBF pieces (Nov 2025, Jan 2026, Jul 2026), the losses are attributed primarily to a **structural cost-price squeeze** — elevated input costs (fertilizer, fuel, interest, labor, up 25–71% since 2020) against low/falling commodity prices — not primarily to tariffs. Trade/export losses to China are cited as a compounding factor in the Nov 2025 piece; tariffs are not the headline driver named for the $31–41B figures.

## What this corpus could and couldn't check

- **Table used:** `ag.ers_farm_income` (USDA ERS Farm Income and Wealth Statistics, US total, 1910–2026, observed live 2026-09-28).
- **Gap:** No USDA ERS *Commodity Costs and Returns* table (the per-crop, per-acre cost-vs-revenue series AFBF's $34.6B/$31B/$32B figures are actually computed from) is loaded in this corpus. `ag.ers_farm_income` is a whole-farm-sector aggregate, not a per-crop cost breakdown, so AFBF's exact arithmetic could not be independently reproduced. **Filed as a sourcing gap** on `kenstott/govdata-ops` (type:sourcing, kind:gap, schema:agriculture, status:open) after confirming no duplicate existed.
- **Independent cross-check that WAS possible:** `SELECT "year", category, subcategory, amount FROM ag.ers_farm_income WHERE state='US' AND category='Net income' AND subcategory='Farm income' AND "year">=2018 ORDER BY "year"` — US net farm income (all commodities + government transfers) was **positive every year 2023–2026**: $146.8B (2023), $128.3B (2024), $162.6B (2025 ERS forecast), $158.4B (2026 ERS forecast). Government-transaction payments swung from -$4.6B (2023) to +$9.8B (2025) to +$28.4B (2026), consistent with the $23B+ in ECAP/Farmer Bridge Assistance/ASCF aid AFBF itself describes. Crop cash receipts (`category='Crops'`) fell from $282.7B (2022) to $238.4B (2025), then an estimated rebound to $253.0B (2026) — directionally consistent with AFBF's pressured-margins narrative, but revenue, not the cost-adjusted return AFBF's headline figures measure.
- **Conclusion:** row-crop margins genuinely have been squeezed for several years, and AFBF's per-crop loss arithmetic is a real, traceable methodology (USDA ERS Commodity Costs and Returns + WASDE + NASS acreage + FAPRI projections) — but the whole farm sector did **not** post a $34.6 billion net loss in any of these years; describing the figure as "combined farm losses" overstates its scope, and attributing it to the July 16, 2026 report misdates AFBF's own, since-revised estimate.

## Compliance

- Pinocchio rating used the **mandatory SPLIT shape** (claim is attributed to AFBF, a named third party): fidelity = 3/4 (misdated/stale figure presented as AFBF's current position), claims_accuracy = 2/4 (real but narrower-than-framed metric).
- `score_claim` was called before publishing; its verdict ("misleading," confidence 0.61) didn't map onto `publish_report`'s fixed verdict enum (no "misleading" option), so "mostly false" was used with an explicit `score_claim_override_reason` documenting the vocabulary mismatch — not a substantive disagreement with the evidence.
- Sourcing gap filed live (verified via `list_tables`/`search_catalog`, not guessed) before finishing: no duplicate existed on `kenstott/govdata-ops`.

## Report

Published via `publish_report` with dashboard inlined (2 line charts + 2 stat tiles comparing AFBF's own crop-year loss estimates across its Nov 2025 / Jul 2026 publications, and the US net-farm-income series). Local ephemeral link: http://127.0.0.1:56681/a/ff52b62fecfc41d27a6b566bb16df71c.html (dies with this process). Saved copy: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q6/askamerica/2026-09-28/report.html`.

**Sources:**
- [AFBF: Farmer Losses Projected to Deepen (Jul 16, 2026)](https://www.fb.org/news-release/farmer-losses-projected-to-deepen)
- [AFBF: Persistent Losses Leave Farmers Needing Economic Support (Jul 16, 2026)](https://www.fb.org/intel/markets/persistent-losses-leave-farmers-needing-economic-support)
- [AFBF: Farmers Urgently Need Economic Assistance (Nov 24, 2025) — source of $34.6B](https://www.fb.org/market-intel/farmers-urgently-need-economic-assistance)
- [AFBF: Significant Farm Losses Persist, Despite Federal Assistance (Jan 21, 2026)](https://www.fb.org/intel/markets/significant-farm-losses-persist-despite-federal-assistance)
