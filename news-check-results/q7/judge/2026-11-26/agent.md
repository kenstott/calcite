# Judge: Pro Farmer, "Profit over peak yield: What crop budgets are telling growers", 2026-11-26

**Piece:** https://www.profarmer.com/news/agriculture-news/profit-over-peak-yield-what-crop-budgets-are-telling-growers (Hillari Mason, 2026-11-17). Fact-check it, extract the claims, and grade it on 0-4 Pinocchios.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. The everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. Neither unaided arm mentions askamerica.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | WebFetch summary | Full fetch | WebFetch summary |
| WASDE yields/prices (179.6 / 53.3; $4.60 / $12.10) | Confirmed; record status unconfirmed | Confirmed against primary WASDE; soybean record (53.0 prior); corn cut 1.6 bu from October and 6.9 below last year's 186.5 | Confirmed; record verified (53.0 in 2025) |
| DTN fertilizer | Confirmed | Confirmed | Confirmed |
| farmdoc breakevens | Upper ends ($5.20; $11.60) unmatched | Mostly true: looser than source; article's own internal inconsistency ($4.80 vs $4.90 low end); 2027 budget vintage beside 2026 prices | Loosely consistent; southern IL $5.39; 2027 vintage; returns at $5 corn |
| Spread / strip-till / nitrogen | Not checkable / no source | Unsupported | Not checkable |
| Other | — | FIELDS grant program verified | — |
| Grade | 1 | 1 (engine-computed after regrouping; first publish was 0) | 1 |
| Visual | none | report.html (no dashboard.png) | agent-dashboard.png, agent-report.html |

## Verification

- All three arms agree on the WASDE and DTN figures, and two arms independently confirmed the soybean record (53.0 the prior year). These are not in dispute.
- Askamerica said "no WASDE table," but the warehouse holds `ag.nass_crop_estimates_current`: current-year NASS national yields, observed coverage 2026, checked 2026-11-25. The November WASDE yields are the NASS Crop Production figures. Askamerica therefore missed a catalog route for claim 1 and confirmed it from the primary WASDE PDF instead. The verdict is right, but the warehouse was not used where it could have been. Season-average prices and DTN fertilizer prices are not in the warehouse, so primary sources were the correct route for those.
- Rating shape: the first publish put all 7 claims in `author_claims` (0 Pinocchios). After a resume, the WASDE, farmdoc and DTN relays are `fidelity` (3) and the framing claims are author claims (5). The engine's rating moved to 1. I verified this from claims.json. The grouping fix changed the rating, so this case shows why the shape matters.

## Ranking rationale

1. **askamerica.** It read the full text and covered the most claims. It found two things no other arm found: the article's internal breakeven inconsistency, and that "strong, steady yields" glosses over a corn yield cut from October and below last year. It also verified FIELDS. Its weak point is the missed NASS warehouse table for yields.
2. **expert.** It checked the claims most carefully against primary sources (record verified, the southern Illinois breakeven, returns at $5 corn) and framed the 2027 vintage issue clearly. It worked from a summary, so it covered fewer claims.
3. **everyman.** Its conclusions are correct, but it left the record status unconfirmed and could not match the breakeven ranges.

This is a close call between askamerica and expert. Weighting rigor on the breakeven claims above breadth would put expert first.

## Recipe check

No recipe change. The catalog miss (WASDE yields are in `ag.nass_crop_estimates_current` as NASS national estimates) is the single lesson. It is the first instance, and askamerica still placed first, so nothing is filed or changed today. If it recurs, the fix is a search_catalog synonym or table comment mentioning "WASDE," not a new dataset.
