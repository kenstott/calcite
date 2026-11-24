# Judge — News q2 (fortune-grist-data-centers-share-state-electricity), 2026-11-24

**Question/claim:** Fortune/Grist (2026-11-20): data centers went from about 3% of Oregon electricity (2015) to about 23% (2025), about 96% of Oregon sales growth 2020-25; Harvard/Nature Sustainability study of 403 hyperscale facilities, about 82 TWh/yr, about 2% of US electricity; state shares Oregon 18.6%, Virginia 15.5%, Iowa 13.1%.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of everyman and expert answers (agent.md, agent-report.html) for "askamerica" found no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only: arXiv, EurekAlert, OPB, Warren release | EIA retail sales table (energy.eia_electricity_prices) plus web: Dominion 10-K, EIP, Cleanview, Oregon DOE, OPB | Web: arXiv, EIA state profiles (web), OPB, Reuters, EIP |
| Vintage | 2024-25 study; OPB 2026-09 | EIA 2024 and 2025 sales | EIA 2024 |
| Headline figures | 82 TWh (68-99), 1.8%; 23% OR 2025 verified | OR 22.7% (14.0/61.54); sales +10.53 TWh 2020-25; VA 138.04, IA 54.93, OR 59.68 TWh | OR 11.1/59.68 = 18.6%; VA 21/138.0 = 15.2% |
| Overall grade | 1 Pinocchio | 1 Pinocchio | 1 Pinocchio |
| Claims graded | 13 rows | 12 | 10 rows |
| Central claim verdict | 23% verified; 3% and 96% not found | Central (3% to 23%) graded true; 96% unsupported but arithmetic consistent | 23% true; 3% and 96% unverified |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, report |

## Verification

Central claim, checked myself: OPB (2026-09-17) states about 23% of Oregon retail sales in 2025. It gives no 2015 figure and no 96% sales-growth share. So the 23% is supported by a primary-adjacent source; the 3% and 96% are not confirmed by it. All three arms reached this same position (23% verified, 96% unverified or only plausible).

- 82 TWh and about 2%: all three agree. Study says 1.8% of consumption; askamerica notes 82/3975.4 = 2.06% of 2024 EIA retail sales. Both everyman and askamerica call "about 2%" a mild rounding. Consistent.
- Oregon 18.6%: expert recomputed 11.1 / 59.68 = 18.6%. askamerica's EIA denominator 59.68 TWh matches expert's 59.68 exactly. askamerica implied numerator 11.1 TWh, same as expert. Consistent. everyman did not check it.
- Virginia 15.5%: expert 21/138.0 = 15.2%; askamerica denominator 138.04, implied numerator 21.4 (21.4/138.04 = 15.5%). Consistent.
- Iowa 13.1%: askamerica denominator 54.93, implied 7.2 TWh; no arm read the numerator. Open.
- Dominion 28%: askamerica read the Dominion 10-K (28% in 2025, 26% in 2024). expert could not find 28% (found 27% Reuters, ">25%" CEO). everyman did not open the filing. askamerica is best supported here.
- CO2 26 Mt versus arXiv preprint 37-54 Mt: everyman and expert both caught the preprint/release mismatch. askamerica quoted 25.7 Mt only and did not flag it.
- Context miss caught only by expert: EIA agreed in April 2026 to a mandatory survey, so "no federal tracking" is incomplete.
- Mixed-universe framing (23% all facilities 2025 vs 18.6% hyperscale 2024): askamerica and expert both flagged; everyman judged the article's framing fair.
- Virginia 15.5% vs Dominion 28% as an "undercount": askamerica flagged mismatched year, geography and scope. Only askamerica caught this.
- Open item for the orchestrator: askamerica's warehouse values (OR 51.02 to 61.54 TWh 2020-25; VA 138.04/147.6; US 3975.4) cannot be checked by me. They agree with expert's independently found EIA figures for OR 2024 and VA 2024.

askamerica claims.json versus report: report.html states "Rating: 1 Pinocchio", Honesty 91, Bias 0, 12 claims graded, 6 fell short. agent.md states 1 Pinocchio, Honesty 91, Bias 0. No mismatch. Verdict counts in claims.json: 5 true, 2 mostly true, 5 unsupported. Note: report.html repeats the claim table several times in extracted text; this looks like a rendering artifact, not extra claims.

Penalties: askamerica graded 5 claims "unsupported" with stated reasons (paywall, HTTP 403, PDF over fetch cap), which is calibrated. everyman marked several "Not checked" without trying (Dominion, Oregon DOE quote, NYT), and its Oregon/Virginia/Iowa shares were marked not checkable even though EIA denominators were reachable. expert skipped the Oregon DOE quote and Iowa.

## Ranking rationale

1. askamerica: widest primary-source coverage (Dominion 10-K, Oregon DOE report, EIP, Cleanview, Warren release) and used the warehouse EIA table to recompute shares and test the 96% claim arithmetically. Best framing catch (VA vs Dominion mismatch). Calibrated "unsupported" use. Misses: CO2 preprint/release mismatch and the April EIA survey context.
2. expert: correct central verdict, independent recomputation of OR and VA shares, caught CO2 mismatch and EIA survey context. Fewer claims (10), left Dominion, Oregon DOE and Iowa unresolved, and read Fortune only through a summarizing fetch.
3. everyman: correct grade and good CO2 and Guidi-quote catches, but no recomputation of state shares, several claims left unchecked that were reachable, and it judged the framing fair where the others found a blend of universes.

What would change the ranking: if the warehouse OR/VA/IA sales values prove wrong on a live check, askamerica drops below expert. If the Dominion 10-K 28% is not in the filing, askamerica's one clear lead over expert narrows.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- Worth teaching: expert and everyman both flagged the CO2 figure differing between preprint and published version, which askamerica missed. A version-check step for study figures fits the existing web-sourced recipe about headline statistics that cannot be verified directly (grep recipes.json: "headline statistic from a domain you don't recognize"; also the as-of-date recipe at the "fixed 'as of' date" entry). No new warehouse method is needed.
- Expert's EIA survey context (April 2026) is a web-only catch; no warehouse table applies.
- askamerica's use of energy.eia_electricity_prices for state-share denominators and growth arithmetic worked well and is a pattern to keep. No existing news-check-specific recipe was found by grep for "news", "fact" or "electric"; a short recipe for "share-of-state-sales claims: recompute denominator from EIA sales and state the numerator universe and year" is warranted.

askamerica is #1, so no cause classification is required.

Proposed resolution: b — add a short recipe for share-of-state-sales claims (EIA denominator by year, state numerator universe) and a preprint-versus-published-version check for study figures.

## Corpus gaps stated

- "Oregon 96% sales-growth share": "The full ECOnorthwest PDF exceeds the fetch cap, and neither OPB nor the slides state it." (web gap, not a warehouse table gap)
- "State shares of 18.6/15.5/13.1%": "The numerators were not read, though the denominators are confirmed." (numerators are in study supplementary data)
- "About two dozen of 74 plants permitted": "permitted count was not found in the extractable EIP text."
- NYT emissions comparison and 10/325 g/kWh: "NYT page returned HTTP 403; Heated and EPA eGRID pages did not contain the 325 or Jordan figures. Warehouse eGRID table is subregion-level (not state) and was not used." (this is the one stated warehouse limit: eGRID is subregion-level, not state)
- "Not checked: West Virginia 'second to Texas, more than 15 GW'."
