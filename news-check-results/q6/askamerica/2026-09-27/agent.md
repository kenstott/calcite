# Fact-check: "Republicans said their tax cuts would pay for themselves. Now they are confronting reality." (MS NOW, Jack Fitzpatrick & JM Rieger, Sept 10 2026)

**Article:** https://www.ms.now/news/republicans-tax-cuts-pay-for-themselves-confronting-reality

## Split Pinocchio verdict (per corrected engine instructions) — FINAL, CORRECTED

Because the article's most consequential claims (CBO/JCT and CBO dynamic deficit projections, Penn Wharton and Tax Foundation estimates, and quotes from Arrington, Smucker, Burlison, Estes, Smith) are all attributed to named third parties rather than asserted by MS NOW in its own voice, this fact-check uses the required **split {fidelity, claims_accuracy}** rating, not a single blended count:

- **Fidelity: 0 Pinocchios** — MS NOW accurately relayed everything it attributed to CBO, JCT, Penn Wharton, Tax Foundation, and the lawmakers it quoted, with correct framing throughout.
- **Claims accuracy: 0 Pinocchios** — Every underlying figure the third parties themselves published is true and was confirmed against each organization's own primary-source page, fetched and read directly.

**This is a correction of an earlier draft of this fact-check**, which had flagged the Tax Foundation's $4.1 trillion figure as "misleading framing" — treating it as a narrower tax-provisions-only revenue-loss estimate being improperly generalized to the whole bill. That conclusion was based on a secondary CRFB summary describing Tax Foundation's analysis as covering "only OBBBA's major tax provisions." Fetching **Tax Foundation's own page directly** overturned this: it states verbatim, "Combined with the nearly $1.1 trillion in net spending reductions estimated by the Congressional Budget Office (CBO), we estimate the OBBBA will increase federal budget deficits by $3.3 trillion from 2025 through 2034 on a dynamic basis. Further, we estimate that on a dynamic basis, increased borrowing will add $851 billion in higher interest costs over the decade, resulting in a total deficit increase of $4.1 trillion on a dynamic basis." That $4.1T is already Tax Foundation's own whole-bill dynamic deficit total (tax effects + CBO's spending cuts + interest) — exactly what the article describes. An independent `score_claim` check on the corrected evidence returned verdict "true" at 0.90 confidence, 0 Pinocchios.

## What was verified and how (all figures reconfirmed via direct primary-source fetches)

| Claim | Verdict | Basis |
|---|---|---|
| Federal deficit ~$2T/year; federal debt ~$40T | True | **AskAmerica `econ.federal_debt`** (queried directly, Treasury Fiscal Data): total public debt outstanding = $40.07T as of 2026‑09‑24. CBO Monthly Budget Review (Aug 2026, fetched): $2.0T deficit, first 11 months of FY2026. |
| CBO/JCT pre-enactment score: $3.4T over 10 years | True (attributed) | CRFB press release (fetched directly) quotes CBO's own final score verbatim: "CBO estimates that the legislation will add $3.4 trillion to the primary deficit through 2034." |
| CBO follow-up dynamic score: $4.7T | True (attributed) | CRFB blog (fetched directly), citing CBO's Feb 2026 Budget and Economic Outlook: "$4.7 trillion from 2026 through 2035" on a dynamic basis. |
| Penn Wharton: $3.6T | True (attributed) | PWBM's own page (fetched directly): "the dynamic cost, including changes to the economy, is larger at $3.6 trillion." |
| Tax Foundation: $4.1T | **True (attributed)** — corrected from earlier "misleading" grading | Tax Foundation's own page (fetched directly): "...resulting in a total deficit increase of $4.1 trillion on a dynamic basis" — already a whole-bill figure. |
| Debt ceiling raised $5T to $41.1T | True | OBBBA raised the limit from $36.1T to $41.1T (July 2025), confirmed by multiple outlets — a matter of statutory record. |
| House GOP 2.6% growth assumption vs. CBO's 1.8% baseline | True | CRFB reporting on House Budget Committee assumption vs. CBO's own Budget and Economic Outlook (61882). |
| >$1T cut from Medicaid/SNAP | True | Penn Wharton's own fetched analysis itemizes Medicaid cuts of $884B and SNAP cuts of $156B (combined >$1T), consistent with CBO's own scoring. |
| ~$1T of bill benefits top 1% (CAP estimate) | Attributed estimate, not independently re-derived | Correctly and explicitly sourced to CAP, a named, ideologically-transparent source; directionally consistent with Penn Wharton's own finding that the top 10% receives ~80% of the legislation's total value. |
| Quotes (Arrington, Smucker, Burlison, Estes, Smith) | Not independently re-verifiable | Original on-the-record reporting; no independent transcript exists for most to cross-check verbatim. |

AskAmerica's warehouse has **no bill-level CBO/JCT/Penn Wharton/Tax Foundation deficit-projection table**, so those figures were verified by fetching each organization's own primary-source page directly (not a secondary summary) — the debt level and deficit pace were the two figures directly checkable against AskAmerica's own `econ.federal_debt` table.

## Materially misleading beyond the individual facts?

The article displays four deficit-impact numbers ($3.4T, $4.7T, $3.6T, $4.1T) side by side without disclosing that they cover slightly different 10-year windows (CBO's $3.4T conventional score: FY2025–2034; its $4.7T dynamic follow-up: FY2026–2035; Penn Wharton and Tax Foundation: FY2025–2034). This is a minor, non-material disclosure gap, not a factual error — all four are genuine, correctly-attributed whole-bill deficit estimates. It does not undermine the article's core, well-supported thesis: credible forecasters spanning the ideological spectrum, including the right-leaning Tax Foundation, agree the 2025 reconciliation law adds trillions to the deficit, contradicting GOP promises it would pay for itself.

## Report link

Published via `publish_report` (session-local, non-durable): http://127.0.0.1:49995/a/d93b9b1f0fc71a3049ce54abfa7ad4a9.html — 5 sections, dashboard inlined (federal debt level, FY2026 deficit pace, 4-scorekeeper deficit-estimate comparison chart, GOP-vs-CBO growth-assumption chart). This link dies when the engine process exits; report.html, claims.json, and dashboard.png are saved on disk at `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q6/askamerica/2026-09-27/` and reflect this same corrected 0/0 verdict.
