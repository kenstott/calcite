# Judge — news q4, Fox Business "Why buying a home is harder today than in the 1980s" (Realtor.com analysis, 2026-11-27), 2026-11-28

**Question/claim:** Fact-check the Fox Business piece (https://www.foxbusiness.com/economy/why-buying-home-harder-today-than-1980s). Central claim: saving a 10% down payment took about 5.7 years in 2019 and now takes 10.5 years, more than triple the 1980s.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica (grep of everyman and expert agent.md returned no hit; no use of askamerica.ai).

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only. Fox via summary; Realtor.com via syndications (stocktitan, inman); Freddie Mac, BLS | Warehouse (FRED MORTGAGE30US, MSPUS, CPIAUCSL, PSAVERT; Census household income history) plus both Fox and Realtor.com pages loaded | Web only. Fox via summary; Freddie Mac, BLS, rismedia, builderonline |
| Vintage | Nov 2026 rate, Sept 2026 CPI | Week of 2026-11-25 rate, Sept 2026 CPI, 2019 and 2025 annual | Rates Nov 12 and 19 only, Sept 2026 CPI |
| Overall grade | 2 Pinocchios | 1 Pinocchio (report title "mostly accurate") | 2 Pinocchios |
| Claims graded | 11 | 10 (incl. 2 fidelity-only) | about 10 (6 listed as not checkable) |
| Central claim verdict | Supported as Realtor.com's finding; notes earlier 7-year figure, unexplained jump | Mostly true; reproduces about 10.5 years from the warehouse, driven by a lower saving rate | Not reproduced; "contradicted or unsupported" because Realtor.com said about 7 years |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

- Central claim. Realtor.com's own page could not be fetched (WebFetch refused the domain). Searches found only the 2025 Realtor.com figure (about 7 years, 12 at the 2022 peak) and no source that contradicts 10.5. The 7-year figure is for 2025 data. The Fox piece cites a new Nov 2026 analysis, so a different value is not a contradiction. Everyman found syndications repeating 10.5. Askamerica says it loaded the Realtor.com page and quotes it. Askamerica also rebuilt the number: 10% of the 2025 price ($41.5k) over income $87.5k at a 4.5% saving rate is about 10.5 years. That supports the claim as faithful to Realtor.com and explains why it differs from 7. Expert's "contradicted" is not supported by evidence; it compared two different analyses. Open item: confirm the exact Realtor.com text live.
- 18.63% in 1981: all three agree. Askamerica gives the date (1981-10-09) from the warehouse.
- 7.21%: askamerica found 7.21% for the week of 2026-11-25 and says 7.28% is the Nov 19 week. Everyman guessed 7.21% may be the Nov 26 reading and marked it unconfirmed. Expert called it close. Askamerica settled it.
- 3.7% CPI: all three tie it to September 2026. Everyman and askamerica both note BLS published no October annual rate. Expert did not check October.
- Price/income 3.2x vs 4.8x: askamerica used Census data (3.77 vs 4.75). Expert cited 3.47x from Realtor.com 1981 figures. Everyman left it as matching Realtor.com. Askamerica gave the only independent check.
- Fivefold/fourfold: askamerica checked it (4.7x/3.6x vs 1980s average; 6.3x/4.7x vs 1980) and noted the sentence is not in Realtor.com's article. Everyman and expert left it unverified.
- LA 85.3% and San Diego 64.8%: askamerica found the Realtor.com page says San Diego "nearly 65%" and never mentions LA, so graded unsupported. Everyman and expert said unverified or not checkable. Hannah Jones quote: only everyman covered it (identified her role, wording unconfirmed). Askamerica did not list it in agent.md or claims.json. Expert called it not checkable.
- Askamerica rating check. claims.json has 10 claims, the central claim "mostly true", no "false" verdict. Report title says "biased and mostly accurate". agent.md says 1 Pinocchio. "Mostly accurate" is the 1-Pinocchio band, so no mismatch.
- Open warehouse items (orchestrator to check live): PSAVERT 7.3% (2019) vs 4.5% (2026), MSPUS 2019 and 2025 values, Census income of $87.5k and $68.7k.

## Ranking rationale

1. askamerica. Only arm that read the Realtor.com source, resolved the 7.21% and 7-vs-10.5 questions, and rebuilt the central number from warehouse data. It found a real framing flaw: the piece ties down-payment time to mortgage rates, which do not enter that calculation, and the rise comes from the saving rate. It graded unsupported where evidence was absent (LA). Fidelity-only grades are labeled as such. Minor gap: no Hannah Jones quote check.
2. everyman. Good coverage of 11 claims, caught the CPI caveat, flagged the 7-year versus 10.5-year puzzle honestly without calling it false. No primary source for the central claim, and several claims left unverified. Calls 2 Pinocchios with "no outright errors", which is a little harsh against its own findings.
3. expert. Thinnest. Wrong central verdict: treats 7 years (2025) as contradicting 10.5 (2026). Six claims "not checkable". Did not check the October CPI gap. Its misleading-by-omission point on 1981 payment size is sound but the lead finding is wrong.

What would change the ranking: if the live Realtor.com text shows a different figure than 10.5, askamerica's "loaded the page" claim would need re-checking and everyman and expert would move up. If the warehouse PSAVERT or MSPUS numbers are wrong, askamerica's explanation weakens but its source reading still stands.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- Existing recipe `fact-check-a-news-article` covers the method. Askamerica followed it: fetched source page, graded fidelity versus independent checks, tested windows.
- Everyman's Hannah Jones quote lookup (role and attribution) is a pattern askamerica skipped. Attributed-quote checks are web-based; `fact-check-a-news-article` could add a line to cover every attributed quote. Warehouse-native part: none.
- Askamerica's saving-rate decomposition (econ.fred_indicators PSAVERT with MSPUS and census.household_income_history_national) is worth keeping; `fact-check-a-news-article` could name it as a method for "time to save" claims.
- Askamerica is #1.

Proposed resolution: d — askamerica ranked first; no actionable cause beyond an optional note in `fact-check-a-news-article` to check every attributed quote.

## Corpus gaps stated

- "The warehouse has no table to replicate it." ($150,000 buying power; fidelity-only, graded true to Realtor.com's own figure.)
- "Not replicated." (43 of 50 metros; metro table not available.)
- LA 85.3% and San Diego 64.8%: "the underlying report table was not read" (graded unsupported).
