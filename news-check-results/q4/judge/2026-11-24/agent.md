# Judge — News q4 (icn-montana-wildfire-homeowners-insurance), 2026-11-24

**Question/claim:** Fact-check the Inside Climate News article (2026-11-24) on Montana wildfire and homeowner insurance costs. Central claim: Montana premiums are up roughly 40% since 2019 (APCIA).

Rank: [everyman, expert, askamerica]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of the everyman and expert answers for "askamerica" found no hits, so neither unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | WebFetch summary only | None. Engine web_fetch got HTTP 403; snippets only | WebFetch summary only |
| Data used | Web: NAIC report, HJ 61 text, CFA testimony, bill coverage | Web: APCIA HJ61 deck, Insurify, CSI Montana. Warehouse search found nothing relevant | Web: NAIC, MT code, CFA/EQC, GAO |
| Claims graded | 11 rows (6 graded, 5 marked not checked / not checkable) | 4 (1 true, 3 unsupported) | About 6 graded, 6 listed as not checked |
| Pinocchio grade | 1 Pinocchio | "2 of 4, provisional"; engine report says "too few to rate" | 1 Pinocchio |
| Central claim (~40% APCIA) | Partly verified; HJ 61 gives 44.3% for 2019-2024; roughly consistent | Unsupported; not in APCIA deck | Both 40% and 10% found; different bases; medium confidence |
| Visual | None | report.html | agent-report.html, dashboard PNG |

## Verification

- I fetched the article through WebFetch (summary form). It confirms: "homeowners insurance premiums in Montana have increased roughly 40 percent since 2019" (APCIA); "increased by 10 percent between 2021 and 2024" (legislature committee, citing CFA); NAIC "between 18.3 percent and 43.3 percent" and nonrenewals "between 96 percent and 216 percent".
- A web search found no APCIA page stating the 40% figure. The APCIA HJ61 deck excerpt does not show it. So the attribution cannot be confirmed from a primary source by me. The article's own figure matches what everyman and expert report as consistent with legislature material (everyman cites 44.3% in HJ 61 text; I did not confirm that number).
- The 10% CFA figure is supported by search (CFA, 2021-2024). All three arms agree.
- NAIC range: everyman and expert both verified it. askamerica never reached it because it never read the article.
- Central verdict: the evidence supports everyman and expert's "roughly consistent, mixed baselines" over askamerica's "unsupported". askamerica graded as unsupported a number it could not trace in one deck, while the article may cite a different APCIA document. It said so itself, but still stamped the claim unsupported and central.
- Penalties: askamerica graded the Brown quote and the "double, triple, quadruple" claim unsupported with no evidence either way; "not checkable" fits better. askamerica missed NAIC, HB 533, HB 136, the 70% acreage and two-thirds WUI claims. everyman and expert missed little more than the 70%/WUI items, which all arms left unchecked.
- Rating mismatch (askamerica): engine report.html says "too few to rate; 2 claims could not be confirmed" (honesty 100/100 on 3 author claims, 1 true and 2 unsupported). agent.md states "2 of 4 Pinocchios (provisional)". These do not agree. Also, the report row for claim 2 (Brown) sits in a separate party table, and claims.json marks claims 1 and 2 both central.
- Open item for the orchestrator: whether the article's APCIA 40% traces to an APCIA document other than the HJ61 deck.

## Ranking rationale

1. everyman: widest claim coverage, primary sources (NAIC, HJ 61, CFA), clear "limit" note about the summary-only text, calibrated not-checked labels, useful misleading-framing list (mixed baselines, regional vs Montana, multiple drivers, HB 136 permissive).
2. expert: same verdict and similar sources, and added GAO context. Fewer claims graded; marked HB 136 "could not find", which everyman found. Rural 11% not found.
3. askamerica: no article text, so it graded 4 snippet-level claims, labelled the central claim unsupported on thin evidence, and gave a grade that conflicts with its own engine report. It was honest about the access limit and did not invent a copy.

What would change the ranking: if the article's 40% is shown to be unsourced anywhere, askamerica's central verdict would move up and everyman's "roughly consistent" would be too generous.

## Severity invariant

SEVERITY DEFECT: askamerica at or below everyman. Concrete cause: the engine's own web_fetch returned HTTP 403 on the article (govdata-ops #1471) while everyman's WebFetch returned a summary. The copy gate then forced a different, unrelated NBC Montana URL into article_copy_url. This is a harness/product asymmetry in access, not askamerica's method. Its method (search the APCIA deck, search the catalog, state the limit) was sound; the access gap explains the thin claim list.

## Recipe check

- Existing recipe: `fact-check-a-news-article` was served (calls.jsonl seq 1). It covers claim extraction; it cannot help when the article cannot be fetched.
- Related: `unfetchable-source-is-unverifiable` and `known-official-source-403-try-wayback-before-giving-up`. The Wayback retry applies to a known source; askamerica tried the `?amp` form (seq 4) but not a Wayback snapshot of the ICN page as far as the call log shows.
- Everyman and expert reached the NAIC report, HB 533 and HB 136 through plain web search once they had claim text. A warehouse-native method is not relevant here; the warehouse has no state homeowners premium table (search_catalog found none).
- Asymmetry judgment: the gap is explained by the 403 on the engine's fetch tool, not by askamerica's method. With the article text, the same claim-by-claim path would likely have matched the others.
- Cause class: (c) tool guardrail missing at the point of use. The fetch tool gives no fallback route to the article text, and the copy gate demands a URL that is a copy, which pushed a wrong one.

Proposed resolution: c — let the engine web_fetch retry through an archive snapshot or fall back to summary-style fetch when it gets a 403, and let preview_report accept "no copy" without naming an unrelated page.

## Corpus gaps stated

- "search_catalog found no state homeowners-insurance premium series, no regulator complaint series and no nonrenewal data. It holds only FEMA/NFIP flood, ACA health and crop-insurance tables." (agent.md)
- Claim 2 reason: "No complaint-count data in warehouse." (claims.json)
- Overall: "The article text could not be read (HTTP 403, no loadable copy)" (access limit, not a warehouse gap).
