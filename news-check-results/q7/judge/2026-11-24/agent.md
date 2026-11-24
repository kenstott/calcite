# Judge — News q7 (bridgemi-michigan-mega-farms-cafo-growth), 2026-11-24

**Question/claim:** Fact-check Bridge Michigan, "Mega farms are booming in Michigan, drowning rivers in manure" (2026-11-19): 272 regulated CAFOs, Gratiot County 24 CAFOs with cattle more than doubled and hogs tripled since 2000, 1.1M solid tons and 3.93B liquid gallons of waste, about three-quarters of rivers polluted.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of the everyman and expert answers for "askamerica" found no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web search only (CRC, ELPC, Michigan Public, New Lede, NASS snippets) | Warehouse: ag.nass_livestock_inventory, environment.attains_state_use_summary; plus EPA, New Lede, court appendix | Web only (Michigan Advance, Justia, ELPC, CRC, New Lede) |
| Vintage/window | NASS 1997/2017 snippets; 2000 and 2022 not found | NASS 2000 to 2025 cattle, 2000 to 2022 hogs; ATTAINS 2026 cycle | none for livestock or rivers |
| Headline figures | 272 vs 271 to 290 CAFOs; Gratiot 24 vs 27 (2020) | Cattle 24,500 to 60,000 (2.4x); hogs 33,500 to ~82,000 (2.4x); rivers 28,104 mi not supporting vs 4,714 supporting | 272 within 271-291 |
| Pinocchio grade | 1 | 1 | 1 |
| Claims graded | 11 (table) | 12 (claims.json) | about 5 verdicts, 9 items listed not checkable |
| Central claim verdict (three-quarters of rivers polluted) | Not verified | True / consistent (ATTAINS, all causes) | Not checkable here |
| Visual | none | report.html + dashboard.png | agent-report.html + dashboard |

## Verification

- Central claim. The article says about three-quarters of rivers exceed standards for waste. Bridge's companion story and Michigan Public (2026-11-19) attribute roughly 75% to an EGLE E. coli testing lead as an estimate from about half the rivers tested. EGLE's own E. coli page says roughly half exceed the TBC standard (undated). I could not fetch the Integrated Report table. The claim is a reported agency estimate, not a measured figure. askamerica's ATTAINS check (86% of assessed miles not supporting, 39% of all miles) is in the same direction and its "consistent, all causes" wording is calibrated. everyman and expert left it unchecked, so neither is contradicted or supported by it.
- Overall rating. Three arms all say 1 Pinocchio. This fits: the figures that can be checked are close, and the causal framing is one-sided but disclosed.
- Hogs "tripled". everyman could not find 2000 data and only said direction is plausible (1997 to 2017, about 2.4x). askamerica found 33,500 (Dec 2000) and about 82,000 (2022 Census) from NASS, 2.4x, and graded "partially false". The 2022 figure is a sum of fragmented rows. Open item for the orchestrator: a live query of ag.nass_livestock_inventory for county_fips 26057 HOGS to confirm 33,500 and the 2022 sum.
- Cattle. askamerica 24,500 to 60,000 (2.4x) supports "more than doubled". everyman gave only 2017 (54,333) and flagged a 2022 snippet as inconsistent. Not checkable by me except by live query.
- 272 CAFOs. everyman and expert both found 271-291 from outside sources and graded it consistent. askamerica graded it unsupported and could not find a source (web_search unavailable). Here the unaided arms did better.
- 2024 Supreme Court ruling and June 2026 appeal. expert and everyman corroborated via Michigan Advance, Justia and an ELPC filing. askamerica graded it unsupported because web_search was down. Gap in askamerica's evidence.
- Claims only askamerica caught: hen threshold 82,000 not 80,000 (EPA), 24 vs 25 Gratiot CAFOs inside the article, the "Eleven years" vs "six years" mismatch, "up to 34 million" omitting the 16 million lower bound, the Farm Bureau "economically useless wilderness" quote found in the court appendix, the E. coli standards resting on Bridge's own companion story.
- Claims everyman caught that askamerica did not: Gratiot 27 CAFOs in a 2020 Michigan Public piece, Gratiot second in land-application acreage.
- No arm graded a claim true or false without evidence. Calibration: askamerica labeled 7 items unsupported with reasons; everyman and expert used "not checkable here" for similar items.
- askamerica engine rating vs agent.md: report.html says "1 Pinocchio"; agent.md says "Rating: 1 Pinocchio". No mismatch. Minor: agent.md says "5 claims could not be confirmed" but claims.json marks 7 as unsupported and agent.md lists 7.

## Ranking rationale

1. askamerica. Only arm that checked the central claim and both livestock claims against primary agency data, and it found a real inaccuracy (hog multiple) plus several internal inconsistencies. Verdicts are calibrated. Weaknesses: no web search, so it missed the court history and the 272 count that the others confirmed.
2. everyman. Broad claim table, useful outside corroboration for the CAFO count, litigation and the New Lede approval. It did not reach the 2000 NASS data or the central claim, so two headline claims stayed unverified. Limited to a summary of the article.
3. expert. Correct overall grade and good sources for the litigation and count, but the thinnest coverage: no livestock or river checks, nine items listed as not checkable in a few lines, and only an article summary.

What would change the ranking: if a live query shows the NASS hog or ATTAINS numbers are wrong or from the wrong table, askamerica would fall to or below everyman. If the 2000 Gratiot hog figure is actually about one-third of the current count, the "partially false" verdict reverses.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- Existing recipe `fact-check-a-news-article` covers extracting every claim and testing at the article's own grain. askamerica followed it (caught internal caption mismatch, hog window test).
- everyman and expert found the court history and CAFO count with web search. askamerica had no web_search this session, so that is an environment gap, not a recipe gap.
- A possible addition to that recipe: for an agency-estimate claim such as "three-quarters of rivers", also state that the figure is an EGLE estimate from partial testing, using environment.attains_state_use_summary only as context. Minor.

Proposed resolution: d — askamerica ranked first; the only misses came from web_search being unavailable, so no recipe or tool change is needed.

## Corpus gaps stated

- "No warehouse table covers these (search_catalog returned nothing for CAFO permits, manure volumes or E. coli sampling)": 272 CAFOs, 1.1M tons, 3.93B gallons, "more waste than the human population"; 420 million gallons in Gratiot; 10x E. coli (MDPI page returned no text); state E. coli standards; 2024 Supreme Court ruling and June 2026 appeal; $300/hour hauling cost; Borrello's "dwarfs other inputs" causal claim.
- "search_catalog has no manure hauling cost table."
- "No warehouse table holds CAFO manure volumes (search_catalog returned none)."
- "web_search unavailable in this session" (stated as the reason no independent EGLE records or court opinions were found).

## Orchestrator verification (2026-11-24)

A live query of `ag.nass_livestock_inventory` (Gratiot, county 26057, hogs, inventory, head, census) returns domain TOTAL rows of 31,258 in 2002 and 41,033 in 2022, an increase of about 31%. The 2000 December survey value is 33,500. askamerica's "about 2.4 times" rests on a 2022 sum near 82,000. That sum adds the TOTAL row to its own size-class rows (for example the 1,000-or-more-head bucket, 40,900), so it counts the same hogs twice. The engine flagged the first query `rollup_contamination:high`; the run excluded rollups for cattle but not for hogs. The verdict "hogs tripled is not supported" still holds, and the real gap to the claim is larger than askamerica reported. The ranking does not change: neither unaided arm checked the hog figure.
