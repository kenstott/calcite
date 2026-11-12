# Judge: CBS Face the Nation, Nov. 8, 2026 transcript fact-check (q5), 3-persona 2026-11-09

Rank: [askamerica, expert, everyman]

Source: https://www.cbsnews.com/news/full-transcript-face-the-nation-with-margaret-brennan-11-08-2026/

**Engine build:** `~/.askamerica/engine/askamerica-engine.jar`, from ee71f1f87d (office-page and window
gates). **Data server:** the patched local overlay on port 45533 with the version-2 catalog cache.

## Judge's own verification

Re-run live through the engine:
- **Lobbying:** LDA quarterly income for CONTINENTAL STRATEGY, LLC is $570,000 (2023), $196,000 (2024)
  and $25,270,936 (2025). BALLARD PARTNERS is $79.92M in 2025. These match `askamerica`.
- **House table:** `officials.house_election_results` spans 1976 to 2022, as `askamerica` said.

Not re-run: the CPI figures. 21.4% from January 2021 to January 2025 and a 9.1% peak in June 2022 fit the
published BLS series as I know it.

**The Continental verdict is too strong.** Means said "their revenue didn't go from zero dollars to $30
million a year". LDA income covers federal lobbying receipts only, not a firm's total revenue, so $25.3M
cannot contradict $30M. "Zero" is contradicted only weakly ($196,000 is close to zero for a firm of this
scale). `askamerica` noted "lobbying receipts only" but still graded it partially false; "mostly true on
the LDA evidence, total revenue unknown" fits. The same measure-mismatch weakness applies to the
"highest revenue" Ballard grade, though there it graded in the claim's favour.

## Why

**`askamerica` is first.** It alone tested the guests' numbers against primary records: Hill's 20% against
CPI, and Means' claims about Continental and Ballard against federal lobbying filings. It read the CBS
page, disclosed its `income IS NOT NULL` exclusion and its name match, and listed what it left undone.
Weak spots: four claims marked not checkable with little outside effort, two of which `expert` confirmed
by ordinary search (the roughly 4,000-vote margin and CENTCOM's $20 million reward), and the Continental
grade above.

**`expert` is second.** It covered more ground by search: the vote margin, the CENTCOM reward, the housing
law's actual scope, and the Forbes figure being a gain. It queried no primary dataset, read only a summary
of the page, and did not test Means' revenue numbers. It also attacks Hill for a "20 percent peak" he did
not claim; he spoke of 20 percent inflation, and its own reading calls that cumulative. Its dashboard has
stat tiles only. Its 2 Pinocchios rest partly on that misreading.

**`everyman` is third.** It confirmed the events but tested none of the guests' statistics.

## Severity invariant

`askamerica` ranks above `everyman`. Not triggered.

## Gate check

Neither gate refused. The window gate's wording test did not match the Continental claim, but `askamerica`
listed two windows anyway, following the tool description. No claim was graded 'unsupported'.

## Recipe check

`askamerica` is #1. One watch item, first instance: a measure narrower than the claim (federal lobbying
income against "revenue") graded contrary. House results stop at 2022: filed as govdata-ops #1143.
