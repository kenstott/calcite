# Judge — news q7 gazette-colorado-springs-utilities-snowpack-22-percent-warmest-winter

**Story:** "Guest opinion: 2026 historic drought and the value of being prepared" (The Gazette, 2026-11-17), by Travas Deal, CEO of Colorado Springs Utilities. Claims of interest: warmest winter on record; snowpack 22% of normal; 40% of typical yield; "since 2000, total water use has been reduced by nearly half, even as our city grew by 42%"; three years of demand in storage; ~70% of supply from the Colorado River Basin.

Rank: [expert, everyman, askamerica]

Judged 2026-11-18 on the three answers first produced the same day (askamerica's first run, preserved before the fix). The post-fix rerun is reported separately below and does not change this ranking.

## Why
- **expert (1st):** 2 Pinocchios. Found the op-ed's headline conservation claim overstated: total deliveries fell about 20–29% (Colorado Sun 2000–2025, Maven's Notebook) while per-capita use fell 44%, which is likely where "half" comes from. Added that the 22% is an April 1 reading against a seasonal peak of 51%, that storage fell to ~2.3 years by October (lowest since 2012–14), and that the 70% Colorado River share counts reuse (raw ~50%).
- **everyman (2nd):** 2 Pinocchios. Caught the same central overstatement (per-capita −39–40%, total deliveries ~−25%) and qualified "worst in modern history" (SNOTEL records from ~1987; precipitation only 15th-driest winter). Seven tool calls, search snippets only, but it got the claim that matters right.
- **askamerica (3rd, first run):** engine 0 Pinocchios. Useful warehouse work: ASOS Colorado Springs airport YTD precipitation (7.63 in vs ~15.3 in 2010–2025 mean, with a caveat about ~56 missing days) and El Paso County Census growth (+41.3% 2000–2020). But it graded the compound "total use down nearly half while the city grew 42%" claim **mostly true** on the growth half alone, while its own report said the total-use half was "not in the warehouse and not independently verified". That is the piece's central misleading claim, and it let it pass.

What would change the ranking: none of the plausible readings rescue the first askamerica run. Its warehouse precipitation and county figures are real additions, but missing the one materially misleading claim that both unaided arms caught is decisive.

## SEVERITY: askamerica ranked below everyman
askamerica is everyman plus the connector, so this is a product defect. Mechanism: hammer-and-nail. The checkable half of a compound assertion (population growth, in the warehouse) set the verdict for the uncheckable half (total water use, not in the warehouse), and the instructions had no rule against this.

## Resolution (exactly one): McpServer.java instruction change
Added to the VALIDATING instructions' step 6 (after the attack-splitting sentence): an assertion joining two or more figures MUST be split into one claim per figure, each graded on its own evidence, and the verifiable figure MUST NOT carry a verdict for the one that could not be verified. A total and a per-person rate are different figures, and a matching per-capita figure is evidence that a "total" claim was overstated. Rebuilt `:askamerica-engine:shadowJar`, verified the literal in McpServer.class (`grep -a -c` = 1), ran smoke_test.py (OK), backed up to `askamerica-engine.jar.bak-2026-11-18-compound-claim-split`, and installed atomically (`mv`) to `~/.askamerica/engine-dev/`.

**Rerun to confirm (Step 6.3):** I re-dispatched askamerica on the same URL against the new jar (the first run is preserved at `~/.askamerica-daily/q7-askamerica-prefix-2026-11-18/`). The behaviour changed on the targeted point. The claim is now **mostly false**: KUNC, quoting the utility's own conservation manager, gives total deliveries −25% and per-capita −39% for 2000–2023, and "nearly half fits neither total nor per-capita". The engine rating moved to 2 Pinocchios. Partial: the compound claim was still published as one row rather than split into two, so the "split" half of the rule did not take, while the total-vs-per-capita half did. The rerun also cites a 2000 Census count "from memory, not fetched", which is a weakness. Judged blind against the same expert/everyman answers, the rerun would rank [expert, askamerica, everyman]: it matches everyman on the central claim and adds warehouse drought-monitor and gazetteer evidence. The answer now on disk, and published, is the rerun.

## Recipe check
Covered by the instruction change above. No separate recipe.

## Gaps
Filed kenstott/govdata-ops#1323 (`type:sourcing`, `kind:gap`, `schema:weather`, `status:open`): NRCS SNOTEL snow water equivalent / % of median. Both askamerica runs reported no snowpack table, and `grep` over every schema YAML confirms it; no prior issue.

## Shape
Both runs: every claim is the op-ed author's own (Travas Deal). The first run grouped them as `subject_claims` with speaker Deal; the rerun used `author_claims`. Either is consistent, since the author is the named official. No resume needed.
