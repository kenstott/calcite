## Fact-check: KATU, "Oregon nears all-time wildfire acreage record as 2026 fires top 2 million acres"

**Overall verdict (split Pinocchio scale, per WaPo methodology for attributed-claim pieces): Fidelity 0 Pinocchios; Claims Accuracy 1 Pinocchio.**

This article attributes nearly every statistic by name to a specific official — Carol Connolly (NWCC) for the acreage figures, Jessica Neujahr (ODF) for the fire-behavior and fuels commentary — which is exactly the case the split {fidelity, claims_accuracy} rating exists for, rather than a single blended score.

- **Fidelity (0 Pinocchios):** The piece represents what Connolly and Neujahr actually said accurately and in context throughout; nothing is misquoted, exaggerated, or taken out of context. The one notable sourcing gap — the 52%→56% human-caused statistic is given no named source, unlike every other figure in the piece — is itself a fidelity-relevant observation (a break from the article's own attribution discipline) but not a misrepresentation of anyone's actual words, so it doesn't independently earn a fidelity Pinocchio.
- **Claims Accuracy (1 Pinocchio):** Every attributed statement from Connolly and Neujahr holds up against AskAmerica's own NIFC wildfire perimeter data and independent reporting. The one claim that does not hold up is that unattributed 52%→56% human-caused figure — AskAmerica's own cause-coded data puts it at ~37–47%, and an independent Aug 29, 2026 Willamette Week/Oregon Journalism Project analysis of the same NIFC data reports 45%. That's a real, checkable numeric error inside an otherwise accurate piece, and it's deployed to support the article's "Oregonians letting their guard down" framing.

Report saved to `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q2/askamerica/2026-09-28/report.html` (dashboard inlined, 10 sections). This deliver_report call regenerates `agent.md` in the same directory so it matches the corrected split rating now live in report.html.

### What checked out (claims_accuracy = true/mostly true)
- **2024 record, 2,081,661 acres** (Connolly/NWCC) — AskAmerica's `disasters.wildfire_perimeters` sums Oregon 2024 wildfire perimeters to ~2,010,781 acres, within ~3%; corroborated by 3 independent outlets.
- **"60 large fires, 2,014,481 acres" as of Aug 3, 2026** (Connolly/NWCC) — can't be reconstructed to the exact acre from AskAmerica's current-snapshot table, but named-fire growth trajectories from independent, dated Central Oregon Fire Information updates bracket the figure precisely, and the season's later total (2.3–2.5M acres) confirms the trajectory.
- **Five largest fires and containment percentages** — Big Grass, Coleman Creek, Second Flat directly confirmed in AskAmerica's data; Rowe Creek Complex confirmed via Central Oregon Fire Information's own incident updates, whose growth curve brackets the article's Aug 3 figure almost exactly.
- **Red Flag Warning, southern Willamette Valley, Aug 3** — confirmed via NWS/local reporting.
- **Spokane fires as urban-fire reminder** — confirmed: Old Trails/Autumn Lane/Fairview fires burned into Spokane Aug 1, 2026, ~700–850 structures destroyed, ~65–67k evacuated.

### What did not check out (claims_accuracy = false)
- **"52% of all Oregon wildfires were human-caused in 2025; 56% in 2026"** — the one unattributed claim in the piece. AskAmerica's cause-coded WFIGS data: 2025 ≈44.5% human-caused (all fires)/46.7% (determined-cause only); 2026 season-to-date ≈37% (all)/46.8% (determined-cause). Independent Willamette Week/Oregon Journalism Project analysis (Aug 29, 2026, same NIFC data): 45% for determined-cause fires. **False as stated.**

### Materially misleading beyond the individual facts
The unsupported human-caused statistic is placed right after paragraphs establishing that most of the season's largest fires were *not* human-caused, creating an implicit "but the human-caused share is rising" contrast the data doesn't support — lending false precision to the "letting their guard down" narrative. Everything else, including the article's central and subsequently-confirmed prediction that 2026 would break the 2024 record, holds up well.

### Method notes
- Data source: AskAmerica `disasters.wildfire_perimeters` (NIFC WFIGS) — a current snapshot with no year-partitioned history, so exact Aug 3, 2026 point-in-time figures could not be reconstructed to the acre; worked around via named-fire growth trajectories from independent, dated incident-command sources (Central Oregon Fire Information).
- Cross-checked against NPR/CNN/Yale Climate Connections/NBC News (Spokane fires) and Willamette Week/Oregon Journalism Project (human-caused percentage, sourced to the same NIFC data).
- `find_recipe` was consulted (twice, across the original and corrected passes); no recipe covered the specific snapshot-table point-in-time reconstruction issue, but general exclusion/coverage-gap recipes informed the approach.
