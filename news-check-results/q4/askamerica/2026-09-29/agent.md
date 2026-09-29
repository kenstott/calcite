# Fact-check verdict: TRUE / Pinocchios 0-0 (SPLIT: fidelity 0, claims_accuracy 0)

**Report saved to:** `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-09-29/report.html`

## Bottom line
Every figure in the claim checks out against primary sources, verified by direct fetch of the actual documents (not secondary summaries):

### 2024 rule figures — exact match to EPA's own fact sheet
(`epa.gov/system/files/documents/2024-04/cps-111-fact-sheet-standards-and-ria-2024.pdf`)
- "1,200 avoided premature deaths... 360,000 avoided cases of asthma symptoms" in 2035 alone — verbatim match.
- "1.38 billion metric tons total of CO2 avoided from 2028-2047 systemwide" — verbatim match.
- "328 million gas cars" equivalence: 1.38B tons / 328M cars = 4.21 t/car/yr, within ~2% of EPA's own current Greenhouse Gas Equivalencies Calculator factor (4.29 t/car/yr, per the methodology page).

### EDF's new repeal-impact analysis — exact match to EDF's own press release
(`edf.org/media/edf-analysis-repeal-power-plant-pollution-standards-will-cause-84000-early-deaths-3-trillion`)
- "up to 84,000 premature deaths" and "7.5 billion metric tons... equivalent to the annual climate pollution from 1.6 billion cars" by 2047 — both verbatim.
- **Key nuance confirmed**: these are EDF's OWN modeling results, not EPA's own published numbers. EDF states explicitly it "estimated the health and climate harms based on EPA's own tools and methodologies, including the emission increase projections in the Regulatory Impact Analysis, using a 2% discount rate" — because the Trump EPA's repeal RIA itself refused to monetize these benefits/harms. The claim's attribution ("EDF's new analysis... estimates") is accurate on this point.
- Car-equivalence implied factor (4.69 t/car/yr) runs ~9% above EPA's current factor — plausible variance (CO2e vs CO2-only scope), not fabrication.

### Lawsuit facts — confirmed via EDF's own primary press release
(party to the suit, datelined Sept 17, 2026): filing date, D.C. Circuit venue, and petitioners (NRDC, American Lung Association, EDF, Clean Air Council, plus American Public Health Association and Clean Wisconsin under "and others") all match. A Justia search result independently corroborates a matching case caption/number (26-1238), though the docket page itself returned HTTP 403 and couldn't be read directly — noted as a limitation (that sub-claim graded "mostly true" rather than "true" for this reason). The "unlawful" framing is correctly presented as the groups' own legal allegation in an unresolved case, not adjudicated fact.

## AskAmerica warehouse coverage
Ran `search_catalog` first as required. The corpus has no table for EPA regulatory-impact-analysis projections, litigation/docket data, or asthma outcomes (`search_catalog` flagged "asthma" as unmatched across the entire catalog). This is a genuine, expected sourcing gap — not a failed query — so the check relied entirely on primary documents fetched directly from epa.gov and edf.org.

## Process notes
- `score_claim` (Jev) returned low-confidence verdicts on 3 of 4 claims because they concern events/documents from September 2026, beyond any plausible model training cutoff. Explicit `score_claim_override_reason` was supplied on each, citing the specific primary-source documents independently fetched and verified in this session, per the tool's own override guidance.
- `find_recipe` was called (no directly applicable recipe for this single-claim primary-source verification task — a valid empty result, not skipped).
- Dashboard (2 stat tiles + 2 bar charts comparing the 2024 rule's benefits to the repeal's projected harms) is inlined in the published report at the path above.

Full report with all sections, sourcing, tool citations, and the Pinocchios banner (fidelity: 0, claims_accuracy: 0, both "no significant issues") is in the HTML file at the saved path.
