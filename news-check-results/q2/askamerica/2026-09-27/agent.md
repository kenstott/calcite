## Fact-check: "Canceled Student Loans Are Still Showing Up as Debt, Lawsuit Says" (AOL/Investopedia, Sept 24, 2026)

**Verdict: Fidelity 0 Pinocchios / Claims Accuracy 1 Pinocchio** (split rating, as required, because the headline claim is attributed to a named third party — PPSL — not asserted by the article itself).

### Why split, not blended
The article's headline claim ($4.6 billion in canceled debt still being reported as overdue, affecting 300,000+ borrowers) is explicitly PPSL's own estimate ("advocates say," "according to PPSL estimates"), not the reporter's independent assertion. WaPo-style Pinocchio grading requires separating **fidelity** (did the article accurately relay what PPSL said) from **claims_accuracy** (is what PPSL said actually true) whenever a claim carries this kind of attribution — collapsing the two into one blended count (as an earlier run today did) hides that these are different questions with different answers here.

### Findings

**Fidelity: 0 Pinocchios.** Every number and quote traceable to PPSL in the article matches PPSL's own Sept 24, 2026 press release (fetched directly from ppsl.org) word-for-word or in close paraphrase — the $4.6B/300,000+ figure, the $23.4B/1.5 million group-discharge history (April 2022–January 2025), the "relief was automatic" promise, the two named plaintiffs (Mandy Woods, Ashford University; Jorge Cortes, ITT Technical Institute), and the D.C. federal court filing under the Fair Credit Reporting Act. No exaggeration or dropped hedge found.

**Claims accuracy: 1 Pinocchio.** The headline $4.6B/300,000+ figure is PPSL's own novel, unaudited litigation estimate "based on public data" — PPSL does not show its work, and no Department of Education publication or AskAmerica table confirms it. It is an allegation in unresolved litigation (the case was filed the same day the press release went out), not an established fact. By contrast, the $23.4B/1.5 million-borrower group-discharge figure **checks out**: it's corroborated in scale by a Dec 4, 2024 congressional letter (Sen. Markey et al., fetched from markey.senate.gov) itemizing the Department's own group-discharge press releases (Corinthian $5.8B/560,000; ITT Tech $3.9B/208,000; Art Institutes $6.1B/317,000; Westwood $1.5B/79,000; and others), totaling over 1.2 million borrowers by Oct 2024 — consistent with reaching 1.5M/$23.4B by Jan 2025.

**AskAmerica coverage gap (confirmed, not just unchecked):** search_catalog was queried for student loan forgiveness/borrower-defense/discharge and for federal student-loan balance/delinquency data. No table matched in either case (unmatched terms: forgiveness, discharge, defense, delinquency) — this warehouse carries no table on federal student-loan discharge programs or on post-discharge credit-bureau reporting. Per the engine's own recipe guidance, I then went straight to the primary sources (PPSL's release, the Senate letter) rather than settling for secondary synthesis.

**Materially misleading element flagged:** the article's KEY TAKEAWAYS box states the $4.6B/300,000+ figure as the lede fact with only a trailing "advocates say" — a skimming reader could easily absorb an unaudited plaintiff estimate as a settled fact. This doesn't make the article inaccurate (the attribution is technically present throughout the body text), but it's the one thing worth flagging even though every individual sentence checks out as written.

### Report
Published report (local, this-session link, 5 sections, dashboard, 5 sources): http://127.0.0.1:64928/a/0372a06c23aaf1c3f1ae7d9b725b5498.html
Saved copy: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q2/askamerica/2026-09-27/report.html`

Two claims were independently scored via `score_claim` before publishing: the $23.4B/1.5M discharge figure → "true" (0 Pinocchios, confidence 0.67); the $4.6B/300,000+ PPSL estimate → "not checkable here" by design.
