## Fact-check verdict: Roll Call, "White House cancels health, education funds approved by Congress" (Sept 28, 2026)

**Overall rating (SPLIT shape — required because the article quotes named officials making their own attributed legal assertions): Fidelity 2 (of 4) / Claims Accuracy 1 (of 4)** — not a single combined score. See rationale below.

Full report (local, dies with this session): http://127.0.0.1:55497/a/2fc2acab3f4416ed52dc542c6e70e550.html
Also saved to: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q1/askamerica/2026-09-29/report.html`

### Why SPLIT, not SINGLE
The piece quotes named officials (Sen. Collins, Rep. DeLauro) making their own legal assertions ("unlawful," "illegal cancellation"). Per the mandatory rating rule, any graded claim attributed to a named third party requires separating:
- **Fidelity** — did Roll Call accurately convey the facts, figures, and quotes (its own reporting)?
- **Claims accuracy** — are the underlying assertions the quoted officials/administration made themselves actually true?

### AskAmerica warehouse coverage
`search_catalog` was queried (twice) for "pocket rescission," "impoundment," "OMB apportionment," and "Office of Refugee Resettlement" — **no table in the corpus covers any of this**. Confirmed sourcing gap; every claim below was checked against primary sources fetched directly (White House fact sheet, NPR's coverage of the SCOTUS order, CNBC, House Appropriations Democrats' own press release, and the Roll Call article itself).

### Fidelity rating: 2 of 4 — "significant omissions or exaggerations"
Roll Call quoted Collins and DeLauro verbatim and accurately, and its $810M topline figure is correct. But:
- **The piece's own unattributed narrative claim** that the 2025 Supreme Court case "ultimately ruled that the administration had the right to cancel the money" **materially mischaracterizes a preliminary stay as a final merits ruling** — this is the article's most significant reporting problem (see Claim 4 below).
- **The $746M-of-$810M breakdown is incomplete**: the article lists 5 of the White House's 11 line items, silently omitting 6 smaller items totaling $64M, without flagging the list isn't exhaustive.

Neither rises to a fabricated figure or an invented quote, but together they're a real, significant shortfall in the piece's own reporting fidelity — hence count 2, not 0 or 1.

### Claims-accuracy rating: 1 of 4 — "some shading, a defensible-but-flattering framing"
This grades whether Collins's and DeLauro's own quoted assertions ("unlawful cancellation," "illegal actions," "unlawfully impounded funds") are themselves true, separate from whether the article quoted them correctly (it did). Their legal conclusion has real backing — GAO has opined pocket rescissions are not permitted under the Impoundment Control Act, and DeLauro authored an amicus brief making this argument in the 2025 case — but it is **not an adjudicated fact**: no court has issued a final merits ruling on pocket-rescission legality. Stating "unlawful"/"illegal" as settled fact overstates the law's actual, unresolved status, though it's a defensible position, not a fabrication — hence count 1.

### Per-claim verdicts (as published)
| Claim | Fidelity | Claims accuracy |
|---|---|---|
| Article's own assertion: SCOTUS "ultimately ruled" admin had the right to cancel the money | **Mostly false** — was a 6-3 emergency-docket stay of a preliminary injunction, not a merits ruling | N/A — this is the reporter's own unattributed claim, not an attributed quote |
| $810M total / 5-item breakdown summing to $746M | **Partially true** — total is correct; breakdown omits 6 of 11 items ($64M) | N/A — reporter's own figure |
| Collins quote ("unlawful cancellation," "illegal actions") | **True** — verbatim match to her own statement | **Partially true** — GAO-backed but judicially unresolved legal conclusion |
| DeLauro quote ("illegal," "unlawfully impounded funds all year") | **True** — verbatim match to House Approps Dems press release | **Partially true** — same contested/unresolved legal status |

### What actually happened with the Supreme Court (the core fact-check)
Sept. 26, 2025: the Supreme Court voted **6-3 on the emergency ("shadow") docket** to **stay** a district court's preliminary injunction requiring the administration to obligate ~$4B in foreign aid — not a merits ruling. Justice Kagan's dissent noted the majority decided this "with scant briefing, no oral argument, and no opportunity to deliberate in conference." The majority found only that the administration made a sufficient *showing* that the Impoundment Control Act likely precludes this kind of APA claim — a preliminary finding for the stay, not a final adjudication. GAO's own opinion holds pocket rescissions are unlawful, and the same legal dispute is being relitigated in this very 2026 story.

### Methodology note
Each graded claim was independently scored via the `score_claim` tool (Jev) before publishing; where its confidence was low or its verdict label didn't map cleanly onto the report's formal verdict scale, an explicit override reason was documented, grounded in the primary source directly fetched (all in the published report's claims table).
