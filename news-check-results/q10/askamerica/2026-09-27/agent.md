# Fact-check: CapitolExposed "120 trades disclosed in the week to September 7, 2026"

**URL checked:** https://www.capitolexposed.com/news/weekly-roundup-2026-w37
**Report saved to:** /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q10/askamerica/2026-09-27/report.html
**Local report link (dies with this session):** http://127.0.0.1:51233/a/f99ceb9b52e01b4762e01c84f675c01b.html

## Verdict: 1 Pinocchio (of 4)

Every individually checkable fact in the piece holds up. It is factually sound but incomplete in a way that gives readers a rosier picture of STOCK Act compliance than the record supports.

## What was checked and how

AskAmerica's government-data corpus has **no table covering STOCK Act periodic transaction reports** (congressional stock trade disclosures) — confirmed via two `search_catalog` passes ("congressional stock trading disclosure STOCK Act" and "member of congress financial disclosure trades periodic transaction report"), both returning no matching table. This is a genuine coverage gap, not a missed search.

What the corpus *could* verify, and verified exactly right: member identity. `officials.current_members` (Congress.gov data) confirmed all 10 named members' party, state, chamber, and district precisely as reported — including the less-obvious ones (Cleo Fields D-LA-6, David J. Taylor R-OH-2, John J. McGuire III R-VA-5).

For trade-level facts, verification relied on independent primary/secondary sources fetched directly:
- A House Clerk Periodic Transaction Report PDF (Steve Cohen, filing #20034796)
- Investing.com's House-Clerk-sourced trade summaries
- NOTUS reporting (Dave Levinthal, Sept 8, 2026) on STOCK Act compliance for this exact period

## Findings

| Claim | Result |
|---|---|
| 120 total trades (51 purchases, 66 sales, 3 exchanges) by 10 members | Internally consistent arithmetic; full re-tally not possible (data not in AskAmerica corpus, would require scraping the full House Clerk/Senate eFD database for the week) |
| Cisneros filed the most (66), Hern second (29) | Member identities/party/state exact; independent reporting corroborates both as unusually high-volume filers this week, though exact counts weren't independently re-derived |
| Steve Cohen's Treasury-bill purchase, "up to $500.0K," Aug 21 2026 | Strongly corroborated — instrument, rate, maturity date, transaction date, and $250,001–$500,000 band all match an independent House-Clerk-sourced account exactly. The article's own caveat (bands, not exact figures) is verified true against an actual PTR PDF's format. |
| "No trade cleared the 30% conflict-alert threshold" | Not checkable — this is CapitolExposed's own proprietary scoring output, not derived from a disclosed public formula |
| "About this window" framing disclosure lag as routine | **Materially incomplete.** NOTUS reported, one day after this window closed, that Cisneros was "days or weeks late" on more than 50 trades and that Hern "was also late, although his office denies it" — the two filers this very article credits with the most trades. The roundup never mentions either being flagged for non-compliance. |

Note: two well-evidenced findings (the Cohen trade and the STOCK-Act-timing omission) are formally graded "not checkable here" in the report's claims table rather than "true," because an independent second-opinion scoring pass (`score_claim`) on each returned only moderate confidence (0.15–0.59) despite agreeing with the underlying assessment on every retry. The full supporting evidence for both is laid out in the published report and is not weakened by that formal grading choice.

## What's materially misleading despite the facts checking out

The article names Gilbert Cisneros and Kevin Hern as the week's two most active filers, then closes with a methodology note that frames the ~4-week median filing lag and 45-day STOCK Act deadline as routine ("a filing pattern rather than a quiet market"). Independent reporting published the very next day (NOTUS, Sept 8, 2026) found that Cisneros and Hern — specifically — had trades that were late or under compliance scrutiny under that same 45-day rule. A reader relying only on this roundup would have no way to know its two headline names were, per outside reporting, live disclosure-compliance stories. This is an editorial completeness gap, not a factual error — which is why the rating is 1 Pinocchio rather than 2.

## Sources cited in the report
- Congress.gov (via AskAmerica `officials.current_members`)
- House Clerk PTR filing #20034796 (disclosures-clerk.house.gov)
- Investing.com, "Steve Cohen makes significant purchase of US Treasury Bills"
- NOTUS, "Mega Millions-Winning Congressman Violates the STOCK Act" (Sept 8, 2026)
