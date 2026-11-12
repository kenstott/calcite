# q9 judge: RISMedia "Pent-up demand could be released if supply, affordability, confidence improve"

Rank: [askamerica, expert, everyman]

Story ran 2026-11-09, relaying NAR chief economist Lawrence Yun's outlook (NAR NXT forum, New Orleans) and NAR's 2026 Profile of Home Buyers and Sellers. Judged 2026-11-12. I read the article via WebFetch and a search for the NAR newsroom release; the aggregate figures (population, jobs, wealth, 2000 sales, sentiment) appear in the article as narration of Yun's comparisons to 2000, not as direct quotes. I did not re-derive FRED/Census/Z.1 series; I relied on the arms' consistent findings (341.8M Census Vintage 2025, 343.1M FRED POPTHM, PAYEMS 159.3M, Z.1 equity $35.8T) and independently confirmed the 2000 sales figure (HUD/NAR: 5.03M first print, 5.11M revised; 5.20M was 1999).

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 0 | Warehouse (FRED PAYEMS, UMCSENT, EXHOSLUSM495S; census.pep_components_of_change), NAR release, NAHB, Z.1 via NAHB |
| expert | 1 | FRED POPTHM, PAYEMS, UMCSENT, OEHRENWBSHNO, HOOREVLMHMV, NAR Sep release, NAR Profile |
| everyman | 1 | Search summaries only; page not opened directly; 4 claims left unchecked |

## Assessment
- **Correct verdict: 0 Pinocchios.** The outlet relays Yun's and NAR's remarks faithfully. The Profile figures (22%, age 40, 12 years, 90%/92% agent use) match NAR. Months' supply (4.8, highest since Nov 2015) and 33 days match NAR's September report. The only inaccuracies are the speaker's rounded comparisons, none of them the outlet's own reporting, and none large enough to mislead.
  - **Population 281M to 350M:** 281M for 2000 is right. Today's figure is about 342-343M, so 350M overstates by about 2%. A Yun rounding, relayed as his.
  - **Payroll jobs 132M to 159M:** true (PAYEMS about 132M mid-2000, 159.3M Oct 2026).
  - **Existing sales near 4M vs 5.2M in 2000:** current pace true (3.99M Sept). 2000 was 5.03M first print, 5.11M revised; 5.2M was 1999. Current sales are still roughly 20% below either, so the point stands.
  - **Housing wealth $7T to $35T:** matches household real-estate net equity ($7.1T 2000Q1, $35.8T 2026Q2). Gross owner-occupied value is $49.8T, so "wealth" means equity. Defensible, undefined in the piece.
  - **Sentiment about half its 2000 level, worse than the Great Recession:** UMich 48.1 (Sept 2026) vs about 107 average in 2000 (about 45%); Nov 2008 low 55.3. True, "roughly half" is loose.
  - The "pent-up demand" headline is a conditional that the piece itself says has unclear likelihood. Framing, not a falsehood.
- **askamerica (1st):** correct verdict, most careful on precision.
  - Caught that 5.2M belongs to 1999 and that 2000 was about 5.0-5.1M, the only arm to do so.
  - Quantified the population overstatement from Census data, defined the $35T as net equity versus $49.8T gross, and verified sentiment against the Great Recession low.
  - Said plainly what is not checkable (the central "wave of sidelined buyers" forecast) and kept it out of the rating.
  - Weakness: did not check the $7T 2000 figure; the "four decades" agent-use phrasing was not confirmed verbatim.
- **expert (2nd):** accurate and sourced, but rating one step high.
  - Good series checks (population 343.1M, PAYEMS, equity $7.1T to $35.8T with the market-value distinction, sentiment) and a transparent medium-confidence flag on months' supply.
  - Accepted 5.2M for 2000 as "per NAR" without checking.
  - Gave 1 Pinocchio for a 2% rounding of the speaker's figure, which the outlet relayed; that is not enough to charge the article. Very terse.
- **everyman (3rd):** least verified and rating one step high.
  - Left $35T unchecked and the 2000 sales baseline from memory. Sentiment checked only as an attributed claim.
  - It reported "no source found" for the wealth figure rather than finding the Z.1 equity series, and left four claims unverified.
  - It did correctly flag the 350M overstatement (but put it at about 8M, against 341.8M) and the conditional headline. Its extra caveat about all-cash first-time buyers is uncited.
  - Sound on honest limits, but the weakest evidence base.

## Recipe check
Not assessed from the files; the three outputs do not show whether a recipe was consulted. Reusable procedure worth encoding: for "grew from X in 2000 to Y" claims, fetch both endpoints from the series (Census/FRED, PAYEMS, UMCSENT, Z.1 equity) and check the baseline year against first-print and revised values (the 5.2M/1999 mix-up), and resolve ambiguous nouns such as "wealth" to a defined series.

## Severity invariant
**Not triggered:** askamerica ranks first, above everyman. askamerica's rating (0) matches the correct verdict; everyman's (1) does not.
