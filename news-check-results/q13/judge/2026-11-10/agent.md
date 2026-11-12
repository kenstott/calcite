# Judge: CNBC, long-term unemployment and the October 2026 jobs report (q13), 3-persona 2026-11-10

Rank: [expert, askamerica, everyman]

Source: https://www.cnbc.com/2026/11/06/long-term-unemployment-october-jobs-report.html (Nov. 6, 2026).

**Engine build:** private dev jar 0.128.0 (`~/.askamerica/engine-dev`). **Data server:** `~/.askamerica/pgwire-dev` on port 45533.

Grades: expert 1 Pinocchio, askamerica 1, everyman 1. All three agree on the core figures: 27.1 weeks mean duration (Sep 24.8), 1.963M unemployed 27+ weeks at a 27.6% share, payrolls +165k, unemployment rate 4.2% to 4.1%.

## Why

**`expert` is first.** It had the widest coverage of any arm:
- It confirmed both historical comparisons ("longest outside the pandemic since 2016", "highest share since late 2021") on FRED.
- It confirmed the supporting claims: JOLTS September layoffs (1.641M, lowest since March 2025), the Disney and Amex cuts, and the 59k consensus.
- It read the Richmond Fed August 2026 brief and found it calls skill erosion and stigma modest, so CNBC overstates that brief.
- It flagged the main misleading frame: the "disturbing trend" rests on one 2.3-week jump in a noisy series, while the 27+ week count and share have been about flat since May-June and BLS says the count "changed little".
- Weakness: WebFetch returned a summary of the article, so its claim list may be incomplete and the quotes were checked only in summary form.

**`askamerica` is second.** On depth it matches expert. It confirmed both historical comparisons from `econ.employment_statistics`, including the Aug 2016 (27.2 weeks) and Dec 2021 (31.2%) anchor points. It graded Stahle's "longer than benefits last" as partially false, reasoning that it rests on one month and on a mean-duration reading. It scored Stahle's claims in their own block, which is correct. But it left JOLTS, Disney/Amex, the Richmond Fed brief and Waller ungraded, though all are web-checkable and expert confirmed them in minutes. It also missed the "count changed little" counterpoint, which sits in the same BLS table it queried.

**`everyman` is third.** It confirmed the headline figures but could not confirm either historical comparison, which are the article's central trend claims. It did flag the noisy single-month move and the missing October 2025 household data.

## Severity invariant

`askamerica` ranked above `everyman`. No flag.

## Defects

None found. The gap audit came up empty: askamerica's ungraded items were left unchecked, not reported as unavailable.

## Recipe check

This is the second instance of askamerica's depth-over-breadth pattern; the first was q12 the same day. Both times it ran thorough warehouse queries on the central figures, then left the web-checkable supporting claims ungraded, and both times it lost breadth to expert. The second instance clears the evidence bar for a recipe nudge: enumerate every factual claim and either grade it (warehouse or web) or state why it can't be checked; for a trend claim, also test whether the level series supports a trend or only a one-period move. Proposed, not applied in this run.
