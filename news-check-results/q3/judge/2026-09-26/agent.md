# Judge: q3 — "2026 cycle IE spending tops $1.45B; Talarico race hit $59.6M in a 4-week window"

Blind judge of three fresh, same-day persona runs (news claim, Workstream A of the daily-eval pipeline).

Rank: [expert, askamerica, everyman]

## A three-way conflict, resolved by dating the sources

All three agree the exact dollar figures in the claim (cycle total $1,452,997,175/881 candidates; four-week window $328,333,233; Talarico $59,564,521/17 committees/$2,086,549 support/$57,477,973 oppose) cannot be confirmed against any FEC.gov-published aggregate. **Expert traced them to their actual origin**: a single third-party site, thepoliticalgroup.com's "PAC Spend Tracker 2026," which discloses its own methodology as an unvetted sum of raw FEC Schedule E API line items — no second source anywhere repeats these specific numbers.

Where the three personas genuinely diverged is on the *direction* of Talarico-race spending (support vs. opposition):
- **askamerica**, computing directly from the corpus's own `fec.independent_expenditures` cycle-to-date data, found opposition dominant (90.8% oppose share) — consistent with the claim's direction.
- **everyman** found a July 20, 2026 Texas Tribune article showing the *opposite* — support (~$4.1M) far exceeding opposition (~$763K) — and reported this as evidence contradicting the claim.
- **expert** found the resolution: everyman's source predates a major opposition ad surge in September 2026 (MAGA Inc.'s $10M buy ~Sept 5-7, Lone Star Liberty PAC's "$19M+" cumulative by Sept 22, Senate Leadership Fund's $100M commitment announced ~Sept 22) — the race's spending balance genuinely flipped between July and the claim's August 24-September 14 window. Both askamerica's corpus-computed direction and expert's September-dated sources agree opposition dominates by the claim's actual window; everyman's contradicting evidence was real but stale relative to the specific dates in question.

## Reasoning for the ranking

**Expert** is first: it's the only answer that both traces the disputed figures to their actual unvetted source and resolves the apparent three-way conflict on the Talarico direction by checking publication dates against the claim's specific window — a genuinely valuable piece of source-hygiene work. It also independently sums named-PAC opposition spending clearly inside the window (~$20-30M) and correctly notes this doesn't strictly contradict the claimed $57.5M, since 13-14 of the alleged 17 committees are unnamed in public reporting and could plausibly close the gap — appropriately calibrated as "not reconcilable to the dollar," not "false."

**askamerica** is second: its corpus-based approach is methodologically the soundest of the three for the parts it *can* check — it found and correctly excluded a real, severe data-quality artifact (a $40B/27x inflation from spam/prank filings by one candidate, `Bettis, Shawn`) that would have badly corrupted a naive sum, and it correctly flagged that `transaction_date` is 100% NULL, making the four-week-window sub-claims structurally impossible to check from this table — both of these are real, previously-filed defects (`govdata-ops#586`, `#587`). Its cycle-total estimate ($1.66B/841 candidates, after excluding the spam rows) lands within 7-14% of the claim, a plausible reconciliation. It's ranked below expert because it couldn't reach the September-dated news coverage that resolves the Talarico direction question, since its verification was corpus-only.

**everyman** is third: its finding was real and useful (correctly identifying that the specific dollar figures trace to no verifiable primary source, and that a July snapshot showed the opposite spending direction), but it treated a stale, out-of-window source as directly contradicting the claim rather than checking whether more recent reporting existed — the exact gap expert's dispatch closed.

## Severity check

askamerica ranks second, above everyman — no severity flag.

## Recipe / resolution check

The two real defects underlying this question (`fec.independent_expenditures.transaction_date` 100% NULL, and unvetted spam filings inflating raw sums) are already filed and open as `kenstott/govdata-ops#586` and `#587` from earlier this session — no new filing needed. Worth noting for future FEC-related news checks: this connector's IE data cannot answer any date-windowed question until #586 is resolved, a structural limitation independent of which specific claim is being checked.
