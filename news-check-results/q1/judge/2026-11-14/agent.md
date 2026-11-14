# q1 judge: AI Weekly "Nvidia, Anthropic and OpenAI Set Q3 Lobbying Records at $2.2M, $2.1M and $1.5M as Antitrust Talks Begin"

Rank: [askamerica, expert, everyman]

The alert is dated Oct 22, 2026 and rewrites a Semafor piece of Oct 21. Judged 2026-11-14. I checked the arms against each other and against the live `law.lobbying_filings` rows returned to me today (Anthropic, OpenAI, Nvidia by quarter).

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 0 | Warehouse LD-2 filings and activity text; Semafor original in full |
| expert | 1 | lda.gov API; Semafor original |
| everyman | 1 | None (lda.gov returned 503); secondary coverage only |

## Assessment
- **Correct verdict: 0 to 1 Pinocchio, with 1 the better fit.** Every dollar figure is right. Two things are shaded: "specifically backed" the CATS Act (an LD-2 lists a bill, not a position), and the headline's "as Antitrust Talks Begin" (the filings show lobbying on a topic, not talks).
- **askamerica (1st, narrowly):** the most complete set of findings, and none of them wrong.
  - All four spend figures matched to the filing: Nvidia $2.15M, Anthropic $2.11M, OpenAI $1.53M, Meta $5.88M.
  - "Meta led Big Tech" tested against Amazon, Google and Apple. It is the only arm that found Google's figure ($3.23M).
  - It quoted the OpenAI filing text verbatim and found S.5105 in Anthropic's filing.
  - It flagged both shaded items, then still scored 0. That is one step low.
  - Its disclosure "could not see pre-2024 quarters for Nvidia" understates the hole. The warehouse has no Nvidia rows before 2025 Q2. The record claim still holds on the public record (expert's lda.gov check agrees).
- **expert (2nd):** right rating, primary data, one doubtful finding.
  - Same four figures from lda.gov, plus the outside-lobbyist context (about $0.93M more for Anthropic).
  - It could not find Alphabet's filing, so "Meta led Big Tech" is tested against three firms only.
  - It reports a "wrong byline" error (Semafor credited to Alexis Dufresne). It saw only a tool summary of the page, and everyman's reading of the same page has Dufresne as the AI Weekly author crediting Semafor. The finding is not established and it feeds the rating.
- **everyman (3rd):** same rating as expert, no primary source behind it.
  - Every figure rests on secondary coverage, which it said plainly.
  - It calls the URL slug "22m/21m/15m" a ten-fold error. It is "2.2m" with the dot dropped. That is a wrong finding.
  - It did catch the "backed" overstatement and the headline framing.

**What would flip 1st and 2nd:** if the byline error is real, expert has the right rating and no wrong finding, and goes first. If the grade is weighted above the findings, expert also goes first.

## Recipe check
No recipe proposed. askamerica reached the warehouse-native method unaided and the unaided arms showed nothing it missed.

One engine observation, first instance, no change made: the engine scored 0 with one author claim at "mostly true" and the headline framing recorded only in prose. The headline phrase was never entered as a graded claim, so it could not move the score.

## Severity invariant
**Not triggered.** askamerica ranks above everyman.

## Defects
- Data: `law.lobbying_filings` is missing client quarters (Anthropic 2025 Q2-Q3, OpenAI 2025 Q1-Q2, Nvidia before 2025 Q2) and 2023 Q3 holds about half the usual filings. Verified live today. Filed as kenstott/govdata-ops#1198.
- Data: an original LD-2 and its amendment both carry `expenses`, so a per-quarter sum double-counts. Noted in #1198.
