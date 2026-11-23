# Judge — q4 (TheWrap, "Elon Musk Regrets Spending Nearly $500 Million Backing Republicans"), 2026-11-23

**Task:** fact-check every claim in the TheWrap article of 2026-11-18 and grade it on the 0-4 Pinocchio scale.

Rank: [askamerica, everyman, expert]

Fingerprint check: passed for all three. Everyman and expert gave 1 Pinocchio, askamerica 1 Pinocchio. The grade is the same across arms, so the ranking rests on claim-level accuracy and calibration.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Grade | 1 Pinocchio | 1 Pinocchio | 1 Pinocchio |
| Article read | summary only (WebFetch summarised) | fetched in full, plus 5 linked TheWrap pieces | summary only |
| Claims | 15 | about 10 grouped, with claims.json | 14 |
| Primary data | none | `fec.individual_contributions` (3 queries on Musk) | none |
| Quote truncation | missed | caught | missed |
| OpenSecrets $445M | accepted via Adweek (correct) | called it AP-attributed, unconfirmed (wrong flag) | "partly", cites $458M |
| $158M | supported, notes transfer caveat | $158M vs $168M reconciled | verified (AP), Paxton pass-through "not confirmed" |
| 51-seat goal | unverified | unsupported (GOP held 53) | unverified (GOP held 53) |

## Independent checks I ran

- **Quote truncation.** A search returned the CNBC rendering: "I feel pretty foolish about having spent so much money and not seeing much for it — or maybe it would have been much worse without that." Askamerica's flag that TheWrap cut the hedge is correct. Everyman and expert both marked the quote "supported" and missed it.
- **OpenSecrets.** Anadolu and KCRA both attribute $445.5M to OpenSecrets. TheWrap's attribution is right. Everyman was correct. Askamerica said it "found it attributed to AP, could not confirm OpenSecrets" and labelled the claim "mostly true". That is a wrong inaccuracy flag on a claim that holds up, though it was hedged rather than asserted as false. Expert hedged ("OpenSecrets page not fetched") and added a $458M Daily Wire figure.
- **$158M vs $168M.** Reporting (CNBC, IBTimes, Common Dreams) gives $75M on Sept 9 plus $83M on Sept 23, which is $158M, matched by SLF transfers to Texas PAC. Votelog shows earlier $5M gifts (June and December 2025) that explain a $168M total. TheWrap's $158M is right. Askamerica's warehouse $168M across four gifts is the larger all-gifts figure, and it said so. It graded the claim "mostly true", which is slightly harsh, but it explained the gap. Everyman's account of the money chain was the most accurate of the web-only arms. Expert's "pass-through not confirmed" is weaker, since the matching transfers are documented.
- **Headline.** Reporting varies ($442M The Hill/AOL, $445.5M OpenSecrets, $474M NYT, "nearly $500M" TheWrap/Breitbart). All three arms flagged the headline as rounded up. Expert's "AP and Reuters also say nearly $500M" is loose: Al Jazeera attributed "nearly half a billion" to Reuters, and I did not find the AP wording.

## SQL sanity check (calls.jsonl)

- seq 11 sums `fec.individual_contributions` for `contributor_name ILIKE 'MUSK, ELON%'`, `amount >= 100000`, `year = 2026`, joined to committee names, with no `memo_cd` filter. The result ($358M through Sept 30, $168M to SLF PAC) could therefore include memo-flagged lines. The response carried no diagnostic, and the answer does not say whether the figure was checked for memo or earmark duplication.
- seq 12 does apply `memo_cd IS NULL OR memo_cd <> 'X'` for the 2024 and 2025 comparison, and the server returned `explicit_exclusion:caution`. So the arm used the exclusion in one query and not in the other. The inconsistency is a minor defect, but it affects the headline warehouse number. The $358M figure agrees with the reporting that $445M includes spending beyond individual contributions to committees (America PAC and others), and it is stated as a floor, not as a total.
- The `ILIKE 'MUSK, ELON%'` name match is a reasonable proxy, but it is not checked against a filer ID or occupation.

## Ranking rationale

**1. askamerica.** It is the only arm that read the article in full and followed the linked pieces. That produced two findings the others lacked: the truncated quote that drops "or maybe it would have been much worse without that", and the internal inconsistency across TheWrap's own pieces ($224M, $440M+, $158M plus $80M). The truncated quote is the most important calibration finding in the story, because "regrets" is the article's thesis. It used the FEC warehouse to test the money claims and reconciled the $158M and $168M figures. It was wrong to doubt the OpenSecrets attribution, and its first query had no memo filter. Neither error changes a verdict.

**2. everyman (narrowly).** It got the OpenSecrets attribution and the money chain right and made the best point on framing: "regrets" is stronger than "foolish" and "would have spent less". It did not read the article text and missed the truncated quote. It left two claims unchecked (Talarico quote and "not just about money") and was honest about it.

**3. expert.** The 14-claim list is well structured, and it is the only arm to cite the Senate seat count (56-44 per AP). But it left three claims unverified that other arms confirmed (checks-and-balances, "committed no crimes", the pass-through), and its OpenSecrets and headline evidence is looser. It also read the article only through a summary. Its grade matches the others but is the least supported.

**What would change the ranking:** everyman vs expert is a close call. If the grader weighted the Senate seat count and the structured claim table more than the money-chain accuracy, expert would move to second. For askamerica to fall below everyman, its $168M or OpenSecrets error would need to change a verdict. Neither does.

## Severity invariant

askamerica ranks above everyman. No defect flag.

## Recipe check

Searched `recipes.json` for "fec", "donor", "contribution". Relevant existing recipes: `fact-check-a-news-article` (already covers attribution and quotes), `campaign-money-name-the-channel-before-ranking-industries` (super PAC channel naming). Neither mentions `memo_cd`, earmarks or conduit double counting, and askamerica ran the `memo_cd` exclusion in one query but not the other. The quote-truncation catch came from askamerica itself, so it is weak grounds for a recipe (it shows the pattern is reachable). The unaided arms showed no pattern that askamerica lacked.

**Resolution: none.** No unaided arm demonstrated a behavior askamerica did not reach, and the one defect (no `memo_cd` filter in the first FEC query) is already surfaced by the server as an `explicit_exclusion:caution` diagnostic on the second query. Adding a recipe sentence about memo-line deduplication to `campaign-money-name-the-channel-before-ranking-industries` is optional if this recurs.
