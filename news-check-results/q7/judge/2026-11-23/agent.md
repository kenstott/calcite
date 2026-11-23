# Judge - News q7 (OPB/NPR, "Democrats' House win was the largest in 50 years"), 2026-11-23

Rank: [askamerica, expert, everyman]

Story: 56% to 42% House popular vote (14-point spread), 46 seats flipped, 260 seats possible. Fingerprints pass for all three. Judged files: everyman agent.md; askamerica agent.md, claims.json, report.html, calls.jsonl; expert agent.md and agent-report.html.

## Ground truth I checked
WebSearch (Brookings Vital Statistics, Wikipedia): the 1974 House margin was about 16.8 points (Brookings 1974 row about 57.1% Democratic). The 1976 margin was about 13.6 points (Wikipedia), with a Brookings row of about 56.2% Democratic. So 1974 clearly exceeds 14 points, and 1976 is within a point of 14 either way, depending on the vote base. 1974 is 52 years back, so it sits outside a strict "50 years" window. 1976 is the real test, and it is a tie within rounding. A fair verdict is "true as worded, with 1976 an essentially equal result". The 1976 margin does not clearly exceed 14 on any source I saw.

## askamerica's 14.3 figure
calls.jsonl seq 8 ran against `officials.house_election_results`. It filtered `stage='gen'`, no specials or runoffs, `uncontested=false`, `writein=false`, `candidate_kind='candidate'`, and grouped by year. So 14.3 is a contested-races-only margin over all candidate votes. It is not the all-seat national vote that NPR and Brookings use (Brookings 14.1 per askamerica, Wikipedia 13.6). askamerica cross-checked against Brookings and reported both figures. It also flagged that its own contested-only 2018 figure (10.9) was an artifact. The 14.3 should be read as a sensitivity check, not a finding that 1976 beat 2026. Its prose says "equal to or slightly above within rounding", which is the right level of hedging.

## Ranking rationale
**1. askamerica.** It is the only arm that put numbers on the central claim. It queried the warehouse for 1976 and 2008, got 56.1/41.9 and 10.7, and reconciled them with Brookings (56.2/42.1, and 16.6 for 1974). It found that 2026 ties 1976 and that the article's comparison list omits 1974 and 1976. It checked 260 seats against House Clerk party counts (102nd 267, 101st 260) and against AP/CBS tallies (258 won, 2 outstanding). It checked the five control changes against House History. It marked 56/42 and 46 flips as unsupported, correctly, because the warehouse has no 2026 rows and AP/CBS report 44 net with no gross count. Weaknesses: the headline rating of 0 Pinocchios sits oddly beside "mostly true", two unsupported claims and a tie with 1976 that the article does not state. 1 Pinocchio fits better. The "borderline" caveat in the text partly repairs this. The 14.3 figure comes from a contested-only basis that is not the national-vote basis.

**2. expert.** It rated 1 Pinocchio, with low-to-medium confidence. It flagged the right risk (the claim depends on the comparison window and on raw shares), and it added a Newsweek two-party split (56.9/43.1) and a NYT figure of 257 for the 2008 peak. But it says 1974 and 1976 margins were larger than 14 "by memory (unverified)". That is right for 1974 and wrong or unproven for 1976. It retrieved no primary table, and it called 46 flips and the 56/42 split "not checkable". Calibration is reasonable but the evidence is thin.

**3. everyman.** It rated 1 Pinocchio, provisional, and was candid about its depth. It said from memory that 1974 was about 16 and "no later House popular-vote margin comes close". That second part is wrong, because 1976 is within a point of 14. It never tested the 1976 comparison, so it accepted the headline's window. It was honest that nothing was retrieved from primary sources.

**What would change the ranking:** if the judge counted askamerica's 0 Pinocchios as a calibration failure heavy enough to outweigh its primary-data work, expert could edge ahead. I do not weigh it that heavily, because the report text carries the correct caveats and askamerica's evidence base is much stronger. The expert/everyman order is close: expert is better on window framing and extra sources, and both rest on memory for 1974 and 1976.

## Severity invariant
askamerica ranks above everyman. No defect flag.

## Recipe check
`fact-check-a-news-article` already exists in recipes.json (id at line 1380, 13 numbered rules) and covers extraction, central-claim grading, partial-period, and gross-vs-net handling. A separate `check-gross-vs-net-before-treating-as-disagreement` recipe also exists, which covers the 46-versus-44 flip count. askamerica followed these: it graded the central claim against warehouse data and tested the "50 years" window against 1976. No unaided arm found anything askamerica missed, so there is no gap to teach. The residual issue (a headline rating of 0 beside a tie and two unsupported claims) is a calibration judgment in one run, and a recipe line would be speculative from one case.

**Resolution: none.** Existing recipes covered it, and askamerica placed first.
