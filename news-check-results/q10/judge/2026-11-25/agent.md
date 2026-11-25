# Judge — CIDRAP, "Hepatitis B vaccination rates in US newborns falling" (Truveta / NEJM Evidence), 2026-11-25

**Question/claim:** Birth-dose hepatitis B coverage fell from 80.0% (2020-22) to 65.6% (2026) after ACIP dropped the universal recommendation, plus the study's supporting figures and the policy background.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | CIDRAP page via summarizer only; paper and PDF unreadable | Full NEJM Evidence PDF, CIDRAP vote/schedule/court stories, VIP report; warehouse births | NEJM Evidence page, Truveta blog, HHS, PBS, Boston Globe |
| Vintage | 2026 article; older JAMA data as proxy | Paper dated Nov 2026 | Paper and Truveta, 2026 |
| Pinocchio grade | 1 | 1 | 0-1 |
| Claims graded | 9 (6 of them "not checkable") | 13 | about 10, grouped |
| Central claim verdict | Not checkable; direction plausible | Mostly true (65.6% matches; "after" framing overstated) | Accurate, with a context caveat |
| Visual | none | dashboard.png, report.html | dashboard.png, report.html |

## Verification

Central claim checked by web search. Truveta's own write-up and press coverage give 65.6% for Jan-Jun 2026, 80.0% for 2020-22, 1,338,961 infants, about 360,000 unscreened-mother births per year, and 93.9% for infants of HBsAg-positive mothers in 2026. Coverage also says the decline began in 2023, nearly three years before the ACIP change. This supports askamerica's verdict (numbers right, causal "after" framing overstated) and agrees with expert on the numbers. It contradicts everyman's "not checkable", since the paper and Truveta page were reachable.

- askamerica numbers match its claims.json and the paper: 1,338,961 and 1,075,767; 90.5%; 15.6% to 7.6%; 1,016,149 (75.9%); 96.9, 75.9, 76.8, 75.4; 97-98% falling to 93.9%. The 360,000 and 648 arithmetic checks (10% x 3.6M; 0.18% x 360,000 = 648). The births denominator (3,596,017 for 2023; 3,628,934 for 2024) comes from health.cdc_wonder_natality and could not be re-queried by me. It is an open item, minor.
- askamerica caught two real flaws the others missed: HBsAg called an "antibody" (it is an antigen), and "median" versus the paper's "mean". Expert noted the mean/median point too, but not the antibody label. Everyman missed both.
- Expert caught that the decline predates the ACIP vote, and that the EHR sample is not national. Both agree with the web evidence.
- Everyman graded 6 claims "not checkable" where the paper was available to the other two arms. It also called claim 8 (court block "in March") unconfirmed; expert confirmed 16 March 2026. Its note that screening of mothers improved is a fair point.
- Expert reported 75.4-76.8%, 360,000 and 640 as not checkable (NEJM 403, PDF 404). askamerica read the PDF and verified them. That is a coverage gap for expert.
- Engine rating check: report.html says "Rating: 1 Pinocchio" (13 claims graded: 9 true, 3 mostly true, 1 mostly false; honesty 88, bias +33). agent.md says 1 Pinocchio. No mismatch. The one "mostly false" is the antibody label, graded on a minor term, which fits.

## Ranking rationale

1. askamerica: right central verdict, 13 claims covered, each tied to the primary paper, two label/wording errors found, causal framing flagged. It used the warehouse only where it applies (births) and said so. Rating computed by the engine matches the stated grade.
2. expert: right central verdict and numbers confirmed against NEJM/Truveta. Rating of 0-1 is slightly generous given the antibody error it did not see. Left several figures unchecked because of blocked pages. Terse, with fewer claims.
3. everyman: could not reach the paper, so the main numbers were "not checkable". Reasonable calibration but low coverage and no primary evidence for the central claim. It used an older JAMA series as a proxy, which it labeled as a different dataset.

What would change the ranking: if the births figures or the PDF numbers askamerica quoted were wrong on a live check, expert would move up. None of the primary-source checks I ran suggests that.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

askamerica did the right thing: it read the primary PDF and used the warehouse only for the denominator. Existing recipe `fact-check-a-news-article` covers the method (grep of recipes.json). Neither unaided arm found a pattern askamerica missed. askamerica is #1, so no cause classification is needed beyond the line below.

Proposed resolution: d — no actionable cause; askamerica ranked first and followed the existing fact-check recipe.

## Corpus gaps stated

- askamerica (agent.md): "It has no birth-dose coverage or EHR table, so study figures were graded against the primary paper, not recomputed from independent data."
- claims.json and report.html: no claim graded "not checkable here" (0 not gradable).
- askamerica also noted the NEJM Evidence editorial and index pages returned HTTP 403 (web access, not a corpus gap).
