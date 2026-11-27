# Judge — q6 — Medical Daily, "States delay Medicaid work rule cuts to 2027" (2026-11-24), 2026-11-27

**Question/claim:** Oregon, Kentucky and New Hampshire opted into a six-month federal grace period so members who miss work rules keep coverage to mid-2027; Kotek: 600,000+ Oregonians must act, up to a third could lose coverage; Urban: ~166,000 Kentuckians drop off in an average month in 2028; NHFPI: Granite Advantage ~59,000 members, 29,000 could lose coverage; Arizona: ~417,000 of 1.7M AHCCCS members subject; 80 hours a month.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design).
A grep for "askamerica" in the everyman and expert answers (agent.md, agent-report.html) found no hits. No unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article read | No. WebFetch failed twice; article never retrieved | Yes, plus 14 linked pages | Yes |
| Data used | KFF, Georgetown, Fierce, Route Fifty, Stateline via search | Warehouse `health.medicaid_chip_enrollment_monthly` (April 2026, enrollment only) plus KATU, Statesman Journal, LPM, Concord Monitor, KJZZ, RWJF/Urban | CMS SMD #26-006, KFF, OHA, Ballotpedia, Concord Monitor, KJZZ, Hertel Report, Federal Register |
| Vintage | Nov 2026 | Nov 2026; warehouse April 2026 | Nov 2026 |
| Overall grade | None ("not rated") | 0 Pinocchios | 1 Pinocchio |
| Claims graded | 0 (all "not checkable here") | 12 in claims.json | 28 (27 verified, byline not checkable, 4 need context) |
| Central claim verdict | Underlying CMS option real; article's claims not assessed | True (all three opt in through June 30, 2027) | Accurate, with context omissions |
| Visual | none | dashboard.png + report.html | agent-dashboard.png + agent-report.html |

## Verification

Central claim checked by search today. CMS offered an optional hold-harmless period through June 30, 2027 on Nov 13, 2026. States must opt in by Dec 11, 2026. Kotek directed OHA to participate. A report puts at least 10 states plus DC as planning to opt in by Nov 23. My search could not name Kentucky or New Hampshire among them; a separate list of 19 states plus DC asked for a different good-faith exemption. So the central claim is supported for Oregon, and for Kentucky and NH by askamerica's and expert's reads of LPM and the Concord Monitor (Beshear: "This is pushing it off, so we'll take it"). I did not fetch those pages myself. Both askamerica and expert support the article on this point. Everyman gives no verdict.

Figures across arms:
- 600,000 and "up to a third": askamerica and expert both verified the Kotek quote. Match.
- 166,000 Kentucky (Urban): askamerica marked true via RWJF/Urban and LPM. Expert said Urban was read only secondhand and put confidence at medium. Both are consistent. Askamerica's wording is a little firmer than the evidence it names.
- 59,000 and 29,000 NH: both confirmed against the Concord Monitor. Both flagged the state's counter-estimate of 6,300.
- 417,000 of 1.7M Arizona: askamerica confirmed 417,000 via KJZZ and checked 1.7M against the warehouse (AZ total Medicaid/CHIP 1,634,618, April 2026; "1,634,618" is consistent with "roughly 1.7 million"). Expert confirmed it too and added that AHCCCS revised the reporting population to about 104,000 (Hertel Report, single trade source). Askamerica missed that. I did not verify the 104,000 figure.
- 80 hours / $580: both confirm 80 x 7.25.
- Veto wording: both caught that the legislature overrode partial vetoes on April 14, 2026.
- Number of states: expert and askamerica both note at least 10 states plus DC.

Everyman's items (CMS letter, Dec 11 date, June 30 end) match what I found. Its Wyden/Pallone quote and 26-state lawsuit are from search summaries only, and it says so.

Askamerica engine rating versus agent.md: report.html says "Rating: 0 Pinocchios"; agent.md says 0 Pinocchios. No mismatch. The 0 sits slightly oddly beside four listed blemishes, one an omission that "leans toward the critics' view" (errs: toward_thesis).

The warehouse added little: it covers enrollment only, and askamerica said so. The Oregon "affects the most people" check (OR 810,101 adult enrollment vs KY 722,456 vs NH 85,924) is a reasonable use. I cannot run the SQL; open item for the orchestrator is a live check of those three numbers and of 1,634,618.

## Ranking rationale

1. expert. Widest coverage (28 claims), correct central verdict, primary and near-primary sources (CMS letter number, Federal Register, OHA). It found the omissions that matter: the Arizona 104,000 revision, NH's 6,300, and that 10+ states opted in. It was calibrated: it marked Urban and the Arizona revision as secondary and the byline as not checkable. A 1 Pinocchio grade fits "facts hold, context thin".
2. askamerica. Correct central verdict, good source reading (14 linked pages), warehouse used honestly for what it holds, and caught most of the same blemishes. It missed the Arizona revision and graded 0 despite a toward-thesis omission it listed itself. Fewer claims (12 vs 28) and no work on the six-month renewal timing nuance.
3. everyman. Never got the article, so it graded nothing. Its refusal to guess a grade is honest and its background facts are right, but it does not do the task.

What would change the ranking: if the Hertel Report 104,000 revision proved wrong or if AHCCCS says 417,000 is still current, expert's extra omission falls away and the two top arms are close; askamerica's tighter claim list and warehouse sanity check could then put it first. If everyman had retrieved the article it would sit near askamerica.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Expert did something askamerica did not: it checked whether the article's headline population figures were still the agency's current ones, and found a revised Arizona number. Everyman did nothing worth teaching. Askamerica's practice of fetching all linked pages and flagging the NH state estimate is good. The related existing recipe is the one at recipes.json around line 414 (a headline statistic from an unfamiliar domain treated as unverifiable); nothing in recipes.json that I grepped covers "re-check whether a cited population or projection figure has since been revised by the agency" for news fact-checks. A broadened recipe is warranted. Warehouse-native method: for a cited program count, query `health.medicaid_chip_enrollment_monthly` for the latest month for the denominator, then run a dated web search on the agency name plus "revised" or "updated estimate" before grading a projection true. The warehouse itself has no revised-estimate data, so the web step is part of the pattern.

Cause: (b) reachable pattern not used. The search for a revised agency figure was reachable by askamerica's web tools.

Proposed resolution: b — add a news-check recipe to search for agency revisions of cited population or projection figures before grading them true.

## Corpus gaps stated

- "Policy decisions, quotes and projections are not in the warehouse and were checked against the primary reporting." (askamerica; stated reason: warehouse holds enrollment only.)
- "I did not open the CMS letter or Becker's directly; their figures came via KJZZ and the Monitor." (askamerica; source access, not a corpus gap.)
- No claim was graded "not checkable here" by askamerica.
