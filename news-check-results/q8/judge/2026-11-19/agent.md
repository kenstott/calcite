# q8 judge — ipwatchdog-gao-uspto-backlog-attrition-7-6-percent (2026-11-19)

Rank: [askamerica, expert, everyman]

Story: IPWatchdog, 2026-11-05, on GAO-27-108491: USPTO's backlog-reduction goals are unsustainable if examiner attrition (7.6% in FY2025, against a 4.8% average) continues. Backlog about 838,000 → 756,000.

All three read the GAO report directly, rate the piece 1 Pinocchio, and catch the wrong statute cite (§ 117 against GAO's § 717(b)(1)). The warehouse has no USPTO operational series, so this was web work for everyone.

**askamerica (#1).** Read the report at files.gao.gov after gao.gov returned 403. It confirmed the attrition path (2.9% → 7.6%), the 4.8% threshold, the backlog numbers, the 18-month wait, the training cut, paused quality surveys and two PPAC meetings. Errors found: Raskin and Johnson are *House* Judiciary ranking members, not Senate (the only persona to catch this); the cut was the 25-hour technical training bank, not "subject matter eligibility" training; and "82,000 over 19 months" is article arithmetic (about 20 months), not Squires' words. Its omission point goes to GAO's headline: the backlog fell while final-decision waits rose 11% and total pendency rose 26.2 → 29.0 months.

**expert (#2).** Went to Squires' written testimony and found "below 754,000" by September, so 756,000 is GAO's August figure misattributed to Squires. Also: GAO sets no deadline for 599,000, and its 2031 scenarios span about 640,000-968,000. Good primary-source work; it missed the House/Senate error and the training-type distinction.

**everyman (#3).** Same core confirmations, the 756,000 attribution and statute slips, and new examiners against "junior". It reads the "unsustainable" headline as stronger than GAO's "unlikely before 2031". A careful answer that misses the committee error and the pendency omission.

**What would change it.** askamerica against expert is close. expert's testimony check and scenario range are as good as askamerica's committee and pendency catches. askamerica is first because its pendency omission goes to GAO's main finding and the committee error is a plain factual mistake in the article.

Severity check: askamerica above everyman. No defect.

Claim shape: no direct quotes of named people; attributed figures (Squires, USPTO General Counsel letter) are relayed as fact, so `author_claims` is the correct group. No resume. Report and agent.md agree at 1 Pinocchio.

Gap audit: "the warehouse has no table for examiner attrition, backlog". Filed govdata-ops#1361 (USPTO unexamined inventory, pendency, examiner workforce; type:sourcing, kind:gap, schema:patents, status:open). The PPAC meeting count is not a dataset and was not filed.

## Recipe check

Nothing warehouse-native to teach; the series is not loaded yet (#1361). askamerica ranked #1.
