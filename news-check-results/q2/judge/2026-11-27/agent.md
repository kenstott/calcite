# Judge — q2 — Fortune, "U.S. defense federal budget / China military investment gap" (2026-11-25), 2026-11-27

**Question/claim:** Fact-check the Fortune piece (citing Brookings). Central claim: "since 1962 defense spending has been squeezed from 49% to 13% of the federal budget"; headline: lowest share of the federal budget since World War II.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web snippets; Fortune via summary; CBO 403 | Warehouse (Treasury MTS, OMB 3.2/4.1, CBO projections, debt) plus Brookings read | Brookings (Riedl, Cancian), AAF, CRFB; CBO 403 |
| Vintage | FY2026 | FY2026 vs FY2025, history from 1962 | FY2026 |
| Pinocchio grade | 2 (provisional) | 2 | 1 |
| Claims graded | 14 | 9 graded in claims.json (plus 3 listed as unchecked in prose) | 18 |
| Verdict on 49% to 13% | 13% supported; 1962 not confirmed | True (OMB 52.3/106.8 = 49.0%; MTS 13.05%) | Verified (Riedl quote) |
| Verdict on "lowest since WWII" headline | Not confirmed | Mostly false (defense was 11.1% of outlays FY2020-21, 12.2% FY2022) | Plausible, not stated by source |
| Other findings | Deficit $1.9T stale vs about $2.0T | $917B is the FY2025 level; FY2026 is $965-966B; China quote is the 2024 Commission, not RAND; deficit about $1.99T | Deficit discrepant; RAND attribution discrepant; omitted 3.6% GDP FY2027 request |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim, 49% to 13%: Brookings (Riedl, Cancian) gives 13% for 2026. askamerica's OMB/MTS arithmetic gives 49.0% for 1962 and 13.05% for FY2026. All three arms support the claim or leave it open; askamerica adds the primary-table check, expert quotes Riedl, everyman left 1962 unconfirmed.

Headline, "lowest since WWII": I checked it with a web search. Treasury's combined statement outlay table gives national defense $754.8B of $6,818.2B total in FY2021 (about 11.1%) and $726.2B of $6,551.9B in FY2020 (about 11.1%). That is below the FY2026 13%. This matches askamerica's "11.1% of outlays in FY2020-21" and contradicts the headline. A CRS item also says defense fell below 13% of outlays in FY2022. So the headline as a budget-share claim is false. askamerica is the only arm that tested it against data. expert said "plausible", everyman "not confirmed". Neither is wrong to hedge, but only askamerica got the right answer.

$917B for FY2026: askamerica shows MTS FY2026 $965.2B vs FY2025 $916.6B, OMB $966.4B vs $916.1B. Expert's own evidence (+$48B) is consistent: 917 + 48 = 965. expert marked $917B "not checkable" and marked the 5.8% as verified, but $48B on $917B is about 5.2%, so 5.8% does not hold with that base. everyman marked both not checkable. askamerica is right and the others missed it.

Deficit: all three agree $1.9T is low; about $2.0T actual. askamerica grades it "mostly true"; expert "discrepant"; everyman "contradicted". The three grade the same gap differently. About $1.99T vs $1.9T is a 5% gap, so "mostly true" is defensible.

China quote: askamerica and expert both found it is from the 2024 Commission on the National Defense Strategy, not RAND three years ago. everyman did not check.

Mandatory programs, $3.5T (+$217B): expert verified it through CBO via AAF (Social Security +86, Medicare +77, Medicaid +55). askamerica marked it "unsupported" because it looked at Treasury totals and found no Medicaid line (see Corpus gaps). This is a miss for askamerica; the claim was checkable.

Net interest: askamerica notes the warehouse shows $1.05T on a different definition and relies on a CBO search summary for +11%. expert verified +$115B, 11% through CBO. Similar result.

claims.json vs report.html: report.html states "Rating: 2 Pinocchios". agent.md states "2 Pinocchios". No mismatch. The agent.md lead sentence ("balanced and partly false") is odd wording but not a grade conflict. claims.json holds 9 entries; the report's unchecked list (JEC 2036, CRFB countdown) lives in prose only.

Open item for the orchestrator: askamerica's FY2020-21 share (11.05%) and FY2026 12.8% came from a live join of OMB outlay tables. I could not query the warehouse. My Treasury arithmetic supports the 11.1% figure.

## Ranking rationale

1. askamerica. It got the central headline right with data, caught the $917B FY2025-for-FY2026 error, and caught the RAND misattribution. Its 2 Pinocchio grade fits a piece with a false headline and a wrong spending level. Weaknesses: it graded 9 claims, fewer than expert; it marked the $3.5T mandatory figure unsupported when it was checkable; the 2.8% GDP figure is its own estimate, flagged as such.
2. expert. Widest coverage (18 claims) and good quote-level checking against Riedl and Cancian. But it let the headline stand as "plausible", left $917B unchecked, accepted 5.8%, and gave 1 Pinocchio, which is too lenient given a false headline and a wrong FY2026 base.
3. everyman. Honest about limits and marked most items "not checkable here". It did catch the stale deficit. It found no primary-data answer on the headline or the defense level, and its grade is provisional.

What would change the ranking: if the warehouse join behind 11.05% is wrong (the orchestrator should confirm), the headline verdict would revert to "unverified" and expert's wider coverage would put it first.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe `fact-check-a-news-article` covers the overall flow. askamerica followed it and caught the issues the other two arms missed. Pattern askamerica did not reach: expert pulled CBO monthly review figures through secondary summaries (AAF) to verify the $3.5T mandatory total and +$115B interest. A broadening of `fact-check-a-news-article` is warranted: for any "program X spending" claim, check the warehouse OMB outlays by agency/function and the health schema (health.medicaid_expenditures_by_state_quarter) before calling a program line missing. The Medicaid miss was a navigation miss on Treasury MTS Table 9, per the orchestrator note, not a data gap.

askamerica is #1, so no cause classification is required; the one defect is the Medicaid navigation miss.

Proposed resolution: b — add to the existing fact-check recipe a step to look for a program line in OMB outlays by function/agency and the health schema when an MTS table lacks it.

## Corpus gaps stated

- "The warehouse has Social Security at $1.667T and Medicare at $1.073T, about $2.74T combined, and no Medicaid line." Reason: Treasury MTS Table 9 has no Medicaid row. Orchestrator note: navigation miss; the warehouse holds Medicaid in health.medicaid_expenditures_by_state_quarter and OMB outlays by agency.
- "The $1.14T level is not reproduced: the warehouse shows $1.05T on a different definition." Definition mismatch, not a table gap.
- "I did not locate the JEC report, so it is not checked." JEC/Schweikert $23T quote; the JEC 2036 claim and the CRFB countdown are likewise unchecked (web, not warehouse).
- "The CBO PDF, the CBO publication page, the cbo.gov home page and the RAND page all returned HTTP 403." Web access failure; CBO figures graded from the warehouse and search summaries.
- "The 2.8% GDP share is my estimate ($966B over roughly $34T of GDP), not a table value." Estimate, not a gap.
