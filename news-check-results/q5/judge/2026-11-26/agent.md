# Judge: White House/ONDCP, "U.S. Drug Overdose Deaths Decrease for 18th Consecutive Month Under Trump Administration", 2026-11-26

**Piece:** https://www.whitehouse.gov/releases/2026/11/u-s-drug-overdose-deaths-decrease-for-18th-consecutive-month-under-trump-administration/. Fact-check it, extract its claims, and grade it on the 0-4 Pinocchio scale.

Rank: [expert, everyman, askamerica]

**SEVERITY: askamerica ranks below everyman.** This is a product defect, not a fair result (see "Resolution").

All three personas ran fresh today. Fingerprint: the everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. No unaided arm mentions askamerica. The ranking below judges the original askamerica answer, preserved at `judge/2026-11-26/askamerica-pre-fix/`. The post-fix confirmation rerun now sits at `askamerica/2026-11-26/` and is assessed separately at the end.

## Answers in brief

| | everyman | askamerica (original) | expert |
|---|---|---|---|
| Data | CDC VSRR API (data.cdc.gov xkb8-kh2a) | Warehouse `health.cdc_state_drug_overdose_deaths` (CDC provisional, as of Nov 1) | CDC VSRR API (xkb8-kh2a) |
| Figures (65,724; -41.5% from 112,418; 5 drug classes) | All match | All match | All 8 match exactly |
| "Lowest since Dec 2016" | Not verified | True (Dec 2016 64,557; Jan 2017 66,194) | True (same values) |
| Pre-inauguration context | About 2/3 of the fall came before Jan 2025; pace slowing | Fall began late 2023; 28% down by Dec 2024; pace slowing; drug-class changes are year-over-year | 67% of the fall came before Jan 2025; the real streak is 34 months, not 18; down 19.2% under the current term |
| Carter quote | Opinion; not tested | Unsupported (subject_claims) | Verbatim; opinion that the data cannot test |
| **Overall grade** | **2** (facts accurate, credit overstated) | **0, "balanced and accurate"** (engine-computed) | **2** (accurate numbers, misleading framing) |

## Verification

- Every arm reproduces the same CDC values, so the figures are not in dispute. Askamerica's warehouse values match the two API pulls exactly, including the 64,557 / 66,194 bracket and the 81,313 Dec 2024 value. That confirms the warehouse copy is current.
- The question is the verdict. The release is a credit-taking piece. Its headline and its "every month since President Trump took office" framing start the count at the inauguration, and that omits a decline already running for 16 months that accounts for about two-thirds of the total fall. Both unaided arms graded it 2 Pinocchios, which is the Fact Checker's standard grade for accurate numbers used to mislead.
- Askamerica found the same omission. It then published 0 Pinocchios with the headline "balanced and accurate". The cause is in how the claims were shaped:
  - It marked the 41.5% arithmetic as the author's `central` claim.
  - It graded the "since Trump took office" streak plain true.
  - It put the causal credit under the ONDCP director as `subject_claims`, which sits outside the headline block the engine rates.
  - `ClaimScoring.pinocchios` rates only the headline block's checked-claim accuracy, so the misleading framing could not move the rating.

## Ranking rationale

1. **expert:** best calibrated. It verified every figure plus the 2016 bracket, quantified the pre-inauguration share (67%) and the true streak (34 months), confirmed the quote verbatim, and gave 2 Pinocchios.
2. **everyman:** the same correct verdict (2) and the same two-thirds finding from the CDC API. It left the 2016 claim unverified.
3. **askamerica:** its data work was as complete as expert's, from the warehouse. Its published verdict contradicts its own findings: 0 Pinocchios and "balanced and accurate" on a piece its own text calls credit-taking with a misleading start date. A reader of the rating and headline alone would draw the wrong conclusion. This is the strict-superset failure: everyman, with less, delivered the right verdict.

What would change the order: if the deliverable were judged on figures alone, askamerica would tie expert for first. It is not. The task asked for an overall grade that flags material misleadingness.

## Resolution (exactly one): McpServer.java instruction change

Under VALIDATING, next to the `author_claims`/`subject_claims` definitions, I added two bare MUST rules:
1. In a press release or statement issued by an organization, a quote from that organization's own officials MUST be grouped `author_claims`.
2. A headline or sentence that ties a trend to a person or policy by its start date ("under X", "since X took office", "Nth straight month under X") MUST be extracted as its own author claim that X produced the trend. It must be graded against the same series before that start date, and marked `central` when it is the piece's thesis.

Build and install: `:askamerica-engine:shadowJar` built clean. The claim tests passed: ValidationClaimsRequiredTest, ValidatorSessionStateTest, 14/14. I backed up to `engine-dev/askamerica-engine.jar.bak-2026-11-26-press-release-thesis`, installed atomically with `mv` to `~/.askamerica/engine-dev/askamerica-engine.jar` (production is untouched), confirmed the new text is in McpServer.class with `grep -a`, and ran smoke_test.py (OK).

**Confirmation rerun: partial.** The new answer now grades the "declined every month since Trump took office" streak **mostly true, errs toward_thesis**. It states that 27.7% of the 41.5% fall (about 67%) happened before the inauguration, and calls the framing "materially misleading". So behavior moved. However:
- the Carter quote is still `subject_claims`, so rule 1 was not followed;
- no separate implied-causation author claim was extracted, and the arithmetic claim is still `central`, so rule 2 was only half followed;
- the engine-computed rating is **still 0**. A "mostly true" credit does not count as inaccurate under `ClaimScoring.pinocchios`.

The fix is therefore **not confirmed** as closing the defect. The remaining lever is the scoring rule, where a mostly-true central framing claim that errs toward the thesis cannot lower the rating. That is a product decision about the rating rubric, not an instruction tweak, so I logged it for the maintainer instead of attempting a second resolution today.

## Recipe check

The lesson worth teaching: **a credit-taking release is graded on its implied causal thesis, tested against the series before the start date it picks.** This is now in the McpServer instruction (above), so no separate recipe was added. The unaided arms' evidence (the 2/3 share, the 34-month streak) is the motivating case. The warehouse-native method is the same table, `health.cdc_state_drug_overdose_deaths`, queried before and after the start date.
