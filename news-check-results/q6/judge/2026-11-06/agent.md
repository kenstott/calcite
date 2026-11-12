# Judge: ABC News "Measles cases surpass 5,000 in the US with infections confirmed in 45 states: CDC" fact-check (q6), 3-persona 2026-11-06

Rank: [expert, everyman, askamerica]

Source: https://abcnews.com/Health/measles-cases-surpass-5000-us-infections-confirmed-45/story?id=137907591
(Mary Kekatos, published 2026-11-06).

**First story on the main-based engine build** (main 845f96d8d6 plus #1101 and #1102), installed
2026-11-06 12:13. q1-q5 ran on a stale Oct 8 govdata build.

## Why

All three personas gave 1 Pinocchio, and none found a material error. The ranking turns on how
much each could verify.

**`expert`** fetched the article and CDC's measles page directly and confirmed:
- 5,182 cases as of Nov 5, and 18 international-visitor cases
- the 4 CDC-confirmed deaths, against 8 state-reported
- the 2025 total of 2,289, and 4,993 of 5,182 cases outbreak-associated

It resolved the "45 states plus DC" wording: CDC lists 47 jurisdictions with NYC separate, which
is 45 states plus DC once NYC is folded into New York. It confirmed kindergarten MMR at 92.4%
(2025-26) and the 96/2/2 vaccination split through secondary relays of CDC data. It was the most
careful about which figures it had read at the primary source.

**`everyman`** read CDC's measles page directly as well and verified the case count, visitor
cases, deaths and both state "first death in decades" claims. It raised a question about the
timing of the Jefferson County, PA death (state-confirmed Sept 15, while the article says "this
week"). Expert's finding that the newest PA death was a different woman (21, unvaccinated)
suggests the article may have conflated two deaths. Neither persona settled that.

**`askamerica`** made no errors but could verify the least. Its `web_fetch` got HTTP 403 from
cdc.gov, so it graded the vaccination split, hospitalization share and visitor count as `not
checkable here`. That grading is correct under the new #1102 rule. From the warehouse,
`health.cdc_nndss_weekly` gave 4,641 cases at MMWR week 43 (ending Oct 24), rising about 240 a
week, which makes 5,182 on Nov 5 plausible but not reproducible (the table lags about two weeks).
One exploratory query double-counted rollup rows (9,269). It discarded that and disclosed it,
after the engine's QC refused the first preview until it did.

## Severity check

**SEVERITY FLAG: `askamerica` ranked at or below `everyman` (#3 vs #2).** The cause is access,
not reasoning. The engine's `web_fetch` is blocked by cdc.gov from this machine (also 403 with a
browser user-agent via `curl`), while the client's WebFetch reaches it. The persona had the
client WebFetch available but did not fall back to it.

## Resolution (exactly one)

Filed **govdata-ops #1105** (type:defect, kind:core, status:open). On a 401/403/429/451, the
`web_fetch` result and the validation instructions should direct the model to retry with the
client's own fetch before grading `not checkable here`. Seen three times today: bls.gov (q1),
opensecrets.org (q4) and cdc.gov (q6).

## Fix verification on this build

- **#1102 (unreachable means not checkable here): working.** The CDC-dependent claims were graded
  `not checkable here`, not `unsupported`.
- **#1101 (refuse a validation with no claims): bypassed.** The persona called `preview_report`
  with neither `claims` nor `source_url`, so the refusal, which keys on `source_url`, never fired.
  That is 6 of 6 today. Reported on #1101 with proposed extra triggers.

## Gap audit

The warehouse's measles data, from CDC's weekly NNDSS counts, ends at MMWR week 43 (Oct 24), about
two weeks behind CDC's Nov 5 count. That fits NNDSS publication lag, not a defect. The
kindergarten MMR coverage figure is sourced (#662). Nothing new to file.
