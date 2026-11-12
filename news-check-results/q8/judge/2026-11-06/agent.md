# Judge: NPR "A new study gives us a clearer picture on how many people have been deported" fact-check (q8), 3-persona 2026-11-06

Rank: [expert, askamerica, everyman]

Source: https://www.npr.org/2026/10/23/nx-s1-6012356/a-new-study-gives-us-a-clearer-picture-on-how-many-people-have-been-deported
(All Things Considered, Jasmine Garsd, published 2026-10-23). Third-party claims: the Migration
Policy Institute study by Marc Rosenblum, and DHS's self-deportation figure.

**Engine build:** main 845f96d8d6 plus #1101, #1102 and #1105 (unchanged since q7).

## Why

All three personas found every checkable number consistent with MPI's fact sheet:
- about 980,000 deportations from Jan 2025 to Jul 2026, against 1.86M in FY2000;
- 69% intercepted at the border;
- interior deportations rising from 8,000 to 19,000 a month;
- 20% with a conviction;
- self-departures of 310,000-1.5M, against DHS's 3M+.

All three flagged that "deportation" mixes removals, returns and turnarounds.

**`expert`** (1 Pinocchio) read the most primary material. It pulled NPR's full transcript with
curl after WebFetch timed out, and read MPI's PDF fact sheet directly. It gave the sharpest
decompositions:
- 31% of formal removals (not 20%) had convictions;
- about 170,000 (17%) of the 980,000 were ICE voluntary departures, which the segment calls "not
  deportation";
- DHS's "3 million" is about 1M deportations plus 2M self-departures;
- the FY2000 comparison sets a single year against 18 months.

It graded "tenfold" and Rosenblum's "explicit strategy" as not checkable, which is correct.

**`askamerica`** (engine: "too few to rate"; earlier self-assigned 1 Pinocchio withdrawn) also read
MPI's fact sheet in full; everyman got a 403 there. It surfaced MPI's own like-for-like figure:
about 480,000 deportations of people in the U.S. a year or more, excluding about 340,000 border
turnarounds. Its claims were correctly grouped after one resume (fidelity, author_claims, and
subject_claims for Rosenblum). It added warehouse context from TRAC detention data
(`crime.ice_detention_population_by_criminality`: 19,329 of 65,765 detainees, 29%, convicted
on 2026-07-11), correctly labeled as a different measure. Two grades are too strong: "tenfold" and
Rosenblum's "explicit government strategy" are graded `unsupported` because they are not in the
fact sheet. The quote most likely comes from his interview, which was not checked, so both should be
`not checkable here`, as expert graded them. That is the #1102 pattern in milder form; it doesn't change the rank.

**`everyman`** (1 Pinocchio) reached the same findings from secondary coverage. MPI's pages
returned 403 and its PDF was unreadable, so it did not read the source itself.

## Severity check

`askamerica` (#2) ranked above `everyman` (#3), so no severity flag. Ranking below `expert` is normal.

## Fix verification on this build

- **#1101: still bypassed.** The first publish had neither `claims` nor `source_url`, and the
  persona said so: "I did not use the claims array." That is 8 of 8 today. Fixed on resume, after
  which the engine returned "too few to rate".
- **#1102: partly followed.** It used `not checkable here` for the Fifth Circuit opinion it could
  not read, but graded two claims `unsupported` because they are absent from one source, which is
  not contrary evidence.
- **Harness note:** on each resume the MCP server restarts and loses the session's call history,
  so the report QC demands fresh evidence calls (about 10-15 extra tool calls per resume).

## Gap audit

- **Filed govdata-ops #1107** (type:sourcing, kind:gap, schema:crime). There is no DHS
  removals/returns series. The OHSS Yearbook enforcement tables are public through FY2024, and
  monthly releases stopped after January 2025. #754 added only detention snapshots.
- The court ruling, the MPI modeled estimates and DHS's press claims are not government
  statistical tables, so there is nothing to file for them.
