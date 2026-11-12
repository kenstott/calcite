# Judge: WNEM "Michigan violent crime hits seven-year low" fact-check (q2), 3-persona 2026-11-06

Rank: [expert, askamerica, everyman]

Source: https://www.wnem.com/2026/10/26/michigan-violent-crime-hits-seven-year-low-governors-office-says/
(published 2026-10-26; relays Gov. Whitmer's office release of the same day).

## Why

**`expert`** and **`askamerica`** reached the same verdict (1 Pinocchio) by the same route: both
checked the three headline claims against FBI Crime Data Explorer data, and both added a national
comparison that undercuts the article's crediting of state spending. Their figures agree: murder
7.7 → 4.5 per 100k 2021-2025 (−41/42%) against −38% nationally; robbery −50% from 2019 (askamerica)
or −52% from 2018 (expert). The ordering between them is narrow.

**`expert`** edges it on analytical rigor about the comparison itself:
- It flagged that robbery was already falling about 8% a year before 2019, so "since she took office"
  borrows a pre-existing trend.
- It noted that aggravated assault, the largest violent-crime component, goes unmentioned.
- It named the 2021 NIBRS transition and agency-coverage changes as a data seam.
- It reported counts alongside rates (murders 759 → 442), plus FBI coverage (about 97% of
  Michigan's population).

**`askamerica`** read the governor's release directly. Expert got a 403 there and worked from WXYZ's
copy. From the release, askamerica produced a source audit that expert lacks: the article says
felony check-ins recovered "drugs and weapons" where the release says drugs and ammunition. Its
claims were grouped correctly after one resume (subject_claims with speaker for the governor's
office and Tony Gant, fidelity for relayed figures), and it reported the engine's per-speaker
honesty and bias scores as returned. Warehouse figures spot-checked by the judge against
`crime.cde_offenses`: 12 monthly rows for every complete year, MI violent crime 427.2 (2025),
homicide 7.71 → 4.54, robbery 54.36 → 27.24, national homicide 6.92 → 4.29, national robbery
82.5 → 49.95. All match its report.

**`everyman`** could not reach FBI or MSP statewide data. It graded the three headline claims as
unverified or only partly corroborated (from Detroit city data) and gave 2 Pinocchios. That is the
wrong grade: all three claims hold up against the federal data the other two personas reached.

## Severity check

`askamerica` (#2) ranked above `everyman` (#3), so no severity flag. Ranking below `expert` is normal.

## Problems in the askamerica answer

1. **Missed a checkable claim.** It graded "nearly a third of violent crimes involve a firearm" as not
   checkable, but `crime.cde_reta` (FBI Return A) carries `actual_robbery_with_a_gun` and
   `actual_assault_with_a_gun`. Robbery and aggravated assault make up the bulk of violent crime, so
   a firearm share is computable for those offenses. Its only `search_catalog` call was
   "FBI violent crime offenses by state", and it never searched for firearm or weapon data. First
   instance of this method miss: logged here, not a recipe yet.
2. **First publish had no `claims` array.** It was fixed on resume, but the resume needed fresh evidence
   calls before the validator accepted it. This is the second of two dispatches today with this
   failure (q1 was the first), which makes it a pattern; see the recipe check below.

## Recipe check

No new recipe for the firearm miss (first instance). The missing-`claims` pattern (2 of 2 today) is
a tool-behavior problem, not a recipe problem. Rule 7 of the server's validation instructions asks
the model to notice and republish on its own, but nothing in `preview_report` or
`create_report_artifact` refuses a validation report (one with an article being checked) that carries
no claims. Candidate resolution: a server-side refusal in McpServer's report QC, the same mechanism
that already refuses a missing `question_coverage`. Not changed during this local test run.

## Gap audit

- The seized-gun count, felony check-ins, the "nearly $2 billion" and the 20% trooper raise are
  program figures, published only by the governor's office. There is no public dataset to source,
  so nothing to file.
- The firearm share is not a gap; the data is in `crime.cde_reta` (see problem 1).
