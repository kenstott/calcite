# Judge — news q2 hechinger-free-tuition-promises-sticker-price

**Story:** With free-tuition promises, some colleges try to untangle confusion over what they cost (Hechinger Report, 2026-11-13)

Rank: [askamerica, expert, everyman]

Judged 2026-11-15 on three answers produced the same day.

## Why
- **askamerica (1st):** engine score 0; its own call was 1. Read the article in full. Confirmed 14 of 19 income thresholds on the colleges' own pages and named the five it could not load. Checked "$175,000 is more than double median household income" against `census.acs1_income` ($84,780, 2.06x) and against the article's own linked figure ($87,460, 2.0008x). Traced each survey figure to its source and caught three paraphrase shifts: Lumina-Gallup asked about net cost at in-state publics, Gallup's 63% is a rating of colleges, and Gallup does not name confusion as a cause of low confidence. Flagged that the same Sallie Mae survey has 39% self-reporting full price.
- **expert (2nd):** 1 Pinocchio. Read a summary, not the text. Confirmed Middlebury, MIT, Harvard, Sallie Mae; found a $3,000 gap between Middlebury's stated total and its components that no other arm saw. Nine items left unchecked, including three thresholds askamerica confirmed.
- **everyman (3rd):** provisional 1 Pinocchio, three searches, about half the claims unchecked.

## Recipe check
No recipe applies.

## Resolution
None required (askamerica ranked first). Gap filed anyway: kenstott/govdata-ops#1230, IPEDS Student Financial Aid, which would test the "445 colleges" and "fewer than 1 in 5" claims.

## Severity invariant
Not triggered.

## Shape
Split used: 11 author claims, 1 subject claim (Levine).
