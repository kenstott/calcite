# Judge: Justice in Aging, "From DC: Medicare Eligibility Changes, Reduced Dental Benefits, Fair Housing Trends, and more!" (q11), 3-persona 2026-11-09

Rank: [expert, everyman, askamerica]

Source: https://justiceinaging.org/from-dc-11062026/ (Nov. 6, 2026; advocacy newsletter, no byline).

**Engine build:** the private dev jar (0af22733bd). **Data server:** `~/.askamerica/pgwire-dev` on port 45533.

**Severity: askamerica ranked below everyman. This is a defect under the standing invariant.**

## Judge's own verification

My search did not surface the final rule text, the 300,000 figure or the 32,000 actuary estimate, so I could not
confirm any of them. Advocacy fact sheets confirm termination on Feb. 1, 2027, with Jan. 4, 2027 as the statutory date
others cite, and SSA's two-notice process under the proposed rule. No arm read the final rule.

## Why

**`expert` is first.** It explained the number most likely to mislead: about 300,000 notices against about 32,000
actual coverage losses (the CMS Actuary, per its reading; I could not confirm it). It also reported that Humana raised
dental allowances, so "insurers" is not uniform, that the sources disagree on the termination date, and that the
"more than 75%" share is unconfirmed. It listed what it could not open.

**`everyman` is second.** Working from a summary, it confirmed the final rule's Nov. 4 announcement, the affected
groups, the NFHA funding findings and the date conflict, and called the 300,000 unsupported. It added that age is not
itself a protected class under the Fair Housing Act.

**`askamerica` is third.** No warehouse table was used for any claim, so the connector added nothing here. It graded
the final rule from the *proposed* rule on the advocacy group's own site, never searched the Federal Register tables
though the article links the rule, and folded the 300,000 into a "mostly true" claim. It bundled "more than 75% of
complaints" into a "mostly true" claim while saying it could not find that figure. Its note that an earlier NFHA
figure ties about 75% to FHIP grantees is useful, and it gave a 32,000 to 300,000 range, but it graded five bundled
claims against the others' finer breakdown. It also could not read the article (HTTP 412).

## Severity invariant

`askamerica` ranked below `everyman`. Flagged.

## Gate check

The QC gate refused one `deliver_report`. None of the claim gates fired. A finding about the gates: a failed
`web_fetch` is returned as text and logged with no error, so the office-page and named-subject gates can count a
page that never opened. See the run report.

## Recipe check

Third instance today of bundling several claims into one verdict (q8 most clearly). Not changed in this run.
