# Judge: The 51st "D.C. evictions hit 12-year high as safety net shrinks and cost of living soars" fact-check (q3), 3-persona 2026-11-07

Rank: [askamerica, expert, everyman]

Source: https://51st.news/dc-record-evictions-housing-crisis (James Jarvis, The 51st with Street Sense
Media and AU's Investigative Reporting Workshop, 2026-11-05). Third-party claims come from OTA eviction
data as analyzed by the outlets, January Advisors, CLCPP / DC Bar Foundation, DCFPI, D.C. Superior
Court, DCHFA, EPI, and interviews with Legal Aid DC, NLSP, Bread for the City, SMOA and a councilmember.

**Engine build:** main at 7f1fa47bb6, installed 2026-11-08, cksum 3275203871. It includes all
gates, the claims-coverage gate and the context-aware figure gate among them. This is run 10.
Runs 6-9 ranked 1st, 2nd, 1st and 1st.

## Judge's own verification

- **The article links two CLCPP reports, and its figures come from both.** I downloaded and read both
  PDFs.
  - **Jul–Dec 2025 report:** carries the 77% possession rate and the 73% ERAP share for Jan 2022–Dec
    2024.
  - **Jan–Jun 2026 report:** "Among the 508 CLCPP cases closed in the current reporting period where
    the outcomes are known by the attorney, tenants retained possession of the unit in 319 cases
    (63%)"; ERAP was used in 32% of dismissals for paid rent; 1,487 tenants were served, and the
    landlord was represented in 1,363 (92%); clients were 81% Black and 67% women.
  - **What that means for the article.** Its "first half of 2026" figures are correctly dated. Its
    63% covers all 508 closed cases with known outcomes, not only tenants represented for their whole
    case, as the article frames it. It gives 93% (the cumulative figure) where the period figure is
    92%, and 82% where the report says 81%.
- **Rent burden.** `census.acs_congressional_district`, D.C. at-large district, ACS 2024: 46.6% of
  renter households whose burden is computed; 44.3% of all renter households.
- **D.C. unemployment.** `econ.regional_employment`: 5.9% in July 2026, 5.7% in August, 5.6% in
  September. "Rose to 5.9% in August" is wrong on both the month and the direction.

## Why

**`askamerica`** (1 Pinocchio) graded 15 claims and listed the anecdote figures (Pleasant's rent,
salary and case amount; the landlord's "$25,000 or $50,000") as unclaimed, each with a reason.

From the warehouse:
- **CPI:** +4.12% and +27.6%.
- **Unemployment:** August 5.7%, national 4.1%. Graded mostly true, which is lenient: the
  article's "rose to 5.9% in August" is wrong on the month and the direction, and earlier runs
  graded it partially false.
- **Rent burden:** 46.6%.

From primary documents:
- **Filings:** 13,051.
- **ERAP:** $7M and more than $20M in under six hours.
- **CLCPP Jan–Jun 2026:** 73% to 32%, 1,487, and 12%/17% representation confirmed.
  - Caught 92% versus 93% and 81% versus 82%.
  - Graded the 77%-to-63% claim partially false for misdescribing the population, and answered the
    figure gate with `why_not` saying so.
  - Matched the nonpayment share to CLCPP's 81%.
- **HFA arrears:** found that the 2024 figure only echoes the outlet's own earlier story.

Its gaps:
- It called "$60M in 2024" consistent via UPO, while the outlet's own earlier story puts the peak
  in 2023.
- It did not reconcile 2,420 with New America's 1,933.

It makes no false accusation against the article.

**`expert`** (3 Pinocchios) found:
- the count conflict with New America and UPO
- the ~1,600 pre-pandemic baseline against FY2019's 1,493
- ERAP's peak being in 2023, not 2024
- the unemployment error

But it wrongly called the CLCPP figures "misattributed and mis-dated." It checked them only against
the Jul–Dec 2025 report, missing the Jan–Jun 2026 report the article also links. Two of its five
"materially misleading" items rest on that.

**`everyman`** (2 Pinocchios) found the count conflict and confirmed ERAP, Ward 8, the RENTAL Act
and the 2022 law, without checking CPI, unemployment, rent burden, filings or CLCPP.

**Ranking:**
- `askamerica` first: the widest verified coverage of any persona (15 graded claims), the CLCPP
  figures read from the right report, and no errors. It is lenient on unemployment and missed the
  ERAP peak year.
- `expert` second: a firm false CLCPP finding.
- `everyman` third.

## Severity check

`askamerica` ranked #1, so no severity flag.

## Gate check

- **Claims-coverage gate.** It refused the first full attempt: 22 of the article's figures had no
  claim. The run went from 6 to 15 claims and excused the anecdote figures in `unclaimed`, shown in
  the report's "Figures in the article not graded as claims" section.
- **Figure gate.** It caught the CLCPP claim graded partially false whose 63% is in the Jan–Jun 2026
  report. The run kept the grade with a correct `why_not` (population misdescribed).
- **Referenced-sources gate.** It refused once; all 35 links were then fetched.

## Gap audit

- D.C. eviction records (OTA completed evictions, Superior Court filings) are local administrative
  data, not a federal statistical table, so this is out of scope for the corpus and I filed nothing.
- CPI, unemployment and rent burden are all present, and all three were found.
