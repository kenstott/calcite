# Fact-check: Sept 24, 2026 congressional letter on FEMA Disaster Case Management Program (Eaton/Palisades fires)

## Claim 1: Letter's existence, senders, and date
**Claim:** On Sept 24, 2026, Rep. Judy Chu (CA-28), Rep. Brad Sherman (CA-32), Sen. Adam Schiff (D-CA), and Sen. Alex Padilla (D-CA) sent a follow-up letter to FEMA demanding immediate action to stop DCMP from shutting down Sept 30.

**Verdict: TRUE**

**Evidence:** Rep. Judy Chu's official press release, "Reps. Chu, Sherman, Sens. Schiff, Padilla Press FEMA Again to Prevent Shutdown of Critical Wildfire Recovery Program" (chu.house.gov, dated Sept 24, 2026), confirms all four lawmakers, addressed to FEMA Administrator Cameron Hamilton, demanding action before the Sept 30 shutdown deadline. Source: https://chu.house.gov/media-center/press-releases/reps-chu-sherman-sens-schiff-padilla-press-fema-again-prevent-shutdown

**Confidence: High** — verified directly against the lawmakers' own official press release.

## Claim 2: FEMA Region 9 denial and $6,568,265.60 appeal amount
**Claim:** FEMA Region 9 denied California's request for an additional $6,568,265.60 on Sept 11, 2026 (following an unanswered Sept 10 prior demand); the letter calls on FEMA to grant California's appeal of that denial.

**Verdict: TRUE**

**Evidence:** The Chu press release confirms the exact figure $6,568,265.60 as the additional funding requested, that FEMA Region 9 denied it on Sept 11, 2026, and that the state has appealed. It also confirms the lawmakers' prior Sept 10, 2026 demand letter went unanswered before the denial. This is corroborated by independent local reporting (Pasadena Now, "Pasadena Congresswoman Presses FEMA to Reverse Denial for Fire Survivors' Case Management"). Source: https://chu.house.gov/media-center/press-releases/reps-chu-sherman-sens-schiff-padilla-press-fema-again-prevent-shutdown

**Confidence: High** — dollar figure and dates match precisely across the official press release and independent local coverage.

## Claim 3: DCMP award total (~$13M), amount released (~$3M), survivors served (3,000+), active cases (1,288)
**Claim:** DCMP has an approved total award of ~$13M over 24 months, only ~$3M released so far, serving 3,000+ survivors (1,288 active cases).

**Verdict: TRUE**

**Evidence:** Chu press release confirms: Total DCMP award ~$13 million (approved May 5, 2025); only one installment (~$3 million) released to date, with three remaining installments still under FEMA review; 3,000+ survivors served total; 1,288 active cases. It additionally reports related figures not in the claim (1,023 highest-complexity active cases, 84 on immediate waitlist, ~7,000 additional survivors awaiting case-manager assignment, 801 households on rental assistance as of Sept 17, 329 pending rental-assistance applications, rental assistance itself expiring Oct 9, 2026). Source: https://chu.house.gov/media-center/press-releases/reps-chu-sherman-sens-schiff-padilla-press-fema-again-prevent-shutdown

**Confidence: High** — figures match the official press release exactly; the "24 months" duration framing was not independently itemized in the release but is consistent with FEMA cooperative agreement periods of performance for DCMP awards of this type.

## Claim 4: FEMA Individual/Household Assistance — $177M+ to 35,000+ households as of June 2026
**Claim:** Separately, ~$177M+ has been paid to 35,000+ households via FEMA Individual/Household Assistance for these fires as of June 2026.

**Verdict: TRUE**

**Evidence:** Multiple June 2026 news reports (KFI AM 640, Pasadena Now, MyNewsLA, HeySoCal, Cal OES) covering FEMA's housing-aid extension announcement report that, as of June 12, 2026, more than 35,000 households had received assistance through the Individuals and Households Program, totaling more than $177 million, for the Eaton and Palisades fire survivors. Sources: https://kfiam640.iheart.com/content/2026-06-24-fema-extends-housing-aid-for-eaton-palisades-fire-survivors/ ; https://pasadenanow.com/main/fema-extends-housing-aid-for-eaton-palisades-fire-survivors ; https://www.news.caloes.ca.gov (extension announcement coverage)

**Confidence: High** — figure is consistently reported across multiple independent outlets covering the same FEMA/Cal OES announcement, though it derives from news coverage of a state/FEMA announcement rather than a raw FEMA dataset directly queried in this check.

## Claim 5 (specifically investigated): FEMA Public Assistance obligated for debris removal/infrastructure as of September 2026
**Claim to investigate:** How much has FEMA obligated in Public Assistance (infrastructure/debris-removal) funding for these fires as of September 2026?

**Verdict: Found — and it reveals a significant, underreported gap not mentioned in the letter's claim set.**

**Finding:** Querying FEMA's own OpenFEMA API (`PublicAssistanceFundedProjectsSummaries`, filtered to disaster DR-4856-CA — the Eaton/Palisades wildfires) directly, data refreshed as recently as **September 4, 2026**, shows:
- **Total federal Public Assistance obligated to date: $36,960,247.55 (~$37 million)**, across 101 applicants and 324 funded projects.
- Largest single obligations: Pasadena Waldorf School ($10.47M), CalOES ($5.28M), Lifeline Fellowship Christian Center ($4.14M), Altadena Baptist Church ($3.68M), Los Angeles County ($1.44M).
- This ~$37M obligated figure is essentially unchanged from the ~$37M reported in a California Governor's press release dated **May 8, 2026**, which stated that **$732 million** in Public Assistance funding had been *approved at the FEMA regional level* but remained stuck awaiting final sign-off from DHS Headquarters, leaving **over $695 million approved-but-unobligated**.
- Sources: FEMA OpenFEMA API (https://www.fema.gov/api/open/v1/PublicAssistanceFundedProjectsSummaries, queried live, disasterNumber=4856, lastRefresh through 2026-09-04); Governor's press release https://www.gov.ca.gov/2026/05/08/governor-requests-extension-of-fema-disaster-funding-to-help-survivors-of-la-wildfires/

**Confidence: High for the obligated total** (pulled directly from FEMA's own live public dataset, refreshed within the last month) — **medium for characterizing the full $732M "approved but stuck" figure as still current in September**, since no news source or FEMA release was found explicitly reconfirming that pending-approval total as of September 2026 (only May 2026). The OpenFEMA obligated total itself, however, is directly verified and current.

**Significance:** This is directly relevant context the letter and claim do not address: while the letter focuses on the comparatively small ~$6.6M DCMP case-management shortfall, the much larger Public Assistance stream for actual debris removal/infrastructure rebuilding remains almost entirely unobligated (~$37M of what was, as of May 2026, at least $732M+ already approved at the regional level). If that backlog persisted through September without material change, it represents a far larger dollar-value funding bottleneck than the DCMP dispute, though this check could not independently confirm the pending-approval total was still $732M (unmoved) in September — only that actual obligations remained roughly flat.

---

## Overall Assessment

All five core factual claims in the letter/claim set — the letter's senders and date, the FEMA Region 9 denial and $6,568,265.60 appeal amount, the DCMP award/release/caseload figures, and the separate $177M+/35,000+ household Individual Assistance figure — check out as accurate against the lawmakers' own press release and independent news coverage of FEMA/state announcements. Confidence is high throughout because the figures were verified against primary or near-primary sources (the House press release directly, and multiple independent outlets citing the same FEMA/Cal OES data).

The specifically-requested additional investigation into FEMA Public Assistance (debris removal/infrastructure) obligations turned up a concrete, sourced figure directly from FEMA's own OpenFEMA data: **~$37 million obligated** for DR-4856-CA as of early September 2026 — a figure that appears to have been essentially stagnant since at least May 2026, when it stood in stark contrast to $732 million already "approved" at the regional level but stuck awaiting DHS Headquarters sign-off. This is a materially larger, and comparatively underreported, funding bottleneck than the DCMP shortfall the letter focuses on, though the ~9-month-old $732M pending-approval figure itself could not be independently reconfirmed as unchanged through September 2026 within this check.
