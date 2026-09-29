# Fact-check: Sept. 24, 2026 Chu/Sherman/Schiff/Padilla letter to FEMA on DCMP

## Verdict: Pinocchios — Fidelity 0/4, Claims Accuracy 2/4

**Fidelity (to the lawmakers' own letter): 0 of 4 Pinocchios.** The DCMP-specific portion of the claim — letter date and senders, the exact $6,568,265.60 denial figure, the Sept 11 and Sept 10, 2026 dates, the ~$13M/24-month award with only ~$3M released, and 3,000+ survivors/1,288 active cases — matches the lawmakers' actual letter essentially word for word.

**Claims Accuracy: 2 of 4 Pinocchios.** Two of three factual clusters are exactly right; one ($2.5B+ pending FEMA public-assistance) is a real number attached to the wrong program.

## What checked out (true)

1. **DCMP letter/program details** — confirmed verbatim against Rep. Judy Chu's Sept 24, 2026 press release, which reproduces the full letter text, and independently corroborated by Pasadena Now (Sept 27, 2026). AskAmerica's `disasters.disaster_declarations` table confirms the governing FEMA declarations (DR-4856-CA and FM-5549/5550/5551 for Palisades/Eaton/Hurst) but carries no DCMP case-management program data itself — that detail is outside the warehouse and was verified against primary sources.

2. **$177M+ paid to 35,000+ households via IHP as of June 2026** — confirmed verbatim: Rep. Chu's June 24, 2026 press release states "As of June 12, 2026, FEMA reported that more than 35,000 households have received assistance through the Individuals and Households Program, with more than $177 million awarded to eligible survivors." Not carried in AskAmerica's corpus (only a boolean `ih_program_declared` flag exists, no dollar/household figures for IHP) — a genuine, disclosed sourcing gap, verified externally instead.

## What did NOT check out (false)

3. **"$2.5B+ in FEMA public-assistance projects remain pending as of September 2026"** — **false**. AskAmerica's `disasters.public_assistance_projects` table (queried live) shows only ~$45.2M in total project amount and ~$36.6M federal obligated across all 318 funded-project records for DR-4856-CA, nowhere near $2.5B, which prompted deeper independent checking. The California Governor's Office (gov.ca.gov, May 8, 2026) states the actual pending Public Assistance backlog is **$732 million** — "approved at the regional level but still awaiting final sign-off from DHS Headquarters" — with only $37 million obligated (closely matching the warehouse's own $36.6M, cross-validating both figures). No source anywhere (FEMA.gov, CA Governor's Office, congressional offices, or news coverage from May–September 2026) ties a $2.5 billion figure to FEMA Public Assistance for these fires. The only $2.5B figure found in coverage of these fires refers to Governor Newsom's separate, state-administered wildfire relief fund — a different funding stream entirely from FEMA Public Assistance. The documented PA-pending figure is roughly 3.4x smaller than claimed and about four months older than the claimed September 2026 vintage.

## Sources
- [Reps. Chu, Sherman, Sens. Schiff, Padilla Press FEMA Again to Prevent Shutdown of Critical Wildfire Recovery Program](https://chu.house.gov/media-center/press-releases/reps-chu-sherman-sens-schiff-padilla-press-fema-again-prevent-shutdown) (Sept 24, 2026, full letter text)
- [Eaton Fire Case Management Still Faces Sept. 30 End – Pasadena Now](https://pasadenanow.com/main/eaton-fire-case-management-still-faces-sept-30-end) (Sept 27, 2026)
- [Rep. Chu Commends FEMA Extension of Financial and Housing Assistance for Eaton Fire Survivors](https://chu.house.gov/media-center/press-releases/rep-chu-commends-fema-extension-financial-and-housing-assistance-eaton) (June 24, 2026)
- [Governor requests extension of FEMA disaster funding to help survivors of LA wildfires — gov.ca.gov](https://www.gov.ca.gov/2026/05/08/governor-requests-extension-of-fema-disaster-funding-to-help-survivors-of-la-wildfires/) (May 8, 2026)
- [Victims get little relief from Newsom's $2.5 billion fire fund — NBC Los Angeles](https://www.nbclosangeles.com/investigations/wildfire-relief-fund-california-eaton-palisades-fire/3896539/)
- [LA Officials Urge FEMA to Release Funds for Wildfire Recovery — MyNewsLA](https://mynewsla.com/weather/2026/09/02/la-officials-urge-fema-to-release-funds-for-wildfire-recovery/) (Sept 2, 2026)

## AskAmerica warehouse queries used
- `disasters.disaster_declarations` — identified DR-4856-CA, FM-5549/5550/5551 for the Jan 2025 CA fires
- `disasters.public_assistance_projects` — DR-4856-CA totals: 318 records, ~$45.2M project amount, ~$36.6M federal obligated, all "Active" status (cross-validated against CA Governor's Office's independently reported $37M obligated figure)

## Deliverables
- Published report (dashboard + claims table + Pinocchio banners): http://127.0.0.1:50458/a/7b950d824a87926d3970ecb9c6b73ede.html — **local/ephemeral, dies when this session ends**
- Saved copy: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q5/askamerica/2026-09-29/report.html`
