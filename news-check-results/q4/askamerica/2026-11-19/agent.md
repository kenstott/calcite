**The yourNEWS (Stateline) article's claims about Gallup crime perceptions, FBI 2025 crime data and scam worries are accurate.**

**Rating: 0 Pinocchios** (engine-computed from the claim scores).

The yourNEWS page loaded directly. It is a republication of a Stateline story, and both of its linked Stateline pages returned HTTP 403. I read the Gallup poll directly. I checked the FBI figures against a Register-Herald copy of the Stateline FBI story and against R Street. I checked the NCVS figures against CBS News and KCCI reports of the BJS release. I did not read the BJS release itself.

**Claims checked**
- **Gallup perceptions:** 46% say national crime rose, 49% last year, 77% in 2023, and only 2001 (41%) was lower. Local crime is 33%, against a 2022 record of 56%. Republicans' perception of rising national crime fell another 15 points. All true against Gallup, including the Oct. 1-19 field dates.
- **Worry figures (central claim):** 70% worry about identity theft and 55% about scams, the only two majorities among 10 crimes. Vehicle theft and a child harmed at school are 41% each, burglary 37%, murder 22%. The subgroup splits (77% vs 52%, 32% vs 13%, 40% vs 13%) also match. All true.
- **FBI violent crime:** down 9.3% in 2025, the largest drop since 1936. True per the Stateline copy, Gallup and R Street.
  - **Warehouse check:** I used `crime.cde_offenses`, an unweighted average of state-month national rates. It gives a violent-crime rate down 9.8% and a homicide rate down 18.0%, consistent with the published figures. It is a proxy, not the FBI's own estimate.
  - **Rolling window:** `crime.cde_trends` covers a rolling Aug 2025 to Jul 2026 window, so it cannot test the calendar-2025 figures.
- **Murder rate:** mostly true. The 4.1 per 100,000 rate and the tie with 1955-56 match. The article says the rate fell 18.5%, but the Stateline-originated copy says 18.1%. The FBI's 18.5% is the robbery decline, so this looks like a transposition. The warehouse proxy (-18.0%) agrees with 18.1%.
- **NCVS, preliminary:** violent victimization excluding simple assault down 25%, aggravated assault 31%, robbery 27%. True per CBS on the BJS release, and KCCI confirms the 25%. The warehouse NCVS tables only run through 2024.

**Framing and omissions**
- "Increasingly optimistic" overstates the one-year move. Perceptions of rising crime went from 49% to 46%, within the poll's ±4-point margin, and the article itself says "little changed". The level is near the lowest since 2001, but "increasingly" is loose.
- The article does not say that FBI data rely on voluntary agency reporting. It also does not say the NCVS figures are preliminary, with a 53% household response rate per CBS, or that the Justice Department called the drop the largest since 1973. These are minor omissions.
- CBS calls 2025 a "20-year low" for murder, while Stateline, Gallup and R Street say tied for the lowest ever recorded. I did not resolve this, since I could not read the FBI release directly.

The saved report is at /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-11-19/report.html.