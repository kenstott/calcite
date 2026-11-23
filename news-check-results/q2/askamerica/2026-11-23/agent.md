# The Willamette Week article's claims about Portland person crime, Oregon FBI crime statistics and property-crime trends are somewhat biased and mostly accurate; 7 claims could not be confirmed.

Article: https://www.wweek.com/news/2026/11/19/violent-crime-rose-in-portland-over-past-two-years/
Rating: **1 Pinocchio** (engine rating from claim scores).
Report: http://127.0.0.1:49447/a/b077e1f895f51e6e232f90c3909bbb75.html (report_id 20261123-065158-the-willamette-week-article-s-claims-abo)

## Verdict
The statewide FBI figures are accurate. The headline "Violent Crime Rises in Portland" overreaches, and one figure is mislabeled.

**Checked out (warehouse crime.cde_offenses and crime.cde_reta, plus KATU's report of the FBI release):**
- Oregon violent crime: FBI -7.9%; warehouse rate -7.3%.
- Oregon homicide: FBI -24.4%; warehouse -24.3%.
- Oregon property crime: FBI -12.1%; warehouse -11.4%.
- Oregon burglary: FBI -20.4%; warehouse -20.0%.
- Portland PD vehicle thefts fell 5,492 to 4,004, which matches "nearly 1,500 fewer".
- The Jan-Aug 2026 (-16%) and August 2026 (about -50%) statewide figures match the FBI release as KATU reported it. The warehouse was not tested for 2026.

**Problems:**
1. **Misattribution (mostly false).** WW says "Statewide... approximately 2,600 fewer property offenses including around 1,500 fewer motor vehicle thefts." The FBI said these are Portland figures. A statewide 12.1% drop from about 90,500 property crimes is roughly 12,000 fewer offenses.
2. **Misleading headline and lede.** The body reports Portland Police Bureau "person crime" (NIBRS). That category includes simple assault and intimidation, which the article says are 57% of the category and the main driver.
   - Aggravated assault is up only 1% (14 offenses) and homicide is down.
   - FBI Return A for Portland PD (ORI OR0260200) shows violent crime (murder, rape, robbery, aggravated assault) fell about 3.8% in 2025 versus 2024: 4,558 to 4,386.
   - In the same data, simple assault rose 8.6% (5,821 to 6,322) and aggravated assault rose 0.8% (3,045 to 3,068).
3. **Mixed bases.** The state figures use FBI UCR definitions for 2025 and Jan-Aug 2026. The city figures use PPB's broader category, year to date through October. The "despite state trends" contrast therefore compares different measures.
4. **Unverifiable.** The central claim (person crime +11% since 2024, +6% in 2026) and the related figures (412, 92, 57%, 28%, 14%, property crime -4% then +1 point) rest on PPB's interactive tracker.
   - The tracker is not in the warehouse. The portland.gov page I fetched describes it but carries no figures.
   - No 2026 city data exists in the warehouse.
   - 2025 Portland PD data are directionally consistent: simple assault rose and homicide fell (murders 75, 68, 51 for 2023-2025).
   - The spokesperson paraphrase appears only in the WW piece.
   - All of these are graded unsupported, not false.

## Sourcing notes
- The FBI release itself was not fetched. Its figures were read through KATU (https://katu.com/news/local/oregon-violent-crime-vehicle-thefts-statistics-fbi).
- Return A data are subject to revision and use different definitions from PPB.
- The WW article loaded without a block.
