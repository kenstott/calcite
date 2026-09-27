# FBI Crime Data Fact Check — Per-Step Record

Question: Is it true that FBI data shows violent crime fell 9.3% from 2024 to 2025 (largest year-to-year decline since FBI estimations began in 1936), murder fell 18.1% with a 2025 murder rate of 4.1 per 100,000 (tied with 1955 and 1956 for the lowest rate since FBI estimations began), robbery fell 18.5%, aggravated assault fell 7.2%, and property crime fell 12.4%?

Date checked: 2026-09-26

---

## Step 1 — Initial web search for the claim

**Answer:** Multiple secondary sources (Axios, ABC News affiliate, NBC News, UPI, WDRB, PJ Media, California Globe, Just The News) independently report the same set of figures, all attributed to the FBI's August 2026 crime statistics release.

**Key Values:**
- Violent crime: -9.3% (2024→2025), largest decline since 1936
- Murder/nonnegligent manslaughter: -18.1%
- Robbery: -18.5%
- Aggravated assault: -7.2%
- Rape: -7.6% (not asked about in the question, but part of the same release)
- Property crime: -12.4%
- 2025 murder rate: 4.1 per 100,000, tied with 1955/1956

**Sources:**
- https://www.axios.com/2026/08/18/fbi-violent-crime-drop-2025
- https://www.upi.com/Top_News/US/2026/08/14/FBI-statistics-Violent-crime-murders-fell-record-levels-2025/1321786743909/
- https://www.nbcnews.com/politics/justice-department/fbi-reports-record-decrease-violent-crime-2025-rcna592593

**Confidence:** Medium (secondary sources only at this stage; all consistent with each other, which is itself informative, but not yet primary-source verified).

---

## Step 2 — Attempt direct fetch of FBI.gov primary press release

**Answer:** Direct automated fetch (WebFetch and curl with browser user-agent) of fbi.gov returned HTTP 403 Forbidden — fbi.gov blocks non-browser automated requests. Worked around by fetching the Wayback Machine archived snapshot instead (archived 2026-09-17).

**Key Values:** N/A (methodology note)

**Sources:**
- https://www.fbi.gov/news/press-releases/fbi-releases-2025-reported-crimes-in-the-nation-statistics (blocked direct; HTTP 403)
- http://web.archive.org/web/20260917193226/https://www.fbi.gov/news/press-releases/fbi-releases-2025-reported-crimes-in-the-nation-statistics (retrieved successfully via curl)

**Confidence:** N/A — process note, not a substantive finding.

---

## Step 3 — Extract text from archived FBI press release

**Answer:** The archived FBI.gov press release ("FBI Releases 2025 Reported Crimes in the Nation Statistics," dated August 14, 2026) states verbatim: "Data reported to the FBI's Uniform Crime Reporting Program shows 2024 to 2025 marked the largest year-to-year decline in violent crime rates since FBI estimations began in 1936. Following the May announcement that violent crime decreased 9.3% from 2024 to 2025... Murder and nonnegligent manslaughter offenses decreased an estimated 18.1%. Rape offenses decreased an estimated 7.6%. Aggravated assault figures decreased an estimated 7.2%. Robbery offenses decreased an estimated 18.5%. The 2025 murder rate of 4.1 per 100,000 inhabitants is tied with 1955 and 1956 for the lowest murder rate." This confirms every figure in the claim except property crime's specific 12.4% (the press release only says "Property crime and hate crime also declined in 2025" without the number).

**Key Values:** Violent crime -9.3%; murder -18.1%; rape -7.6%; aggravated assault -7.2%; robbery -18.5%; 2025 murder rate 4.1/100,000, tied 1955/1956; largest decline since 1936. n = "more than 17,000" agencies, 96.2% population coverage (per later detail in same page).

**Sources:**
- FBI.gov press release, "FBI Releases 2025 Reported Crimes in the Nation Statistics" (Aug. 14, 2026), archived copy.

**Confidence:** High — this is the FBI's own primary press release, directly quoted.

---

## Step 4 — Locate and fetch the FBI's detailed primary data report (PDF)

**Answer:** Found and downloaded the FBI's own "UCR Summary of Reported Crimes in the Nation, 2025" PDF directly from the FBI's Crime Data Explorer (cde.ucr.cjis.gov), released August 2026. This is the FBI's authoritative detailed statistical report and confirms every single figure in the question verbatim, including the property crime 12.4% figure that the press release omitted:

- "Violent crime decreased an estimated 9.3% from 2024 to 2025."
- "Property crime decreased an estimated 12.4% from 2024 to 2025."
- "Murder and nonnegligent manslaughter decreased an estimated 18.1%." / "Rape decreased an estimated 7.6%." / "Robbery decreased an estimated 18.5%." / "Aggravated assault decreased an estimated 7.2%."
- "Since the FBI began estimations in 1936, the United States has recorded its largest year-to-year decreases in both the estimated violent crime rate and the estimated murder and nonnegligent manslaughter rate. The violent crime rate declined by 9.7%, and the murder and nonnegligent manslaughter rate fell by 18.5%." (Note: these are *rate*-based percentages, slightly different from the *volume*-based 9.3%/18.1% figures quoted in the question — see caveat below.)
- "The 2025 estimated murder and nonnegligent manslaughter rate—4.1 per 100,000 inhabitants—matches the rates observed in 1955 and 1956, making it tied for lowest rates recorded since national estimates began."
- "Property crime decreased an estimated 12.4%. Burglary decreased an estimated 15.8%. Larceny-theft decreased an estimated 9.8%. Motor vehicle theft decreased an estimated 22.7%."
- Agency participation: "RCN, 2025" includes data from 17,075 law enforcement agencies (87.3% of enrolled agencies), covering 328,918,006 people (96.2% of U.S. population).
- 20-year murder rate table (2006–2025) confirms 2025's rate of 4.1 is the lowest in that window; prior 20-year low was 4.4 in 2014; rate peaked at 6.6 in 2020 and 2022.

**Key Values (all n=17,075 reporting agencies, 96.2% population coverage, CY2024 vs CY2025):**
| Metric | % change | Units |
|---|---|---|
| Violent crime | -9.3% | volume; rate change -9.7% |
| Murder/nonneg. manslaughter | -18.1% | volume; rate change -18.5% |
| Rape | -7.6% | volume |
| Robbery | -18.5% | volume |
| Aggravated assault | -7.2% | volume |
| Property crime | -12.4% | volume |
| Burglary | -15.8% | volume |
| Larceny-theft | -9.8% | volume |
| Motor vehicle theft | -22.7% | volume |
| 2025 murder rate | 4.1 | per 100,000 inhabitants |

**Sources:**
- FBI UCR Program, "UCR Summary of Reported Crimes in the Nation, 2025" PDF: https://cde.ucr.cjis.gov/LATEST/resources/reports/UCR_Summary_of_Reported_Crimes_in_the_Nation_2025.pdf (primary source, released August 2026)

**Confidence:** Very high — this is the FBI's own authoritative detailed data report, directly quoted and cross-referenced against its own tables.

---

## Step 5 — Methodology check (Summary Reporting System vs. NIBRS)

**Answer:** The report's "Differences in Crime Measures" section explains that these CIUS (Crime in the United States) estimates combine aggregated Summary Reporting System (SRS) data (only the single most serious offense per incident counted) with NIBRS incident data converted to SRS-equivalent format, to represent the full U.S. population. This is the FBI's traditional, decades-old estimation methodology (used since 1936) and is explicitly distinguished by the FBI itself from (a) newer "NIBRS-only" national estimates (first published 2021), (b) BJS's forthcoming "Crime Known to Law Enforcement, 2025" (also NIBRS-only, expected 2026), and (c) the victimization-survey-based NCVS. The FBI states these are "analogous measures rather than as one being more accurate than the other." No methodology mismatch was found — the question's figures are exactly the FBI's own headline CIUS-based numbers, the same ones used in the FBI's own press materials.

**Key Values:** N/A (methodology, not a statistic)

**Sources:**
- FBI UCR Program, "UCR Summary of Reported Crimes in the Nation, 2025" PDF, "Differences in Crime Measures" section (p. 23).

**Confidence:** High.

---

## Step 6 — Sanity check on the "1955/1956" and "since 1936" historical claims

**Answer:** Attempted to independently verify the 1955/1956 murder rate of 4.1 per 100,000 against a third-party historical archive (Disaster Center's UCR compilation), but that source's series begins in 1960 (rate 5.1), predating the years in question, so it could not independently confirm or refute the 1955/1956 figure. A web search corroborated the 4.1 rate for 1955–1956 as consistent with widely cited historical crime-rate compilations, and — more importantly — the FBI's own report treats this as an established fact drawn from its own 90-year estimation series (the same series the FBI is comparing 2025 against), which is the appropriate authority for a claim about "FBI estimations since 1936." The FBI's own 20-year table (2006-2025) is internally consistent with the "20-year low" framing used elsewhere in the same report. No independent primary source with pre-1960 UCR data was located to fully triangulate the exact 1955/1956 rate, so this specific historical figure rests on the FBI's own claim plus consistent secondary corroboration, rather than a third independent primary source.

**Key Values:** 1955/1956 murder rate: 4.1 per 100,000 (per FBI's own historical claim, corroborated by secondary reporting; not independently re-derived from a third pre-1960 primary archive in this check).

**Sources:**
- Disaster Center UCR archive (data begins 1960; did not cover 1955/1956): http://www.disastercenter.com/crime/uscrime.htm
- FBI UCR Program, "UCR Summary of Reported Crimes in the Nation, 2025" PDF (Director's message and Violent Crime Estimates section).

**Confidence:** Medium-high for the 1955/1956 figure specifically (FBI's own authoritative claim, not independently re-derived from a third pre-1960 source); high for everything else.

---

## Overall verdict

**TRUE.** All five percentage-change figures (violent crime -9.3%, murder -18.1%, robbery -18.5%, aggravated assault -7.2%, property crime -12.4%), the 2025 murder rate of 4.1 per 100,000, the "tied with 1955 and 1956" claim, and the "largest year-to-year decline since FBI estimations began in 1936" claim are all directly and verbatim confirmed by the FBI's own primary release, "UCR Summary of Reported Crimes in the Nation, 2025" (released August 2026), and corroborated by the FBI's own press release and multiple independent secondary news sources.

**One nuance flagged, not an error:** the FBI separately reports slightly larger *rate*-based percentage declines (violent crime rate -9.7%, murder rate -18.5%) alongside the *volume*-based declines (-9.3%, -18.1%) quoted in the question; the question uses the volume-based figures, which is also what the FBI's own press-release bullet points lead with, so this is not a discrepancy — just a detail to be aware of if a -9.7% or -18.5% figure is encountered elsewhere attached to "violent crime" broadly.
