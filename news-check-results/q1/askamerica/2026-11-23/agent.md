**The American Experiment article's claims about Minnesota's median household income growth rank and per capita GDP are accurate; 1 claim could not be confirmed.**

Article: https://www.americanexperiment.org/since-2014-minnesotas-median-household-income-growth-ranks-49th-out-of-50-states/

**Rating: 0 Pinocchios**, from the engine's claim scores. This describes this article only.

**Claim results** (6 true, 1 unsupported, 1 source-fidelity claim true)
1. GDP per capita in Minnesota was $4,658 above the US in 2014, or $18,632 for a family of four. TRUE. I divided BEA real GDP (econ.state_gdp SAGDP9) by BEA population (econ.regional_income SAINC1) and got $4,658. Times four is $18,631.
2. The premium fell every year but one until it vanished in 2023. TRUE. The gap fell each year except 2022 ($656 to $742) and went to -$312 in 2023. The warehouse population series starts in 2007, so "first time on record" is confirmed only back to 2007.
3. In 2025 Minnesota was $2,767 below the US, or $11,068 for a family of four. TRUE. I got -$2,767, and -$11,070 for a family of four.
4. Per capita GDP growth from 2014 to 2025 ranks 46th of 50. TRUE. Minnesota grew 8.8%, which is 46th. North Dakota is 49th and Wyoming 50th.
5. Per capita GDP growth from 1997 to 2014 ranks 17th. UNSUPPORTED. The warehouse population series starts in 2007, and the BEA page I fetched has no 1997 per capita rank.
6. Median household income growth from 1997 to 2014 ranks 16th. TRUE. On Census CPS ASEC Table H-8 in 2025 dollars, Minnesota is up 11.1%, which is 16th of 50.
7. Median household income growth from 2014 to 2025 ranks 49th (the central claim). TRUE on the article's apparent source. On CPS H-8, Minnesota went from $88,680 to $95,160 (+7.3% real). Only North Dakota is lower.
8. The article says it found a statistically significant link between per capita GDP growth and income growth. TRUE as reported. The linked post gives r=0.557 and R²=0.31 across 50 states for 1997 to 2025. I read the post but did not re-estimate it. It shows an association, not causation.

**Misleading framing, even though the facts check out**
- **The rank comes from one noisy survey.** The CPS state medians have large errors. Minnesota's 2025 estimate is $95,160 with a standard error of $5,191, against $91,890 (SE $2,013) in 2024. The warehouse's ACS 1-year series (census.acs1_income) puts Minnesota 38th of 50 for 2014 to 2025, not 49th. The article does not disclose this.
- **The start year is favorable to the thesis.** 2014 is the peak of Minnesota's per capita GDP premium, and the article does not say why it chose that year.
- **Figure 2 is credited only to "Census Bureau."** No table is named. The related post links H-8.
- **The correlation is used as support for a causal point.** The linked post uses the r=0.557 association to suggest that boosting GDP growth would boost incomes.

**Method and exclusions**
- I excluded the District of Columbia from all rankings, because the article ranks 50 states.
- I excluded the ACS national row (null state) and the US row from the state rankings.
- Including Puerto Rico in the ACS list moves Minnesota from 38th to 39th.
- One query predicate on Alabama and Alaska only re-added them and removed no one.
- Table H-8 is read from Census's own file (www2.census.gov). The ACS and BEA figures are from warehouse queries.

Report page (works only while the engine process runs): http://127.0.0.1:53961/a/a46c5d8d36adf89627028a13c52858d8.html
Saved copy: /Users/kennethstott/.askamerica/reports/20261123-070757-the-american-experiment-article-s-claims.html

I can publish this for a permanent link if you want.