**The Newsweek article's claims about SNAP enrollment, state declines and OBBBA work rules are balanced and partly false.**

**Rating: 1 Pinocchio.** (The article is at https://www.newsweek.com/snap-enrollment-drops-300k-single-month-here-are-worst-impacted-states-12466361.)

Source checked: fiscal.snap_benefits_by_geography (USDA FNS) in the warehouse. It is loaded through May 2026, and its June 2026 rows are null.

**What checks out**
- Arizona, Illinois, Texas and Florida tie to the person. Warehouse June 2025 minus the article's year-over-year decline equals warehouse May 2026 plus or minus the article's monthly change.
  - Arizona: 886,806 - 465,659 = 421,147 = 409,584 + 11,563.
  - Illinois: 1,869,744 - 350,633 = 1,519,111 = 1,565,308 - 46,197.
  - Texas: 3,457,259 - 646,356 = 2,810,903 = 2,881,747 - 70,844.
  - Florida: 2,928,850 - 570,055 = 2,358,795, which matches the article's 2.36M.
- Alaska (66,572 in June 2025 vs 69,502 in May 2026) and North Dakota (51,179 vs 51,790) are higher than a year earlier. I checked only these two plus the states named; I did not enumerate the other 46.
- The national drop since June 2025 is consistent in size. The warehouse shows 42.02M in June 2025 and 36.50M in May 2026 (states plus DC), a 5.5M drop. The article says more than 5.3M, or 12.8%.

**Errors and problems**
1. **Age limit is wrong.** The article says the work-rule upper age limit rose "from 59 to 64". It was 54 (ages 18-54) before OBBBA (KFF Health News and others). One outlier, Fox LA, repeats the 59 figure. The other OBBBA description (80 hours a month, exemptions removed for veterans, homeless people and former foster youth, parents of children 14 and older now covered) is accurate.
2. **Georgia does not reconcile.** The article says Georgia fell 124,381, from 1.32M to just under 1.2M, between May and June. The warehouse has Georgia at 1,352,134 in April 2026 and 1,187,277 in May 2026. The article's June level looks like the warehouse's May level. Either USDA revised the figure or the months are mislabelled. I could not resolve which. Georgia is about 124k of the 332k national drop.
3. **"16 consecutive months since March 2025" is stretched.** April 2025 (42.35M) was above March 2025 (42.19M). The warehouse shows declines every month from May 2025 through May 2026, which is 13 declines, or 14 with June. The 16 comes from counting months inclusively.
4. **Headline June 2026 figures are not checkable here.** That covers the 300k+ drop, the 332,000 national drop, 36.35M, 36.69M, and the 34 declining and 16 rising state counts. June 2026 is null in the warehouse. A third-party tracker I found puts June at 36.52M, not 36.35M, and I could not reach USDA's June file. The article's May 36.69M also differs from the warehouse's 36.50M (states plus DC; 36.56M with territories).
5. **The $385M-a-day figure** is a USDA spokesperson quote. It cannot be compared with the SNAP table: warehouse FY2025 SNAP benefits are about $95.8B, or about $262M a day.

**Framing**
- The piece ties the drop to OBBBA, but it also says the decline began before the law, and the USDA quote says the same. The Georgia swing, about 124k of the 332k, goes unexplained.
- The Share Our Strength quote is opinion and was not graded.
- The three Newsweek pages the article links were fetched. They are consistent with the warehouse (about 42M in early 2025 to just over 37M by April 2026).

**Exclusions.** I dropped fiscal-year total rows (month IS NULL). I also dropped rows with null persons_participating: American Samoa, Puerto Rico and the Northern Mariana Islands in May 2026, plus every June 2026 row. The national totals exclude PR, GU, VI, AS and MP. The warehouse total is 36.50M without those territories and 36.56M with them (June 2025: 42.02M vs 42.08M), a difference under 0.2%.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-11-08/report.html
