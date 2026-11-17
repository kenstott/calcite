**The Mortgage Professional article's claims about October existing-home sales and mortgage rates are mostly accurate.**

**Rating (engine-returned): 1 Pinocchio.**

Sources read this session: the mpamag article, the NAR Nov 12, 2026 release, NAR's housing snapshot and existing-home-sales statistics pages, and the two mpamag articles it links. Warehouse queries were run against econ.fred_indicators (EXHOSLUSM495S and MORTGAGE30US).

**Grouping**
All reported figures are `author_claims`. Yun's quote is a `subject_claims` entry with speaker. There are no `fidelity` entries.

**Verified in the warehouse and against NAR**
- 3.75M SAAR in October, down 5.5% from September (3.97M): true.
- Down 8.8% from October 2025 (4.11M): true.
- "Weakest pace in 16 years" (the central claim): 3.75M is the lowest month since at least January 2010, and NAR says lowest since August 2010. True.
- 7.33% Freddie Mac rate for October: true. The monthly average of the weekly series is 7.328%.

**Verified against NAR's release only (not in the warehouse)**
- Regional sales: West -9.7% to 650k and -14.5% YoY; South -6.5%; Midwest -3.2%; Northeast flat and -4.1% YoY.
- First-time buyers at 29% versus 31% and 32%.
- Inventory of 1.59M (+1.3%), a 5.1-month supply versus 4.7.
- Median price $416,700 (+0.4%), the 40th straight month of annual gains; affordability index 100.7 versus 107.3.
- Investors at 20% versus 16%: true. NAR's category is investors plus second-home buyers, which the article shortens.
- The Yun quote matches NAR's text verbatim.

**Misleading**
- The article calls the 7.33% rate Freddie Mac's "highest mark since 2000" and the subhead calls it a "23-year high". In the warehouse's monthly averages, October 2023 (7.62%) and November 2023 (7.44%) were higher. Yun's own quote carried the caveat "aside from a single month in 2023"; the article's prose and subhead dropped it. Graded partially false.
- Yun's "single month in 2023" does not match my data: two months in 2023 averaged above 7.33%.
- Yun's "as a result" is a causal interpretation the data cannot isolate.

**Server refusals**
No refusal on the final call. Two earlier calls were refused because the AskAmerica server restarted and dropped the record of my fetches and queries. I re-ran them and the final call passed.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q1/askamerica/2026-11-17/report.html