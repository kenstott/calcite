The Travel And Tour World article's claims about sunset-window fatal crashes, seasons and November are accurate.

Rating: 0 Pinocchios (engine rating from claim scores).

**What was checked.** The article relays a Lerner & Rowe study (a personal-injury law firm) of 2024 NHTSA FARS data. I fetched the study itself and compared it with the article. I also tested the central claim against the warehouse (transport.fatal_crashes, FARS 2024).

**Findings**
- Central claim: autumn had 28.3% more fatal crashes per day within two hours of sunset than winter (25.8 vs 20.1). The study says 28.4%; the article's inputs give about 28.4%, so this is a rounding difference. My warehouse approximation, using a solar-formula sunset and time zones set from longitude, gave 25.2 vs 19.7 per day, or +28%.
- Seasonal crash and death counts: the study's figures match the article. The deaths total 8,921 (2,497+1,979+2,189+2,256), and the season ordering matches my approximation.
- November 27.3 per day vs January 19.0: the article matches the study. The arithmetic gives +43.7% against a stated 43.4%, which is a rounding difference.
- November post-sunset crashes (543) vs pre-sunset (276) is +96.7%, which checks. The national 5,101 vs 3,233 is +57.8%, which checks. The 26.9%, 21.0% and 72.2% shares match the study.
- Phoenix and Las Vegas sunset times match the study. They are consistent with Phoenix not observing daylight saving time.
- I could not reproduce the exact counts. My total was 8,163 against 8,334, and my pre/post gap was about +46% against 57.8%, because I used whole crash hours. The query dropped crashes with missing hour, latitude or longitude, and I did not rerun it without those exclusions.
- Of the 14 links in the article, I fetched all but one. Thirteen were social share buttons with no content, and the other was the study.

**Framing caveats (the article's facts hold up, but these are real)**
- The headline says crashes "rise" in autumn. The data is one year of cross-season comparison. The study itself says one year cannot establish a recurring pattern, and the article leaves that out.
- The source is a law firm's marketing piece, and the article does not say so.
- These are counts, not risk, because there is no mileage or exposure measure. Autumn sunsets also fall during the evening commute.
- The article did say the analysis does not establish cause.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q9/askamerica/2026-11-23/report.html