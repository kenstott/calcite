# q3 judge — newsweek-mortgage-free-homeowners-west-virginia (2026-11-19)

Rank: [askamerica, expert, everyman]

Story: Newsweek, about 2026-11-13, "Map shows states where homeowners are most likely to be mortgage-free". Realtor.com's read of the 2025 ACS: West Virginia 56.6% owned outright against 41% nationally. The piece also gives the Census trend 34.4% (2010-14) → 39.4% (2020-24), Freddie Mac rates and October existing-home sales.

All three rate it 1 Pinocchio, and none could retrieve the 2025 state shares (the Census API now needs a key, and Realtor.com returned 429). So the comparison turns on what else each found.

**askamerica (#1).** Checked the rates (7.11/7.18/6.24) and 3.75M existing-home sales against the warehouse (`econ.fred_indicators` MORTGAGE30US, EXHOSLUSM495S), and the 2021→2022 rate doubling from the same series. It found two errors inside the piece that neither other persona caught: "15.6 percent more than the national average" is 15.6 *percentage points* (about 38% relative), and Maryland at $415,000 is not "just as expensive" as DC at $528,000 by the article's own numbers. It also flags the unlabelled 2025 1-year against 2020-24 5-year comparison. After one resume, the Realtor.com economist's quote is a `subject_claims` entry with speaker Jiayi Xu. Miss: it confirmed the sales figure from FRED's NAR series without noting that the article credits it to Realtor.com.

**expert (#2).** Computed the regional mortgage shares from the 2024 ACS 1-year (West 64.3%, South 57.5%, and so on, against the article's 63.8%/56.8%/58.7%), so the ordering is right. It caught that 3.75M is NAR's number, not Realtor.com's, and gave older-vintage West Virginia figures (55.1% 2024 1-year, 53.9% 5-year) that agree with the ranking. It also flags the missing map and the vintage mixing. It missed the percent/point slip and the Maryland/DC contradiction.

**everyman (#3).** Verified the Census trend and the Freddie rates, and reported West Virginia's 53.9% (Census 2024) against 56.6% as a vintage difference. It noticed the 41%/39.4% conflict. Much else is left as not checkable.

**What would change it.** askamerica and expert are close. Each caught something real the other missed. askamerica's two catches are errors in the article's own text, so they count for a little more than expert's attribution catch and regional recomputation.

Severity check: askamerica above everyman. No defect.

Claim shape: first delivery graded the Xu quote as an author claim. Resumed the same subagent; claims.json now shows `subject_claims | Jiayi Xu`. Re-checked on disk.

Gap audit: askamerica reported "the warehouse has no state-level mortgage-free table". Confirmed: mortgage counts exist only in `census.acs_congressional_district` (B25091). Filed govdata-ops#1357 (ACS B25081 mortgage status by state/county; type:sourcing, kind:gap, schema:census, status:open).

## Recipe check

expert's "check who actually publishes the number the article attributes" (NAR, not Realtor.com) is a general fact-checking habit. The engine's validation instructions already require checking attribution, so no recipe. askamerica ranked #1, so no optimize resolution beyond the gap filed.
