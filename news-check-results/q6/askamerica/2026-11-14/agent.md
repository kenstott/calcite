The StartBusinessByState generative AI patent tracker's claims about USPTO published applications, LLM-title filings and AI share are accurate; 1 claim could not be confirmed.

**Rating: 0 Pinocchios** (engine-computed from claim scores).

Source: https://startbusinessbystate.com/?p=16687 (fetched successfully).

**Checks.** Every figure reproduces from the publisher's own monthly CSV, which I re-added by hand. The sister pages repeat the same annual totals.
- 939 (2023), 1,141 (2024), 2,754 (2025) and 4,187 (Jan-Oct 2026) all match. Oct 2026 = 584. The up-84% claim holds: 4,187 / 2,275 = 1.840.
- The generative-network code peaked at 526 in Sept 2026, and ran 18-119 a month in 2020-22. G06N3/0475 was introduced in the January 2023 CPC revision.
- LLM-title counts of 7/112/601/780 match, and Aug 2024 is the first month with 10 or more.
- AI share peaks match: 9.99% (Apr 2026), 9.97%, 9.75%, 9.41% (Dec 2022), 5.8% (Jan 2019).
- The top-filer table (SAP/Google/IBM) is unsupported: no accessible source carries applicant data. It is not contradicted.

**Data sourcing.**
- The warehouse has only granted patents (patents.patent_grants), no published-application or CPC-by-month data. A sanity query returned 327,100 grants dated 2025, against the publisher's 325,041 U.S. utility grants for the same year.
- The USPTO Open Data Portal API returned HTTP 401, on both the engine fetch and the WebFetch retry. Raw USPTO published-application counts are therefore not independently confirmed.
- All 13 links the article cites were fetched.

**Fragile or potentially misleading points.**
- The Sept 2026 code spike is the figure most exposed to retroactive reclassification (disclosed in the article).
- Counting any title containing "generative" adds noise (disclosed), so the total is an upper-bound proxy.
- Oct 2026 "all utility applications" is 50,039, about 36% above Oct 2025 (36,827). The Oct share rests on an unusually large denominator that I could not check.
- Counts reflect filings from about 18 months earlier (disclosed in the FAQ).
- The page carries affiliate and service promotions, and the About page says AI tools help produce content.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q6/askamerica/2026-11-14/report.html