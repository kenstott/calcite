# Fact-check: "Generative AI Patent Tracker" (startbusinessbystate.com/?p=16687)

Piece: StartBusinessByState.com, by Robert Smith, last updated Nov 3, 2026. It is a data tracker, not a news story. It contains no attributed quotes.

## Method and limits
I used WebFetch and WebSearch only. I could not run Python, and I did not reach the USPTO Open Data Portal bulk data, so the underlying counts are not independently re-derived. What I could do: fetch the article's own downloadable CSV (generative-ai-patents-monthly-2026-10.csv) and check every headline number in the text against it by hand arithmetic. That tests internal consistency, not the USPTO source.

## Claims checked against the article's CSV
| Claim | Result |
|---|---|
| Gen-AI applications 939 (2023), 1,141 (2024), 2,754 (2025) | Match the CSV monthly sums |
| 4,187 in Jan-Oct 2026; 2,275 in Jan-Oct 2025; up 84% | Match (4187/2275 = 1.84) |
| October 2026 = 584 | Match |
| G06N3/0475 peaked at 526 in Sep 2026 | Match (highest month in the CSV) |
| G06N3/0475 ran 18-119 a month in 2020-2022 | Match (min 18 Feb 2020, max 119 Jun 2022) |
| LLM/"large language model" titles 7 / 112 / 601 / 780 (2023 / 2024 / 2025 / Jan-Oct 2026) | Match |
| First month with 10 or more LLM titles was Aug 2024 | Match |
| AI share peaked 9.99% Apr 2026; 9.97% Sep; 9.75% Mar; Oct 8.58%; Jan 2019 5.8% | Match |
| Top LLM-title filers (SAP 35, Google 35, IBM 33 in 2026; Google 37, Microsoft 29, Cisco 24 in 2025; Microsoft 25, Google 13, Shopify 7 in 2024) | Not checkable here. The assignee data is not in the CSV and I could not query the USPTO source. |
| Methods statements (about 18-month publication lag, Thursday publication, published is not granted, CPC definitions) | Consistent with general USPTO practice. Not itemized further. |

## Primary-source status
No USPTO or PatentsView figures were independently retrieved. Web search found no independent source that reproduces these counts. They rest on the author's own pull from the USPTO Open Data Portal.

## Notable observation
A search-engine snippet of an earlier version of this same page quoted 3,452 gen-AI applications for Jan-Sep 2026 and 375 for G06N3/0475 in Sep 2026. The current page and CSV give 3,603 (Jan-Sep) and 526. The page says past months may be revised and the author discloses reclassification. Still, a revision of that size (Sep G06N3/0475 up about 40%) shows the latest months are unstable. The Sep 2026 spike (526 vs 347 in Oct) may reflect a classification batch rather than real filing behavior.

## Materially misleading or context issues
- These are published applications, not filings or grants, and they lag filing by about 18 months. A headline reading of "AI patenting is surging in 2026" actually describes filings from roughly 2025. The article says this itself.
- "Generative AI" is defined broadly: G06N3/0475 plus title keywords ("generative", LLM). This includes things like "generative design", so it overcounts.
- Counts are volatile because of five-Thursday publication months, which the article discloses.
- The share-of-AI measure uses a broad AI definition (G06N, G06F40, G06V, G10L15), so it is not strictly "AI patents".

## Rating (Washington Post Fact Checker scale)
**0 Pinocchios (provisional).** Every checkable number matches the article's own dataset, and the caveats are disclosed. Two limits: the USPTO source data and the assignee rankings were not independently verified, and the numbers are being revised between versions. If the filer rankings or counts fail against USPTO data, this would move up.
