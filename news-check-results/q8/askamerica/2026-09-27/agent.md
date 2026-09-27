# Fact-check: CNBC, "Apple faces more than $5.7 billion patent infringement verdict" (Sept 26, 2026)

**Split Pinocchio rating (Washington Post scale): Fidelity 1/4, Claims Accuracy 0/4.** (Updated per corrected engine guidance: split grading is mandatory whenever a graded claim is attributed to a named third party — here, the court/jury, Bloomberg Law's overlapping reporting, and Quinn Emanuel's attorneys.)

## Method
AskAmerica's government-data corpus (SEC, Census, BLS, FEC, USPTO, etc.) has no litigation/court-docket schema, so this civil-litigation story falls mostly outside what the connector can verify directly. The one genuine match: **USPTO's `patents.patent_grants` table**, queried directly (`SELECT patent_id, patent_date, patent_title FROM patents.patent_grants WHERE patent_id IN ('10659885','10820117')`), confirming both patents CNBC cites are real, granted 2020-05-19 and 2020-10-27, titled "Systems and methods for generating damped electromagnetically actuated planar motion for audio-frequency vibrations" — matching the article's vibration/haptic-transducer description. All other claims were verified against independent primary/press sources fetched directly this session: Bloomberg Law (docket No. 3:21-cv-00812), AppleInsider (quotes the jury verdict form directly and reproduces Apple's statement verbatim), and Quinn Emanuel's own attorney directory.

## Why split, and how each half was scored
- **Fidelity (1/4)** grades whether CNBC accurately represented the named third parties it quoted/cited. CNBC's quotes from Apple and Lance Yang are word-for-word verified against AppleInsider's independent reporting. The one issue: CNBC calls Yang Taction's "lead counsel," but Bloomberg Law attributes a near-identical quote on this same verdict to a *different* Quinn Emanuel partner, Tigran Guledjian, and at least five Quinn Emanuel partners worked the case with no one designated "lead counsel" in any filing found — a real, minor mislabeling of a quoted source's role.
- **Claims Accuracy (0/4)** grades whether the substantive facts attributed to those parties are true. The verdict amount, patent details, case timeline, trial mechanics, and willfulness finding are all independently corroborated with no significant errors found.

## Claim-by-claim
| Claim | Verdict |
|---|---|
| Jury awarded Taction >$5.7B (exact: $5,721,961,750) for infringing two patents (US 10,659,885, US 10,820,117) | **True** — USPTO data + Bloomberg Law + AppleInsider |
| Taction sued Apple in 2021 in S.D. Cal.; Apple won dismissal in 2023; Federal Circuit revived the case (Aug. 13, 2025) | **True** — Bloomberg Law docket + AppleInsider |
| Apple's full statement quote to CNBC | Verified verbatim via AppleInsider (no matching AskAmerica table for corporate statements) |
| Lance Yang (Quinn Emanuel) quote | Quote and speaker verified true; "lead counsel" label unverified — see Fidelity note above |
| Trial began Sept. 14; 7 jurors deliberated 2 days; verdict 1:15pm PT Friday | Substance independently corroborated by Bloomberg Law (no matching AskAmerica table for court-docket mechanics) |
| Jury did not find infringement willful | **True** — Bloomberg Law + AppleInsider (citing the actual verdict form) |

## What's materially missing (flagged separately from the claim scores)
CNBC frames this purely as scrappy-inventor-vs-Apple. Bloomberg Law's own reporting on the same verdict discloses that Taction's suit was financed by outside litigation funders — Kenosha Investments LP and Gronostaj Investments LLC, with Kenosha identified in separate litigation as an indirect subsidiary of **Burford Capital Ltd.**, a publicly traded litigation funder. Apple fought hard in discovery to expose this relationship (a judge even threatened the funders with sanctions back in July 2023). This doesn't make anything CNBC reported false, but its absence leaves readers with an incomplete picture of who has a financial stake in a $5.7 billion award.

## Report
Published: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q8/askamerica/2026-09-27/report.html` (local session link: http://127.0.0.1:50411/a/9a1ea4bf1a50956d39a556d8f9d29de2.html) — 6 sections, 6 citations, dashboard inlined, split fidelity/claims_accuracy Pinocchio banner.

## Sources
- [CNBC article under review](https://www.cnbc.com/2026/09/26/apple-taction-technology-patent-infringement-verdict.html)
- [Bloomberg Law: Apple Owes $5.7 Billion for Infringement of Haptics Patents](https://news.bloomberglaw.com/ip-law/apple-owes-5-7-billion-to-litigation-funded-firm-in-patent-case)
- [Bloomberg Law: Apple's Win in Haptics Patent Case Undone by Federal Circuit](https://news.bloomberglaw.com/ip-law/apples-win-in-haptics-patent-case-undone-by-federal-circuit)
- [AppleInsider: Apple owes Taction $5.7B after losing haptic feedback IP trial](https://appleinsider.com/articles/26/09/26/apple-owes-taction-57b-after-losing-haptic-feedback-ip-trial)
- [Quinn Emanuel attorney profile: Lance Yang](https://www.quinnemanuel.com/attorneys/yang-lance-1/)
- AskAmerica `patents.patent_grants` (USPTO PatentsView bulk data)
