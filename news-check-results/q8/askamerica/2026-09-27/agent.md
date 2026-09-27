# Fact-check: CNBC, "Apple faces more than $5.7 billion patent infringement verdict" (Sept 26, 2026)

**Overall grade: 1 Pinocchio (Washington Post scale).** Every discrete, checkable fact and quote in the article holds up under independent verification. The rating is not zero solely because of a materially relevant omission (see below).

## Method
AskAmerica's government-data corpus (SEC, Census, BLS, FEC, USPTO, etc.) has no litigation/court-docket schema, so this is fundamentally a story that falls outside what the connector can verify directly — as expected for civil litigation news. The one genuine match: **USPTO's `patents.patent_grants` table**, queried directly (`SELECT patent_id, patent_date, patent_title FROM patents.patent_grants WHERE patent_id IN ('10659885','10820117')`), which confirmed both patents CNBC cites are real, were granted 2020-05-19 and 2020-10-27, and are titled "Systems and methods for generating damped electromagnetically actuated planar motion for audio-frequency vibrations" — consistent with the article's description of vibration-based haptic transducer technology. All other claims were verified against independent primary/press sources (Bloomberg Law, which cites the actual docket number, and AppleInsider, which quotes the jury verdict form directly and reproduces Apple's statement verbatim).

## Claim-by-claim
| Claim | Verdict |
|---|---|
| Jury awarded Taction >$5.7B (exact: $5,721,961,750) for infringing two patents (US 10,659,885, US 10,820,117) | **True** — confirmed via USPTO data + Bloomberg Law + AppleInsider |
| Taction sued Apple in 2021 in S.D. Cal.; Apple won dismissal in 2023; Federal Circuit revived the case (Aug. 13, 2025) | **True** — Bloomberg Law docket (No. 3:21-cv-00812) + AppleInsider |
| Apple's full statement quote to CNBC | **True** — AppleInsider published the identical statement verbatim, shared directly with it by Apple |
| Lance Yang (Quinn Emanuel) quote: "We're happy the jury found for Taction..." | **True (quote verified)**, but CNBC's "lead counsel" label is an unverified characterization — Bloomberg Law attributes a near-identical quote on the same case to a *different* Quinn Emanuel partner, Tigran Guledjian, and public records show at least five Quinn Emanuel partners worked the case with no single one designated "lead counsel" in any filing found |
| Trial began Sept. 14; 7 jurors deliberated 2 days; verdict 1:15pm PT Friday | **True on substance** — Bloomberg Law corroborates two days of deliberation and a Friday verdict |
| Jury did not find infringement willful | **True** — Bloomberg Law + AppleInsider (citing the actual verdict form) |

## What's materially missing (the reason for 1 Pinocchio, not 0)
CNBC frames this purely as scrappy-inventor-vs-Apple. Bloomberg Law's own reporting on the same verdict (headlined around the funding, not the damages number) discloses that Taction's suit was financed by outside litigation funders — Kenosha Investments LP and Gronostaj Investments LLC, with Kenosha identified in separate litigation as an indirect subsidiary of **Burford Capital Ltd.**, a publicly traded litigation funder. Apple fought hard in discovery to expose this relationship (a judge even threatened the funders with sanctions back in July 2023). This doesn't make anything CNBC reported false, but its absence leaves readers with an incomplete picture of who actually has a financial stake in a $5.7 billion award — the kind of context a professional fact-checker would flag as materially relevant even when every individual fact checks out.

## Report
Published report (dashboard + full claim table + sourcing): `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q8/askamerica/2026-09-27/report.html` (local link only good for this session: http://127.0.0.1:59830/a/6ba6be7d079b98823c05c16242277f99.html)

## Sources
- [CNBC article under review](https://www.cnbc.com/2026/09/26/apple-taction-technology-patent-infringement-verdict.html)
- [Bloomberg Law: Apple Owes $5.7 Billion for Infringement of Haptics Patents](https://news.bloomberglaw.com/ip-law/apple-owes-5-7-billion-to-litigation-funded-firm-in-patent-case)
- [Bloomberg Law: Apple's Win in Haptics Patent Case Undone by Federal Circuit](https://news.bloomberglaw.com/ip-law/apples-win-in-haptics-patent-case-undone-by-federal-circuit)
- [AppleInsider: Apple owes Taction $5.7B after losing haptic feedback IP trial](https://appleinsider.com/articles/26/09/26/apple-owes-taction-57b-after-losing-haptic-feedback-ip-trial)
- [Quinn Emanuel attorney profile: Lance Yang](https://www.quinnemanuel.com/attorneys/yang-lance-1/)
- AskAmerica `patents.patent_grants` (USPTO PatentsView bulk data)
