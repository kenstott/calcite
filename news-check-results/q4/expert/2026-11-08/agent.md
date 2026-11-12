# Fact-check: Newsweek, "SNAP enrollment drops 300k in single month—here are worst impacted states"

## Verdict: 1 Pinocchio (national figures check out; state figures could not be fully checked and have since been revised)

**Source access.** The Newsweek URL fetched, but WebFetch returned a model-written summary rather than the raw text, so quotes are paraphrased. The USDA FNS program page returned 403 to WebFetch. It fetched with curl, and I pulled the official files directly: the FNS Nutrition Assistance Program Report (the "birdseye" PDFs) for May, June and July 2026, `snap-persons-10.xlsx` and `snap-4fymonthly-10.xlsx` (both data as of Oct 9, 2026). USDA has no archived state-level June-vs-May table I could reach, so state month-over-month figures are not checked.

| Claim (per Newsweek) | Verdict | Source / confidence |
|---|---|---|
| About 36.35M on SNAP in June 2026 | Confirmed (initial release) | FNS June 2026 birdseye: 36,353 thousand. High. |
| Down from 36.69M in May, about 332k, 0.9% | Confirmed | Birdseye June: May 36,685, Jun 36,353. The change is -332k, or -0.90%. High. |
| Down more than 5.3M (12.8%) vs June 2025 | Confirmed | Birdseye: Jun 2025 was 41,675. The change is -5.32M, or -12.8%. High. |
| "Enrollment fell 16 straight months, since March 2025" | Roughly right, likely off by one | FY monthly file (revised): Mar 2025 was 42.197M, up slightly from Feb's 42.184M. Declines run Apr 2025 to Jun 2026, which is 15 months. The initial-release series may differ. Medium. |
| 34 states down, 16 up in the month | Not checkable here | No state-level June-vs-May table found. |
| Monthly state moves: GA -124,381, TX -70,844, IL -46,197, NY -38,539, MI -24,231; FL +68,036, VA +45,776, AZ +11,563, TN +4,278, CO +4,128 | Not checkable here | Plausible direction. Revised June values in USDA's July table (GA 1.355M, TX 2.811M, IL 1.519M, NY 2.721M, MI 1.329M, FL 2.359M, AZ 421k) are the right order of magnitude but cannot reproduce monthly deltas. |
| YoY: 48 of 50 states down; only Alaska and North Dakota up | Partly contradicted by later data | In USDA's Oct 2026 table, Alaska is up vs July 2025 (+3.5%). North Dakota's revised June 2026 count (52,904) is below its July 2025 count (56,789), so ND likely is not "up" after revision. June 2025 state values were not available. Low-to-medium. |
| YoY declines: TX -646,356, FL -570,055, AZ -465,659, CA -365,350, IL -350,633 | Consistent in size, not exactly verifiable | Against July 2025 in the Oct table: TX -635k, FL -491k (revised), AZ -460k, CA -338k, IL -369k. The ranking is similar. Florida differs most, and Florida data have been volatile. Medium. |
| Cause: OBBBA (enacted July 2025) raised the work-requirement age from 59 to 64 and removed or narrowed exemptions for veterans, homeless people, former foster youth and some caregivers | Accurate | Matches the public text of the law as I know it. I did not re-fetch the statute this session, so this is from memory. Medium-high. |
| Decline also reflects recertification and reporting rules, and enrollment was already falling before the law | Supported | USDA series: declines began Apr 2025, before the July 2025 law. High. |
| USDA spokesperson: SNAP is a "means tested, appropriated entitlement" and changes reflect many factors | Quote not independently found | Attributed statement only. Not checkable here. |
| Anne Filipic (Share Our Strength) criticized the new paperwork rules | Not checkable here | Attributed statement only. |

**Materially misleading or missing context**
- The figures are from USDA's initial, preliminary release. The revised series in the Oct 9 file puts May 2026 at 36.73M and June at 36.52M, so the June drop is about 205k rather than 332k. The headline's "300k in a single month" is true for the initial release only. USDA also warns that FY2026 data are preliminary and subject to revision.
- Georgia and Florida figures have been revised heavily. Georgia's drop was initially reported larger. Florida's +68k June gain was followed by a -5.9% fall in July.
- June is not the biggest monthly drop of the period. The revised series shows larger drops in Oct to Dec 2025 (about 0.65-0.8M a month), so any "worst" framing needs that context.
- The state comparisons are ecological, and the causal link to OBBBA is asserted rather than demonstrated. Several states (AZ, FL, TX) show large drops that are partly timing and administrative effects.

**Confidence.** High on the national numbers. Medium or low on the state-level claims and the "16 months" count.
