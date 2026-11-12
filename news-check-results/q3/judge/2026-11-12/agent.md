# q3 judge: The Center Square (via yournews) "Interest on the debt cost $1.4 trillion last year, audit finds"

Rank: [askamerica, expert, everyman]

The story ran Nov 11, 2026, on GAO-27-108795 (FY2026 and FY2025 Schedules of Federal Debt, Nov 9, 2026). Judged 2026-11-12. CBO pages return HTTP 403 to fetch tools, so CBO levels rest on secondary coverage and arithmetic. I verified against the article text, the GAO product page, NTU's debt-limit projection, and Fortune (Nov 9, 2026) on the Bank of America projection.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 0 | GAO product page; warehouse econ.federal_debt and econ.federal_outlays_by_function; AAF/CRFB/NTU/CRS; CBO blocked |
| expert | 1 | GAO product page; Treasury Fiscal Data API (debt_to_penny, interest_expense, MTS); CBO MBR; CRFB/NTU/CRS; CBO PDF blocked |
| everyman | 1 | GAO product page; CBO summary page; CRFB/BPC; CBO PDFs blocked |

## Assessment
- **Correct verdict: 0 Pinocchios.** Every traceable figure matches. The headline's $1.4T is GAO's own wording ("interest costs were $1.4 trillion"). The article's second sentence says about $1.1T went to bondholders and the rest was credited to federal trust funds, so the gross-versus-public split is disclosed up front. A headline that uses the audit's own number and a body that immediately qualifies it is not shading.
  - **GAO $1.4T total, $1.1T on debt held by the public, $392B in FY2021: true.** GAO attributes the rise to "growing debt and higher interest rates".
  - **Debt $40.2T, up about $2.5T, up about 105% from $19.6T: true.** GAO states all three. Treasury data in two arms gives $40,171.8B vs $37,637.6B vs $19,573.4B.
  - **Schedules fairly presented and controls effective: true.**
  - **Deficit about $2T (CBO), $24.4T over 2027-36 (about $2.4T a year), limit $41.1T from OBBBA July 2025, eight suspensions since 2013, CR through Dec 11: true.**
  - **CBO net interest $1.14T, second only to Social Security: not independently confirmed.** The +$115B / 11% growth and the rank are corroborated. The level is consistent with a base of about $1.03T. Unverifiable, not false. The warehouse OMB-function table (net interest $1.017T, below Medicare) is a different basis and does not refute it.
  - **Dickerson's February 2027 limit date: a forecast, defensible.** NTU's August piece centers on May 2027 with February possible. Bank of America (Fortune, Nov 9) projects February contact on $557B of remaining room. It is a named expert's expectation, correctly attributed. The "just under $1.5 trillion in cash on hand" figure found no source anywhere (NTU says over $1T of resources). This is a quote from a named source, not the outlet's claim.
  - **Ditch's remark: opinion.**
  - GAO's $1.4T is accrual expense, not cash. "Went to bondholders" is slightly loose for the $1.1T, a minor imprecision with no effect on the verdict.
- **askamerica (1st):** correct rating and the best handling of what could not be verified.
  - It separated unsupported from false for the CBO ranking and the cash-on-hand figure, and did not penalize the article for them.
  - It noticed that NTU's range includes February and that Bank of America independently projects February, which is the right resolution of the date question.
  - It judged the headline against the article's own disclosure of the split, which is the correct test, and flagged the accrual-versus-cash looseness.
  - Weaknesses: it never read a primary CBO page, never ran score_claim, and its "Dickerson end-of-November 2027 exhaustion" line rests on secondary sources.
- **expert (2nd):** the most rigorous number-checking, one step off on the rating.
  - The Treasury API split ($1,068.5B public plus $291.9B intragovernmental = $1,360.4B) confirms the article's description exactly. The MTS net interest of $1,051.8B is useful context.
  - It then rated 1 Pinocchio for a framing the article itself discloses.
  - It described Dickerson's dates as conflicting with NTU's August piece. That overstates it: February is inside NTU's range and a newer Bank of America projection supports it.
- **everyman (3rd):** correct figures, weakest handling.
  - It rated 1 for the headline while conceding the article explains the split in its body, and it called the headline overstated by 25-30%, which charges the outlet for the audit's own wording.
  - It called the three measures "easily confused" even though the article labels each.
  - It checked fewer items and left the Dickerson dates as "no source found for this speaker" when NTU's projection page is findable.
  - Positives: it verified the CR, the eight suspensions and the deficit, and it flagged that the $2.4T average is its own division.

## Recipe check
Not tested. No recipe step mapped to this story (federal debt-and-interest audit claims). A reusable procedure would have helped on one point: when a headline figure is gross, check the article text for the disclosed split before grading framing. The warehouse supplied the debt series and an outlay comparison, but the comparison was on a different basis from CBO net interest. No table carries FY2026 GAO interest by holder, so the web was the right route.

## Severity invariant
**Not triggered:** askamerica ranks above everyman. askamerica's rating (0) matches the correct verdict. Everyman's (1) does not.
