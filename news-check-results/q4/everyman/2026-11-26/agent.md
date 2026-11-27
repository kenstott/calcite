# Fact-check: shattered.io "CISA KEV additions 298, up 35% from 221; ransomware 20% of catalog"

Method: fetched the article (WebFetch returned a summary, not verbatim text, so quotes were not checked word-for-word), the live CISA KEV JSON feed (catalogVersion 2026.11.25, count 1805), and web searches. I could not run a full count over the 1.8M-character JSON, so the year-over-year totals and the ransomware flag count are checked only by indirect arithmetic.

## Claims and verdicts

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | 298 KEV additions Jan 1 - Nov 17, 2026 | Plausible, consistent, not directly counted | CISA catalog ended 2025 at 1,484 (Heise/SecurityWeek). 1,484 + 298 = 1,782, matching the article's "roughly 1,782" entries. The live feed is 1,805 on Nov 25 (includes the Nov 18 batch and later). |
| 2 | 221 additions in the same 2025 period | Plausible, not directly counted | CISA added 245 in all of 2025 (Heise). 221 by Nov 17 leaves 24 for the rest of the year, which is reasonable. |
| 3 | +77, about 35% | Arithmetic correct | 77/221 = 34.8%. The comparison is the author's/ImmunitySec's calculation, as the piece says. |
| 4 | Nov 18 batch of four: CVE-2024-10441 (Synology DSM), CVE-2026-40217 (BerriAI LiteLLM), CVE-2026-90436 (Apache Axis), CVE-2026-90817 (Vanderbilt REDCap) | Confirmed | CISA feed lists all four with dateAdded 2026-11-18. |
| 5 | Synology DSM flaw linked to ransomware | Confirmed | Feed: knownRansomwareCampaignUse = Known. The other three are Unknown. |
| 6 | LiteLLM tied to LLMjacking, Axis to credential theft, REDCap to cryptomining | Not checked | Exploitation-type descriptions not verified against CISA notes. |
| 7 | 361 of about 1,782 entries (20%) carry the ransomware flag | Not checked here (arithmetic 361/1782 = 20.3% is right) | Full flag count not tallied. Direction is plausible; the article correctly says this is the whole catalog, not 2026 additions. Note the total is stale: the live catalog is 1,805. |
| 8 | KEV requires a CVE ID, evidence of exploitation, and clear mitigation guidance | Consistent with CISA's published criteria | Standard KEV inclusion criteria. |
| 9 | Beazley: exploitation was the initial vector in 28% of Q3 2026 ransomware cases, up from 14% in Q2 | Confirmed | Insurance Edge (Nov 19, 2026) reports the share "doubled to 28%". The article itself notes it is one vendor's caseload. |
| 10 | BOD 26-04 applies to federal civilian agencies, deadlines as short as 3 days | Confirmed | CISA bulletin, BleepingComputer, SecurityWeek: BOD 26-04 "Prioritizing Security Updates Based on Risk", supersedes BOD 19-02 and 22-01, up to 3-day deadlines. |
| 11 | 27 ransomware-linked 2026 additions; 78% of recent entries had deadlines of 3 days or less | Not checkable here | Needs a full feed tally. The article flags these as single-source. Feed note: due dates seen were 3 days (Nov 21) for three of the four Nov 18 entries and 14 days for LiteLLM, which fits the BOD tiers. |
| 12 | Forecasts | Opinion | Not fact-checkable. |

## Misleading-context assessment

- The 35% rise is a raw count, not evidence of more attacks. The piece says so (curation, verification speed, product mix). It does not mention that BOD 26-04 itself may change how and what CISA lists.
- The 20% ransomware share is for the whole catalog, and it is mutable (CISA relabeled 59 entries in 2025, per The Register). Headline pairs it with the 2026 count, which can lead readers to read 20% as a 2026 statistic. The body clarifies this.
- Cutoff Nov 17 means the headline is stale; the catalog has grown since.
- Early-2026 trackers (Axis: 181 vs 155 through Aug 11, +16.8%) showed a smaller gap than 35%; the pace would need to have accelerated sharply later in the year. Not contradictory, but worth an explicit source for the 221 figure.

## Rating

1 Pinocchio (mostly accurate, minor caveats). Everything I could directly check (Nov 18 entries, ransomware flag on Synology, Beazley 28%, BOD 26-04, arithmetic) is right, and the piece hedges its own weak claims. Unverified: the exact 298/221 counts and the 361 flag count, which are internally consistent with known catalog totals. Not-checkable list: items 6, 7 (count), 11.
