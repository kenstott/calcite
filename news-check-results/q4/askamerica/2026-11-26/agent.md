The Shattered article's claims about CISA KEV additions, the ransomware flag and Beazley's Q3 ransomware data are accurate; 1 claim could not be confirmed.

**Rating: 0 Pinocchios.**

**Central claim: true.** CISA added 298 vulnerabilities to the KEV catalog from Jan 1 to Nov 17, 2026, against 221 in the same window of 2025. The warehouse (cyber_vuln.kev_catalog, date_added) returns exactly 298 and 221. The gap of 77 is 34.8%, which rounds to 35%. The figures come from an ImmunitySec release (openPR, Nov 18), which the article attributes correctly and does not credit to CISA.

**Other claims checked**
- **Ransomware flag: true.** 361 of 1,782 entries is 20.3% as of Nov 18. The warehouse now shows 362 of 1,795 (Nov 24), and the CISA feed shows 1,805 on Nov 25. The article correctly says this is a whole-catalog share, not a 2026 share.
- **27 of 298 ransomware-linked: true.** The warehouse also gives 27 of the 298 additions, about 9%.
- **Nov 18 and Nov 20 batches: true.** The Nov 18 batch is Synology DSM, LiteLLM, Axis and REDCap. The Nov 20 batch is SAP NetWeaver, Cisco Expressway, VMware vCenter and Apache Roller. Only Synology is flagged Known for ransomware.
- **Nov 10, 13 and 17 batches: true.** Dates and products match. The Microsoft entry is catalogued as Office but is described as Outlook in the publisher's own Nov 10 report.
- **Beazley figures: mostly true.** Incidents rose 38% and exploitation as an entry point went from 14% to 28%. I read these in a syndicated Reinsurance News copy, because the KXAN page returned 403. The 38% is quarter over quarter. The article does not say so, and places it next to a year-over-year KEV figure.
- **78% of post-June-10 entries had due dates of 3 days or less: true as a reported figure.** ImmunitySec states 128 of 165. Pyramid Systems counts 113 of 139 (81%) for Jun 11 to Oct 23. The article calls the figure "unverified", which is overly cautious. My own SQL recomputation failed on date arithmetic.
- **Synology patched in December 2024, C3RB3R ransomware link: unsupported.** The KEV feed confirms the entry and the Known flag. It does not confirm the patch date or the ransomware family. The only support is the publisher's own sister article, and the article hedges this claim itself.

**Framing flags**
- The article says 298 vs 221 is "the only comparison available". Same-window counts in the warehouse are 2022: 544, 2023: 173, 2024: 161, 2025: 221, 2026: 298. So 2025 was a low base, and 2022 (the catalog's launch backlog) was higher than 2026. This is a minor omission, not a distortion.
- The article correctly avoids applying the 20% lifetime ransomware share to 2026 additions.

**Source limits.** The CISA Nov 18 alert page and the KXAN page returned 403, and the trebsec page returned 404. I checked those claims against the CISA JSON feed and the warehouse instead. I fetched all other linked pages. They are the publisher's own sister articles and add no independent evidence.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-11-26/report.html