# Judge: shattered.io, "CISA KEV Additions Hit 298 This Year, Up 35%", 2026-11-26

**Piece:** https://shattered.io/cisa-kev-additions-298-up-35-percent-from-221-ransomware-20-percent-of-catalog/. The task was to fact-check the piece, extract its claims, and grade it on 0-4 Pinocchios.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. The fingerprint check passed: the everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. No unaided arm mentions askamerica.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Core count (298 vs 221) | Not counted; checked for consistency only (1,484 + 298 = 1,782) | Warehouse `cyber_vuln.kev_catalog`: exactly 298 vs 221, +34.8% | Counted from CISA's KEV JSON: 298 vs 221, +34.8% |
| Ransomware 361/1,782; 27/298 | Not counted | 361/1,782 = 20.3%; 27/298, both from the warehouse | 361/1,782 = 20.3%; 27/298, both from the JSON |
| 78% short deadlines | Not checkable | True as reported (ImmunitySec 128/165; Pyramid 113/139); its own SQL recompute failed on date arithmetic | Recomputed: 73-76% for windows ending Nov 17; 78% only for Sep 1 to Nov 25 |
| Beazley 38% | 28% confirmed | Mostly true: the 38% is quarter over quarter, but sits beside a year-over-year KEV figure | Quarter over quarter, not year over year |
| Context added | Live catalog at 1,805 on Nov 25; CISA relabeled 59 entries in 2025 | Same-window history: 544 (2022), 173 (2023), 161 (2024), 221 (2025), 298 (2026). 2025 is a low base, and 2022 was higher than 2026 | None beyond the windows |
| Grade | 1 | 0 (engine-computed) | 0 |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

- The two arms that counted got identical results by independent routes: askamerica's warehouse query (298, 221, 361, 1,782, 27) and expert's count from the CISA JSON. That agreement shows the warehouse KEV table is current through at least Nov 18.
- Askamerica's multi-year same-window series is the most material context any arm found. The article's "the only comparison available" is false as a statement about the data, and 2026 is not a record (2022 had 544). Askamerica graded this "mostly true / minor omission". The judge reads it as fair, because the piece makes no record claim.
- Askamerica's SQL recompute of the 78% failed on date arithmetic. Expert recomputed it and found the figure depends on the window. Expert handled this one better.
- Claim shape: split present (10 author, 1 fidelity). Claim 1 (the 298/221 count) is explicitly attributed to ImmunitySec in the piece. It would ideally be `fidelity`, but the shape requirement (split when any claim is attributed) is met. I did not resume, because a resume restarts the MCP process and repeats the research.

## Ranking rationale

1. **askamerica.** It matched expert's verification of every headline count from its own warehouse in a few queries. It also added the 2022-2026 same-window history, which reframes "up 35%" as recovery from a low base. It flagged the quarter-over-quarter vs year-over-year juxtaposition and graded the unsourced Synology patch-date claim as unsupported.
2. **expert.** It matched every count and did the 78% window analysis correctly. It did not provide historical context for the year-over-year comparison.
3. **everyman.** Its arithmetic and directional reading were correct, but it could not count the catalog, so the core numbers rest on consistency checks.

Askamerica and expert are close. If the 78% recompute counted for more than the multi-year base context, expert would edge ahead.

## Recipe check

Nothing new to teach. Askamerica's date-arithmetic failure in its own SQL is a one-off agent error; I have no evidence it is a tool defect. No corpus gap: the KEV table answered everything.
