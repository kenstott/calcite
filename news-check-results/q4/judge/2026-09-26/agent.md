# Judge: q4 — "Federal research funding cuts: JHU layoffs, NIH -25% grants, NSF -46% grants, AAU 10-25%/32% declines"

Blind judge of three fresh, same-day persona runs (news claim, Workstream A of the daily-eval pipeline).

Rank: [expert, askamerica, everyman]

## Reasoning

All three converge on the same overall verdict: claim 1 (JHU layoffs, >$500M portfolio decline) is solidly TRUE across 5+ independent outlets and JHU's own statement; claim 3 (NSF -46%, lowest since the 1980s) is TRUE; claim 4 (AAU institution declines) is plausible but weakly sourced (AAU's own page returns HTTP 403 to direct fetch in every attempt across all three personas); and claim 2 (NIH ~25% fewer grants) is the one genuinely metric-dependent claim, where "25%" doesn't cleanly match any single official NIH statistic.

**Expert** is first: it does the sharpest version of the metric-precision check the question is really testing — pulling NIH's own primary "FY2025 By the Numbers" report directly and showing three different metrics give three different declines (all extramural awards -6.2%, RPG new/competing -20.5%, R01-equivalent -21.8%), then tracing the specific "~25%" figure circulating in secondary reporting (STAT News) to a *different* thing entirely — a mid-year FY2026 pace comparison, not a same-metric FY25-vs-FY24 count. This is exactly the "which metric" trap the task brief warned about, caught and precisely diagnosed. It's honest about claim 4's sourcing weakness (AAU's page 403'd twice, so its content rests on search snippets, not primary text) rather than treating repeated snippet consistency as equivalent to a verified primary read.

**askamerica** is second: its per-claim structure is essentially identical to expert's conclusions (NIH awards down 20.5%/21.8% by two metrics, a few points below the claimed 25% but same order of magnitude), independently arrived at via the same NIH primary source. Its genuinely distinctive contribution is checking whether the *corpus itself* could answer any of this — finding real, specific data gaps (`research.nih_award_projects` declares FY2022-2026 coverage but only FY2026 is actually loaded; no NSF award-count-by-year table exists anywhere; `research.nsf_herd_by_institution` has a 2-year publication lag, so JHU's exact FY2025 decline falls entirely outside its window) — and running a genuine diff-in-differences check on JHU's pre-2025 federal R&D growth trajectory relative to 6 peer universities (p<0.001, n=40) as supporting context. It's ranked below expert only because it couldn't reach year-2025/2026 data from its own warehouse at all, so its verification of the disputed period rests entirely on the same external sources everyman and expert also used.

**everyman** is third: reaches essentially the same conclusions with good sourcing (Science/AAAS, STAT News, AAU, AIP.org) and adds one genuinely distinctive, concrete detail neither other answer has — NSF's specific funding-freeze mechanism (~$1B of NSF's $8.8B budget held in a central account inaccessible to grant programs) and named AAU examples (University of Kansas -$182M YoY, MIT >20% down) — but it doesn't catch the NIH metric-conflation issue the way expert does, presenting the "~25%" figure as "directionally correct... under one specific metric" without identifying that the commonly-cited figure actually blends two different measurement periods.

## Severity check

askamerica ranks second, above everyman — no severity flag.

## Recipe / resolution check

The `research.nih_award_projects` coverage gap askamerica hit is already tracked — and it's a regression: `kenstott/govdata-ops#172`/`#173` fixed this exact gap (FY2022-2025 backfill) on 2026-09-12, but `#610` (filed 2026-09-26, same day as this check) confirms the backfill "is gone — only FY2026 loaded" again, with `#646` covering the same symptom. The NSF award-count-by-year absence is a separate, real gap without an obvious existing issue — worth filing if it recurs on a future run.
