# Judge - q9 (sunset-window fatal crashes, autumn vs winter), 2026-11-23

**Story:** Travel And Tour World, 2026-11-22, relaying a Lerner & Rowe (law firm) study of 2024 FARS data: autumn has 28.3% more fatal crashes per day around sunset than winter (25.8 vs 20.1). Each persona fact-checked every claim and graded on 0-4 Pinocchios.

Rank: [expert, askamerica, everyman]

Fingerprint check: everyman, askamerica and expert all pass. None cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Rating | 2 Pinocchios | 0 (engine-computed from claim scores) | 0 on accuracy, framing flagged separately |
| Original study | not found (two searches, WebFetch summary only) | fetched lernerandrowe.com study; compared article to it, claim by claim | not located; stated so |
| Recompute from FARS | none ("microdata could not be queried") | warehouse `transport.fatal_crashes`, whole-hour times | full NHTSA FARS 2024 national CSV, minute-level |
| Total in window | none | 8,163 vs article 8,334 (-2.1%) | 8,348 crashes / 8,934 deaths (+0.2% / +0.1%) |
| Autumn vs winter | arithmetic only (25.8/20.1 = 1.284) | 25.2 vs 19.7, +28% | 26.0 vs 20.1, +29.4% |
| After vs before sunset | not checked | about +46% vs 57.8% (unresolved) | +58.5% vs 57.8% |
| Nov vs Jan | arithmetic only (+43.7%) | study and article agree; arithmetic +43.7% vs stated 43.4% | 27.7 vs 18.9 (article 27.3 vs 19.0) |
| Data-quality finding | n/a | study says 28.4%, article says 28.3% (rounding) | 27.3/19.0 is +43.7%, not 43.4% |
| Framing flags | no exposure data, mixed clock hours, law-firm source | no exposure data, commute overlap, law-firm source undisclosed, one year only | no exposure data, commute and clock change |
| Sunset/DST handling | none | solar formula, longitude-based zones, AZ/HI excluded from DST | solar formula, own timezone approximation |
| Missing-value handling | n/a | dropped null hour/lat/lon; disclosed; not rerun without them | n=36,297 crashes, 35,894 valid time and location, stated |

## Ranking rationale

**1. expert (narrowly).** It reproduced the figures most closely. Total window crashes land within 0.2% of the article (8,348 vs 8,334) and deaths within 0.1%. The after/before-sunset gap is +58.5% against the article's 57.8%, and the January after-sunset share (72.4% vs 72.2%) matches. Its method is the stronger one: minute-level crash time from the full NHTSA file, a stated valid-n, a stated season definition (meteorological, about 91-day autumn), and a named approximation for sunset and time zone. Its autumn/winter ratio is +29.4%, a point above the article's 28.3%. Its summary says "accurate" but also says "medium on decimals", which is fair. Its weakness is provenance: it never located the Lerner & Rowe study, so every article figure was checked against its own recompute, not against the study. It also left the pedestrian-concentration claim untested, and said so.

**2. askamerica.** It had one advantage the other two lacked. It found and fetched the original study. That let it show that the article's figures match the study and catch the small 28.4% (study) vs 28.3% (article) mismatch. It also gave the strongest account of source quality: a law firm's marketing piece that the article does not label as such. Its warehouse recompute is directionally right: season ordering matches, and the headline ratio is +28%. It is imprecise on counts. Its total is 8,163 vs 8,334 (-2.1%), and the post-vs-pre gap is about +46% vs 57.8%, a 12-point miss. The headline ratio landing near 28% is partly luck, since the count error is spread across seasons. Its SQL (seq 7 in `calls.jsonl`) uses `hour+0.5` as the crash time, with a window of sunset plus or minus 2 h. It filters `hour IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL`. A whole-hour midpoint shifts crashes across the sunset boundary, which fits the pre/post miss. It did not rerun without the exclusions, and said so. The answer is honest about this, but the 12-point gap was left unexplained. It also lets "the 57.8% check" stand as verified against the study while its own figure disagrees. It is therefore a weaker reproduction than the expert's. It ranks above everyman because it recomputed, found the source, and disclosed its limits. It ranks below expert on reproduction and method.

**3. everyman.** It checked arithmetic only (25.8/20.1 = 1.284; 27.3/19.0 = +43.7% vs 43.4%; 2,349/25.8 = 91 days), and it flagged the framing well: no exposure measure, season mixed with clock time and darkness, and a law-firm source. It could not find the study or touch FARS, so every substantive figure is "not checkable". The 2 Pinocchios rating is a hedge from unverified numbers and weak framing. This is the floor.

## Why askamerica is not #1

askamerica beats everyman, so there is no severity defect. It loses narrowly to expert because the connector's table has only whole-hour time. The question depends on a boundary (two hours either side of sunset), and the table cannot place a crash relative to it. The expert's file has minute-level time and reproduced the figures within 1%. The study-fetch advantage is real and is why this is close.

## Warehouse table check

`transport.fatal_crashes` is defined in `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/govdata/src/main/resources/transport/transport-schema.yaml` (line 577). Its time columns are `crash_date` and `hour` only. `hour` is `TRY_CAST(src."HOUR")` with 99 mapped to null. There is no minute column. `grep -i minute` on the schema file returns only airline and network-wait lines. The source is `accident.csv` from the FARS National CSV, which carries a MINUTE field. So minute-level time exists in the source file but is dropped at ingest. askamerica's imprecision is therefore a warehouse data gap, not a method error. Two other contributors on this table: `latitude` and `longitude` nulls (sentinel values mapped to null) shrink the sample, and askamerica's n of 8,163 vs the expert's 8,348 includes this effect.

## What would change the ranking

- If the warehouse carried minute-level time and askamerica reproduced the totals within 1%, it would rank first on provenance (study found and compared) plus reproduction.
- If the judge weighted provenance above numeric reproduction, askamerica would edge ahead, since the expert never found the study. This is a close call.
- If the expert's +29.4% vs 28.3% gap were counted as a failure to match, the expert would lose most of its lead. The expert called it a match; the gap is about one point.

## Severity invariant

askamerica ranks above everyman. No defect flag. A new run should keep that order whatever the exact figures.

## Recipe check

Existing recipe `fact-check-a-news-article` (recipes.json, line 1380, about 11k characters) already covers extracting every claim, fetching the article, and testing at the article's grain. Its triggers include "fact-check", "pinocchio" and "news article". I searched `recipes.json` for "fars", "sunset" and "crash": no recipe names this pattern, and the only "fatal" hit is the BLS 403 recipe, which is unrelated. This run's gap is not a recipe gap. askamerica followed the process (found the study, queried the warehouse, disclosed exclusions) and was limited by the table, not by its approach. The expert's closer reproduction came from a column the warehouse lacks, so a recipe cannot reach it. A recipe telling askamerica to "rerun without the exclusions" would be marginal: the unexplained pre/post gap would remain unless time had minute grain.

**Resolution (one): issue.** Add a `minute` column to `transport.fatal_crashes` in `transport-schema.yaml`, from `src."MINUTE"` in FARS `accident.csv`, with 99 mapped to null as is done for `hour`. This is a type:defect / schema-gap issue (kind: missing column, schema: transport). Evidence: askamerica's whole-hour recompute missed the article's after/before gap by 12 points (46% vs 57.8%), while the expert's minute-level recompute landed at 58.5%.
