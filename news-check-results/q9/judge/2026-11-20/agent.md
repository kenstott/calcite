# Judge — news q9 (politicalwire-nyt-federal-workforce-17-percent) — 2026-11-20

Rank: [expert, askamerica, everyman]

Story: Political Wire, "Federal employees look to Democrats for a lifeline" (2026-11-15), excerpting the New York Times. Central claim: Trump's cuts have been "shearing 17 percent of employees." Verdicts: expert 2 Pinocchios, everyman 2, askamerica 0 (engine).

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **expert** — Tested the central figure against OPM's own data: net change since 2025-01-20 of -258,108 (through September 2026, partly incomplete), about 11.2% of a ~2.31M base, consistent with GAO (>11%) and Pew (~10%). It also reconciled where 17% comes from (389,000 departures in the Boston Globe copy, about 16.8% of the same base), so the figure is gross departures read as net shrinkage. Its 2 Pinocchios reflect that the piece's one number misleads as written. Worked from summaries and did not reach the NYT original.

2. **askamerica** — Found the Times text through the Boston Globe's syndicated copy (word-for-word match with the excerpt) and reached the same gross-versus-net explanation (389,000 departures ≈ 16-17%; OPM headcount via Fox Business ≈ 12% net). It flags the framing. But it grades the central claim "mostly true" and the piece 0 Pinocchios, and its net figure rests on a secondary citation because it concluded "the warehouse has no federal-government employment series." That is wrong: `officials.federal_workforce_by_agency` holds OPM monthly headcount 2010-2026 and answers the question directly. `calls.jsonl` shows two `search_catalog` calls ("federal government employment total nonfarm CES", "federal government employment CES"); neither returns the table (replayed today, limit 15).

3. **everyman** — Right on direction (OPM ~10% net, GAO 11.3%) and 2 Pinocchios, but says it "found no source" for 17% and did not reconcile it; claims are checked from summaries.

askamerica vs everyman: askamerica explains the 17% figure and reads the actual Times text; everyman's verdict is closer to the expert's but its reasoning is thinner. askamerica places above everyman, not by a wide margin. What would put askamerica first: querying the warehouse OPM table it holds, which gives the exact net change with no secondary source.

## Severity check

askamerica ranks above everyman. No severity flag. The loss to expert is a connector cost: the product held the answer and its catalog search hid it.

## Pinocchio shape and gaps

claims.json: 3 claims, all `author_claims` (the excerpt has no quoted speaker); compliant. Stated gap ("no federal-government employment series") is false; not filed as sourcing.

## Resolution (one)

Filed kenstott/govdata-ops#1376 (`type:defect`, `kind:wrong`, `schema:officials`, `status:open`): `search_catalog` misses `officials.federal_workforce_by_agency` for "federal government employment" phrasings (returns it for "federal workforce headcount"), with the measured query table and a suggested comment/ranking fix. No YAML or engine change made in this headless run; the fix needs a seed rebuild.

## Recipe check

No recipe: the miss is discoverability, not method.
