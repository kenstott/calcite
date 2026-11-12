# Judge: IBTimes, "The U.S. Just Recorded A Record Drop In Homelessness. The Government Is Claiming Credit." (q3), 3-persona 2026-11-09

Rank: [askamerica, expert, everyman]

Source: https://www.ibtimes.com/hud-2026-pit-count-homelessness-decline-3809530
(Raphael McMahon, Nov. 7, 2026).

**Engine build:** `~/.askamerica/engine/askamerica-engine.jar`, from 14b2ddacce (includes the
office-page gate). **Data server:** the patched local overlay on port 45533 with the version-2
catalog cache.

## Judge's own verification

- **The Ruege attribution:** I had the article and the report read in full. The article says Adam
  Ruege "speculated" about "increased civic cooperation" and a "cooling housing market". His report
  (https://community.solutions/research-posts/2026-overall-homelessness-estimates/, Nov. 6) contains
  none of "civic cooperation", "cooperation", "cooling" or "housing market", and offers no cause for
  the decline. It could come from an interview I did not look for, so "unsupported" is the right
  grade and "false" would not be.
- **Arithmetic:** 497,256 / 348,630 is +42.6%, and 698,425 against 745,652 is -6.33%. I did not
  re-run the warehouse queries; the data server had stopped by the time of judging.
- **The 745,652 baseline** rests on press and search coverage in all three arms. HUD's release gives
  no prior-year count, per `expert`.
- **New York Times "nearly half of the drop":** no arm could read it, and I did not try.

## Why

**`askamerica` is first.** It is the only arm to test the article against primary data: 771,480
for 2024, and the 43% shelter rise computed as 42.6% from 348,630 to 497,256, from
`housing.hud_pit_counts_by_state`, plus state-level sheltered counts for the four named places. It
read HUD, NAEH and Community Solutions directly, and caught the one attribution the source does not
support. It alone noticed the article omits that Community Solutions' figure is preliminary and
extrapolated, and it reported Turner's "near a historic high" omission. It marked the NYT claim
not checkable after a 403. Weak spots: it graded Turner's causal claim "unsupported" while quoting
NAEH's opposite view as though that settled it, it did not state the two baselines numerically
(-6.3% against 2025, about -9.5% against 2024), and it admits fetching six related links only to
satisfy the engine's source check.

**`expert` is second.** Same 1 Pinocchio, with the two baselines stated and the causal limit put
well, and it read HUD's release. It queried no dataset (its shell had no network), so the 43% is
corroborated only by secondary summaries, and it missed the Ruege problem. It saw Community
Solutions only through relayed coverage.

**`everyman` is third.** It read HUD and gave the useful -9.5% baseline, but graded the 43%, Ruege
and Oliva "not checkable", and 2 Pinocchios rests mainly on the two baselines, which the article
states and labels with their years. It worked from a summary of the article.

## Severity invariant

`askamerica` ranks above `everyman`. Not triggered.

## Gate check

The office-page gate did not fire. The two 'unsupported' claims were the Ruege attribution, not a
government office, and Turner's causal sentence, whose wording does not name an office. `askamerica`
had read `hud.gov` regardless. This run is no evidence for or against the gate.

## Recipe check

`askamerica` is #1, so no recipe or instruction change is warranted. One freshness gap: the PIT
tables stop at 2024, filed as govdata-ops #1139.
