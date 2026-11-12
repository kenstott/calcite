# Judge: OPB "Fewer fires, record damage: Oregon's 2026 wildfire season by the numbers" fact-check (q5), 3-persona 2026-11-08

Rank: [askamerica, expert, everyman]

Source: https://opb.org/article/2026/10/24/oregon-2026-wildfire-season-by-the-numbers (Casey Frizzell,
OPB, 2026-10-24, updated 10-26). All three read it, the web arms only as a tool summary.

**Engine build:** main at 382db82223 (installed). Data server: pgwire-govdata 0.124.4.

## Why

All three found the same weak point. The article's "more than 2.1M acres, about 13% above the 2024
record of 1.9M" does not add up:
- 2.1 / 1.9 is +10.5%;
- OPB's own Sept 1 piece put the total at 2.5M by Aug 20.

`expert` and `askamerica` graded the piece 2 Pinocchios; `everyman` gave 1, provisionally.

**`askamerica`** was the only persona to measure acreage from primary data. It used
`disasters.wildfire_perimeters` (NIFC WFIGS), Oregon incidents only:
- **Acreage:** 2.54M acres in 2026 against 2.01M in 2024, so the record holds and is about +26%. It
  flagged that perimeter sums can double-count overlaps, so the figure is an upper bound.
- **Top five fires:** confirmed they make up about 57% of the acres (1.44M of 2.54M).
- **USDA loans, its distinctive find:** the article says farmers in 18 counties are eligible. The
  USDA FSA release lists 24 counties (9 primary, 15 contiguous), and the designation is for
  storms/flooding with drought criteria, not wildfire. It rated the claim mostly false.
- **Other confirmations:** both firefighter deaths (the flag-lowering order covers only Gutierrez
  Toral), the record-low snowpack, and the climatologist's statement.

It called the $123M Emergency Board funding "not checkable"; `expert` did confirm it.

**`expert`** worked from secondary reporting of NWCC/ODF figures (2.336M by Aug 9, 2.08M for 2024)
and caught the same arithmetic problem. It confirmed:
- the $236.2M cost and +$35.3M margin;
- the $123M Emergency Board split;
- the $150M treasury borrowing (HB 3940);
- the Bench and Grasshopper home counts.

It could not verify the firefighter names, and it did not check the USDA loan claim.

**`everyman`** found the arithmetic problem and the $123M wording ("Legislature" vs. the Emergency
Board), but left more claims unverified than either other persona.

**Ranking:** `askamerica` first, for an independent acreage measurement from federal perimeter data
and the USDA eligibility error no one else caught. `expert` second. `everyman` third.

## Severity check

`askamerica` ranked #1, so no severity flag.

## Gate check

The wording gate fired on the first `preview_report`. `create_report_artifact` was then refused
twice, first on 5 issues and then on 2. It passed on the third attempt.

## Data check

`describe_table` failed again, this time reported as "THE OBJECT STORE DID NOT ANSWER IN TIME …
stalled step: describe_table step 'primary key'". This is the same primary-key step as
kenstott/govdata-ops#1129, on the 0.124.4 data server. The fix (#760, in main at 5db73b4e94) is not
in a released bundle yet.

## Recipe check

None needed; `askamerica` ranked #1.

## Gap audit

None. ODF fire counts, ODEM evacuation counts and NWCC homes-destroyed counts are state/interagency
operational figures with no warehouse equivalent.
