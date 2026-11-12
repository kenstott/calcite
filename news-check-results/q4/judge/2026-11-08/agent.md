# Judge: Newsweek "SNAP Enrollment Drops 300k In Single Month—Here Are Worst Impacted States" fact-check (q4), 3-persona 2026-11-08

Rank: [expert, askamerica, everyman]

Source: https://www.newsweek.com/snap-enrollment-drops-300k-single-month-here-are-worst-impacted-states-12466361
(Aliss Higham, Newsweek, 2026-09-21). It reports USDA's initial June 2026 SNAP participation release.
All three read it.

**Engine build:** main at 8087c200ed (installed). Data server: pgwire-govdata 0.124.4.

## Why

All three gave 1 Pinocchio.

**`expert`** pulled USDA FNS's own files directly with curl: the June 2026 "birdseye" report plus
`snap-persons-10.xlsx` and `snap-4fymonthly-10.xlsx` (data as of 2026-10-09). From them it:
- **Confirmed the headline figures:** 36.353M in June, 332k (0.90%) down from May, 5.32M (12.8%)
  down from June 2025.
- **Found the revision:** USDA's revised series puts the June drop at about 205k. "300k in a single
  month" is true only for the initial release.
- **Checked the state year-over-year rankings** against the October table.
- **Partly contradicted the North Dakota claim.**
- **Counted 15 consecutive declines,** not 16.

It wrongly accepted the article's "59 to 64" work-requirement age change, from memory.

**`askamerica`** caught the article's one clear factual error: before OBBBA the upper age was 54, not
59. It also:
- **Tied the article's state numbers to warehouse values:** for Illinois, June 2025 minus the
  year-over-year drop equals May 2026 plus the monthly change, exactly. Arizona, Texas and Florida
  tie the same way.
- **Confirmed Alaska and North Dakota** were above their year-earlier levels.
- **Flagged Georgia's level as a likely month mislabel or revision.**
- **Could not check any headline June 2026 figure,** because the warehouse has no June data (see
  the data check).

**`everyman`** worked from FRAC's republication of USDA data. It confirmed the national June figures
and also accepted the age claim.

**Ranking:** `expert` first. It verified the article's core claims, the national June figures,
against USDA's primary files, and found the revision that undercuts the headline. `askamerica`
second. It found the one outright error, but the missing June data left it unable to check the
headline. `everyman` third.

## Severity check

`askamerica` ranked above `everyman`, so no severity flag. Ranking below `expert` is normal.

## Gate check

The wording gate fired again: the first preview was refused because "the question says
"Fact-check"". Later previews were refused, each in turn, by:
- the linked-sources gate (3 unread Newsweek links);
- the coverage gate (3 unclaimed figures: 300,000, $385, 332,000).

The fourth preview passed.

## Data check (verified live)

1. **`fiscal.snap_benefits_by_geography` has June–September 2026 rows with NULL
   `persons_participating`** for all 56 geographies, so the latest populated month is May 2026.
   USDA had published June (birdseye) and revised data through July by 2026-10-09. This is a
   freshness lag behind the source.
   - `year` is the fiscal year: FY2026 months 10–12 are Oct–Dec 2025 (Oct 41,096,660).
   - The query was:
     `SELECT "year","month",count(*),count(persons_participating),sum(persons_participating) FROM
     fiscal.snap_benefits_by_geography WHERE "year">=2026 GROUP BY "year","month"`
2. **`describe_table` fails for every table on this machine,** with "No enum constant
   org.apache.calcite.sql.fun.SqlLibrary.BIGQUERY".
   - Reproduced on `fiscal.snap_benefits_by_geography`, `econ.fred_indicators`,
     `health.cdc_county_overdose_deaths` and `econ.ces_revision_vintages`.
   - The data server's `spawn.log` shows it failing on the
     `information_schema.key_column_usage` lookup. The first occurrence is 08:15 today, right after
     this machine moved to the 0.124.4 bundle.
   - The same call succeeded in 11-08 q1, which ran on bundle 0.118.0.
   - Filed as kenstott/govdata-ops#1129.

## Recipe check

None. One loss to `expert`, caused by data freshness, not a recipe gap.

## Gap audit

SNAP freshness, described above under the data check. Filed as kenstott/govdata-ops#1128.
