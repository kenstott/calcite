# Fact-check: Boston Globe, 2026-11-12, NH historic wildfire risk

**Rating: 1 Pinocchio (some shading).** 18 claims/quotes: 11 verified, 3 verified but needing context, 4 not checkable here, 0 contradicted.

## Step 1. Claims
**Answer**
- Drought: verified. US Drought Monitor NH (week of Nov 10): D0+ 93.40%, D1+ 40.46% (Nov 3: 93.48 / 40.46; Oct 27: 93.48 / 32.76). Globe: ">90%", "about 40%".
- Size: "nearly 1,000 acres" / "982 at one point per InciWeb" shaded. USFS remapped to 856 (Nov 5), 821 (Nov 8-9), InciWeb 817 on Nov 12. 982 is from a tracker (Fire Nearby); Union Leader reported 921.
- Containment "nearly 60%": 59% (Nov 11-12). Started Nov 1: verified.
- Largest in WMNF since 1918: verified; Forest Supervisor Ibarguen said "unofficially"; Globe drops hedge. Forest created May 16, 1918.
- 87 people Wednesday: InciWeb/WMTW Nov 11 listed 87; Nov 12 lists 58 (stale but date-correct).
- Cause under investigation: USFS says human-caused, under investigation; InciWeb says undetermined.
- Closures: order R9-22-26-08 (Nov 4) in effect.
- Typical year about 250 acres: NH DFL about 250 fires / about 250 acres; Sherman said 220-250.
- 1941 Marlow-Stoddard, 4 days, 24,000 acres: verified (some sources 27,000).
- Matson title: NHPR/Keene Sentinel say chief of forest protection; state page still lists Steven Sherman.
- Guiterman CU Boulder / Dartmouth affiliate: verified (CIRES, Dartmouth Irving).
- Not checkable: Matson quotes, Evans (Shelburne) quotes, Guiterman quotes, 127 fires / 180 acres (no 2026 state YTD source; 2025 was 157 fires / 146 acres per Fremont town report).

**Key values:** 817 vs about 1,000 acres; 180/250 = 0.72x.
**Sources:** InciWeb, USFS WMNF, USDM API (usdmdataservices.unl.edu, aoi=33), WMTW, NHPR, Concord Monitor, WCAX, WMUR, NH DFL, Forest History Society, CIRES.
**Confidence:** high on drought/containment/size history; low where only the Globe is the source.

## Step 2. Misleading framing
**Answer:** "This year the number has outstripped [250 acres] four times over" holds only if the Wild River Fire is included: (180+817)/250 = 4.0x, (180+982)/250 = 4.6x. The other 127 fires (180 acres, unverified) are 0.72x the average, below normal; the sentence does not say so.
**Confidence:** medium (127/180 unverified).

## Gaps
Globe text came via a fetch summarizer, not raw text. Union Leader returned 403. Territories not applicable.

Deliverables: agent-dashboard.png, agent-report.html in the same directory.
