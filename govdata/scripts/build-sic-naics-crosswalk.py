#!/usr/bin/env python3
"""Generate the checked-in SIC->NAICS-sector crosswalk used to backfill
epa_facilities.naics_codes when the EPA ECHO Exporter feed carries a SIC
classification but no NAICS one (govdata-ops#694).

Source: Census Bureau's official 1997 NAICS to 1987 SIC concordance
(https://www.census.gov/naics/concordances/1997_NAICS_to_1987_SIC.xls), the
authoritative SIC<->NAICS crosswalk. A SIC code frequently splits across
several precise 6-digit NAICS codes (a real many-to-many mapping — SIC is
coarser than NAICS), but the resulting NAICS codes usually still share a
single 2-digit sector. This script keeps only the SIC codes where every
mapped NAICS code agrees on the 2-digit sector, and emits that sector as the
derived value; a SIC code whose NAICS candidates span more than one sector is
dropped rather than guessed at, since EpaFacilitiesNaicsBackfillTransformer
must not report an approximate NAICS as source-native.

Requires the third-party `xlrd` package (the source file is legacy OLE2 .xls,
not parseable with the stdlib zipfile/ElementTree approach build-refs.py uses
for modern .xlsx) — `pip install xlrd`. This is a one-time/occasional
generation step; the checked-in JSON has no runtime dependency on xlrd.

Usage:  python3 build-sic-naics-crosswalk.py
Writes: ../src/main/resources/ref/sic-naics-sector-crosswalk.json
"""
import json
import os
import urllib.request
from collections import defaultdict

import xlrd

CROSSWALK_URL = "https://www.census.gov/naics/concordances/1997_NAICS_to_1987_SIC.xls"
NAICS_COL = 0
SIC_COL = 3
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

OUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "src", "main", "resources", "ref", "sic-naics-sector-crosswalk.json",
)


def normalize_sic(raw):
    if raw is None or raw == "":
        return None
    if isinstance(raw, float):
        return str(int(raw)).zfill(4)
    return str(raw).strip().zfill(4)


def main():
    req = urllib.request.Request(CROSSWALK_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()

    workbook = xlrd.open_workbook(file_contents=data)
    sheet = workbook.sheet_by_index(0)

    sic_to_sectors = defaultdict(set)
    for r in range(1, sheet.nrows):
        row = sheet.row_values(r)
        naics_raw = row[NAICS_COL]
        sic = normalize_sic(row[SIC_COL])
        if not naics_raw or sic is None:
            continue
        naics_sector = str(int(naics_raw))[:2]
        sic_to_sectors[sic].add(naics_sector)

    crosswalk = {
        sic: next(iter(sectors))
        for sic, sectors in sic_to_sectors.items()
        if len(sectors) == 1
    }
    dropped = len(sic_to_sectors) - len(crosswalk)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(crosswalk, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    print("wrote {} unambiguous SIC->NAICS-sector entries to {} ({} dropped as "
          "multi-sector/ambiguous)".format(len(crosswalk), OUT_PATH, dropped))


if __name__ == "__main__":
    main()
