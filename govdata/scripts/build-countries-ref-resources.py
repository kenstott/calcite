#!/usr/bin/env python3
"""Generate the checked-in ISO enrichment resource for the ref.countries table.

country codes are extremely stable, so the ISO "spine" (alpha-3, numeric, FIPS,
M49 region/subregion/continent, currency) is baked into a committed resource
rather than fetched at ETL time. The ref.countries ETL table fetches only the
Census Schedule C code list live (freshness-gated) and enriches each row from
this file via CountriesReferenceTransformer.

Source: DataHub `datasets/country-codes` (Public Domain / PDDL), keyed by ISO
3166-1 alpha-2. Re-run this script to refresh the spine after a country change
(e.g. a new ISO assignment); commit the regenerated JSON.

Usage:  python3 build-countries-ref-resources.py
Writes: ../src/main/resources/ref/country-enrichment.json
"""
import csv
import io
import json
import os
import urllib.request

DATAHUB_URL = (
    "https://raw.githubusercontent.com/datasets/country-codes/master/"
    "data/country-codes.csv"
)

OUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "src", "main", "resources", "ref", "country-enrichment.json",
)


def blank_to_none(value):
    if value is None:
        return None
    v = value.strip()
    return v if v else None


def main():
    with urllib.request.urlopen(DATAHUB_URL, timeout=60) as resp:
        text = resp.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    enrichment = {}
    for row in reader:
        iso2 = (row.get("ISO3166-1-Alpha-2") or "").strip()
        if not iso2:
            continue  # entries without an alpha-2 code cannot key a join
        # Only the stable ISO "spine" (codes + geography + currency) lives here,
        # keyed by ISO alpha-2. The human-readable country_name is taken from the
        # live Census Schedule C row instead (clean, trade-oriented, BEA-aligned
        # names), so it is intentionally NOT duplicated in this file.
        enrichment[iso2] = {
            "iso_alpha3": blank_to_none(row.get("ISO3166-1-Alpha-3")),
            "iso_numeric": blank_to_none(row.get("ISO3166-1-numeric")),
            "fips": blank_to_none(row.get("FIPS")),
            "currency_code": blank_to_none(row.get("ISO4217-currency_alphabetic_code")),
            "region": blank_to_none(row.get("Region Name")),
            "subregion": blank_to_none(row.get("Sub-region Name")),
            "intermediate_region": blank_to_none(row.get("Intermediate Region Name")),
            "continent": blank_to_none(row.get("Continent")),
            "official_name": blank_to_none(row.get("official_name_en")),
        }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(enrichment, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    print("wrote {} ISO entries to {}".format(len(enrichment), OUT_PATH))


if __name__ == "__main__":
    main()
