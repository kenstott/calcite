#!/usr/bin/env python3
"""
AskAmerica / GovData JDBC driver test — uses the shared calcite_driver_test harness.

Usage:
  python scripts/test_govdata_python.py [path/to/askamerica-engine*.jar]
"""

import sys
import os
import glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
    "../../driver-base/src/main/python"))

from calcite_driver_test import DriverTestHarness


def find_jar():
    here = os.path.dirname(os.path.abspath(__file__))
    calcite_root = os.path.dirname(os.path.dirname(here))

    engine = os.path.join(calcite_root, "askamerica-engine", "build", "libs",
                          "askamerica-engine*.jar")
    matches = [m for m in glob.glob(engine)
               if "-sources" not in m and "-javadoc" not in m]
    if matches:
        return sorted(matches)[-1]

    govdata = os.path.join(calcite_root, "govdata", "build", "libs",
                           "calcite-govdata-*-all.jar")
    matches = glob.glob(govdata)
    if matches:
        print("  (using full govdata JAR — "
              "run ./gradlew :askamerica-engine:shadowJar for the engine JAR)")
        return sorted(matches)[-1]

    raise FileNotFoundError("No JAR found. Run: ./gradlew :askamerica-engine:shadowJar")


jar_path = sys.argv[1] if len(sys.argv) > 1 else find_jar()

harness = DriverTestHarness(
    jar_path         = jar_path,
    driver_class     = "org.apache.calcite.adapter.askamerica.AskAmericaDriver",
    url              = "jdbc:askamerica:source=sec",
    expected_product = "AskAmerica",
    expected_driver  = "AskAmerica JDBC Driver",
    expected_url_prefix = "jdbc:askamerica:",
    test_schema      = "sec",
    test_table       = "filing_metadata",
    test_columns     = ["cik", "company_name", "filing_type"],
    test_query       = ("select cik, company_name, filing_type from sec.filing_metadata "
                        "order by filing_date desc fetch first 3 rows only"),
    test_param_col   = "filing_type",
    test_param_val   = "10-K",
)

harness.run_all()
