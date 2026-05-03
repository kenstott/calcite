#!/usr/bin/env python3
"""
check-release-window.py  <schema_yaml>  <table_name>  [--force]

Reads the `releaseWindow:` block from the named table in a GovData schema YAML
and exits 0 (proceed) or 1 (skip) based on today's date.

releaseWindow fields (all optional; absent = no constraint on that dimension):
  months:     list of calendar months that have new data  (1=Jan … 12=Dec)
  yearParity: "odd" or "even" — further constrains to odd/even calendar years
  dow:        list of days-of-week that should run (0=Sun, 1=Mon … 6=Sat)

Exit codes:
  0  proceed  — within window, no window defined, or --force
  1  skip     — outside window; caller should fast-exit without doing any work
"""
import sys
import datetime

try:
    import yaml
except ImportError:
    # PyYAML missing — fail open so callers are never silently blocked
    print("[release-window] WARNING: PyYAML not installed — proceeding for all tables", file=sys.stderr)
    sys.exit(0)


def find_table(schema: dict, name: str) -> dict | None:
    for section in ("partitionedTables", "tables"):
        for tbl in schema.get(section, []):
            if tbl.get("name") == name:
                return tbl
    return None


def check(schema_yaml: str, table_name: str, force: bool) -> int:
    if force:
        print(f"[release-window] --force: bypassing window for {table_name}")
        return 0

    with open(schema_yaml) as f:
        schema = yaml.safe_load(f)

    table = find_table(schema, table_name)
    if table is None:
        print(f"[release-window] '{table_name}' not found in schema — proceeding", file=sys.stderr)
        return 0

    window = table.get("releaseWindow")
    if not window:
        return 0  # No window defined: always proceed

    today = datetime.date.today()

    # Year-parity check
    parity = window.get("yearParity")
    if parity == "odd" and today.year % 2 == 0:
        print(f"[release-window] skipping {table_name} — odd-year source, {today.year} is even")
        return 1
    if parity == "even" and today.year % 2 != 0:
        print(f"[release-window] skipping {table_name} — even-year source, {today.year} is odd")
        return 1

    # Month check
    months = window.get("months")
    if months and today.month not in months:
        print(f"[release-window] skipping {table_name} — outside release months {months} (today: month {today.month})")
        return 1

    # Day-of-week check — convention: 0=Sunday (matches bash `date +%w`)
    dow = window.get("dow")
    if dow:
        # Python: isoweekday() 1=Mon..7=Sun; convert to 0=Sun..6=Sat
        today_dow = 0 if today.isoweekday() == 7 else today.isoweekday()
        if today_dow not in dow:
            print(f"[release-window] skipping {table_name} — not a run day (days={dow}, today DOW={today_dow})")
            return 1

    return 0


def main():
    if len(sys.argv) < 3:
        print("Usage: check-release-window.py <schema_yaml> <table_name> [--force]", file=sys.stderr)
        sys.exit(1)

    schema_yaml = sys.argv[1]
    table_name = sys.argv[2]
    force = "--force" in sys.argv

    sys.exit(check(schema_yaml, table_name, force))


if __name__ == "__main__":
    main()
