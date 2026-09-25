#!/usr/bin/env python3
"""Read-only check that each Iceberg table's metadata directory holds ONE table lineage.

version-hint.text is the only authority a reader has, so a directory that mixes two table
lineages (different table-uuid) loads whichever one the hint names. For every table this compares
the table-uuid at the lowest version, at the hint, and at the highest version, and flags any table
where they differ. Nothing is written.

  iceberg-lineage-scan.py --alias prod --bucket govdata-parquet-v1            # every table
  iceberg-lineage-scan.py --alias r2 --table disasters/wildfire_perimeters --segments
  iceberg-lineage-scan.py --alias prod --table disasters/wildfire_perimeters --closure 45

--alias is an mc alias (MC_HOST_<alias> in the environment). --segments prints each contiguous run
of versions per uuid with commit time, column count and total-records. --closure V checks that the
manifest list, manifests and data files of the current snapshot at version V all exist (needs
fastavro).
"""
import argparse
import datetime
import json
import re
import subprocess
import sys

VERSION_FILE = re.compile(r"^v(\d+)\.metadata\.json$")


def mc(*args):
    return subprocess.run(["mc", *args], capture_output=True, text=True, check=True).stdout


def mc_cat(path):
    return subprocess.run(["mc", "cat", path], capture_output=True, check=True).stdout


def names(path):
    return [line.split()[-1].rstrip("/") for line in mc("ls", path).splitlines() if line.split()]


def discover_tables(alias, bucket):
    tables = []
    for schema in names(f"{alias}/{bucket}"):
        if schema.startswith("_"):
            continue
        for table in names(f"{alias}/{bucket}/{schema}"):
            if "version-hint.text" in names(f"{alias}/{bucket}/{schema}/{table}/metadata"):
                tables.append(f"{schema}/{table}")
    return tables


def read_metadata(meta_dir, version):
    return json.loads(mc_cat(f"{meta_dir}/v{version}.metadata.json"))


def versions_of(meta_dir):
    found = []
    for name in names(meta_dir):
        m = VERSION_FILE.match(name)
        if m:
            found.append(int(m.group(1)))
    return sorted(found)


def summarize(metadata):
    snap = [s for s in metadata.get("snapshots", [])
            if s["snapshot-id"] == metadata.get("current-snapshot-id")]
    records = snap[0]["summary"].get("total-records") if snap else None
    schema = metadata["schemas"][-1] if "schemas" in metadata else metadata["schema"]
    committed = datetime.datetime.fromtimestamp(
        metadata["last-updated-ms"] / 1000, datetime.timezone.utc).strftime("%m-%d %H:%MZ")
    return metadata["table-uuid"][:8], len(schema["fields"]), records, committed


def scan_table(meta_dir, segments):
    versions = versions_of(meta_dir)
    hint = int(mc_cat(f"{meta_dir}/version-hint.text").decode().strip())
    probes = {"min": versions[0], "hint": hint, "max": versions[-1]}
    uuids = {k: read_metadata(meta_dir, v)["table-uuid"][:8] for k, v in probes.items()}
    mixed = len(set(uuids.values())) > 1
    if segments:
        runs = []
        for v in versions:
            uuid, cols, records, committed = summarize(read_metadata(meta_dir, v))
            if runs and runs[-1]["uuid"] == uuid:
                runs[-1].update(last=v, last_info=(cols, records, committed))
            else:
                runs.append({"uuid": uuid, "first": v, "last": v,
                             "first_info": (cols, records, committed),
                             "last_info": (cols, records, committed)})
        return hint, versions[-1], mixed, uuids, runs
    return hint, versions[-1], mixed, uuids, None


def closure(alias, bucket, table, version):
    import fastavro
    import io
    root = f"{alias}/{bucket}/{table}"
    listed = set()
    for line in mc("ls", "-r", root).splitlines():
        if line.split():
            listed.add(line.split()[-1])
    metadata = read_metadata(f"{root}/metadata", version)
    current = [s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"]]
    if not current:
        print(f"{table} v{version}: no current snapshot")
        return

    def rel(location):
        return location.split(f"/{table}/", 1)[1]

    manifest_list = current[0]["manifest-list"]
    missing = []
    if rel(manifest_list) not in listed:
        missing.append(rel(manifest_list))
    manifests = list(fastavro.reader(io.BytesIO(mc_cat(f"{root}/{rel(manifest_list)}"))))
    files = records = 0
    for manifest in manifests:
        path = rel(manifest["manifest_path"])
        if path not in listed:
            missing.append(path)
            continue
        for entry in fastavro.reader(io.BytesIO(mc_cat(f"{root}/{path}"))):
            if entry["status"] == 2:
                continue
            data_file = entry["data_file"]
            files += 1
            records += data_file["record_count"]
            if rel(data_file["file_path"]) not in listed:
                missing.append(rel(data_file["file_path"]))
    print(f"{alias}:{table} v{version}: manifests={len(manifests)} data_files={files} "
          f"records={records} missing={missing}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--alias", required=True)
    ap.add_argument("--bucket", default="govdata-parquet-v1")
    ap.add_argument("--table", action="append", help="schema/table; repeatable; default all")
    ap.add_argument("--segments", action="store_true")
    ap.add_argument("--closure", type=int, metavar="VERSION")
    args = ap.parse_args()

    if args.closure is not None:
        if not args.table or len(args.table) != 1:
            sys.exit("--closure needs exactly one --table")
        closure(args.alias, args.bucket, args.table[0], args.closure)
        return

    tables = args.table or discover_tables(args.alias, args.bucket)
    flagged = 0
    for table in tables:
        meta_dir = f"{args.alias}/{args.bucket}/{table}/metadata"
        hint, top, mixed, uuids, runs = scan_table(meta_dir, args.segments)
        if not mixed and not args.segments:
            continue
        flagged += mixed
        print(f"{'MIXED' if mixed else 'ok   '} {table} hint=v{hint} max=v{top} "
              f"uuid(min/hint/max)={uuids['min']}/{uuids['hint']}/{uuids['max']}")
        for run in runs or []:
            print(f"    uuid {run['uuid']} v{run['first']}..v{run['last']} "
                  f"first(cols,rows,commit)={run['first_info']} last={run['last_info']}")
    print(f"{len(tables)} tables scanned, {flagged} mixed")


if __name__ == "__main__":
    main()
