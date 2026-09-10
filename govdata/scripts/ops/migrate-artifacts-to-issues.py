#!/usr/bin/env python3
"""Migrate the Govdata Defect Register and Remediation Ledger artifacts to GitHub issues.

Reads the saved HTML of the two artifacts, converts each entry to a GitHub issue, and either
writes them to a directory for review (default) or files them (--create).

    # 1. save both artifacts locally (Artifact action:"read" writes them to disk), then:
    ./migrate-artifacts-to-issues.py --register <register.html> --ledger <ledger.html> --out /tmp/plan
    # 2. review /tmp/plan/*.md and /tmp/plan/plan.tsv, then:
    ./migrate-artifacts-to-issues.py --register <register.html> --ledger <ledger.html> --create

Defect entries become `type:defect` issues keeping their D-NNN id in the title. Ledger units
become `type:remediation` sub-issues of the defect they serve. Infrastructure rows become
standalone `type:infra` issues. Resolved/withdrawn entries are created and then closed with the
matching `resolution:` label, so history stays searchable.

The Resolved Archive artifact is deliberately not migrated — see docs/ops/defect-tracking.md.
"""

import argparse
import json
import os
import re
import subprocess
import sys

from bs4 import BeautifulSoup, NavigableString, Tag

REGISTER_URL = "https://claude.ai/code/artifact/e5262dc4-e879-4bb4-9bea-d7c04c0b6d6d"
LEDGER_URL = "https://claude.ai/code/artifact/4a9d427b-f15f-4f5d-bd42-dd801e70edec"

# Register disposition -> (state, status label, resolution label)
DISPOSITION = {
    "open": ("open", "status:open", None),
    "investigating": ("open", "status:investigating", None),
    "partial": ("open", "status:partial", None),
    "pending": ("open", "status:ready-to-run", None),
    "pendingetl": ("open", "status:pending-etl", None),
    "pending-etl": ("open", "status:pending-etl", None),
    "watch": ("open", "status:watch", None),
    "resolved": ("completed", None, "resolution:fixed"),
    "capped": ("completed", None, "resolution:capped"),
    "withdrawn": ("not planned", None, "resolution:withdrawn"),
    "not-reproducible": ("not planned", None, "resolution:not-reproducible"),
    "notavailable": ("not planned", None, "resolution:not-available"),
}

# Ledger run status -> (state, status label, resolution label)
LEDGER_STATUS = {
    "succ-yes": ("completed", None, "resolution:fixed"),
    "succ-partial": ("open", "status:partial", None),
    "succ-pending": ("open", "status:running", None),
    "succ-no": ("open", "status:investigating", None),
    "run-no": ("open", "status:ready-to-run", None),
    "run-killed": ("open", "status:investigating", None),
}

CATEGORY = {"wrong": "kind:wrong", "gap": "kind:gap", "core": "kind:core"}

# Pool slot type -> govdata schema. Slots are not schemas: sec runs as two slots, and a few
# slots are named for a work split rather than the schema they write to.
SLOT_TO_SCHEMA = {
    "sec_primary": "sec",
    "sec_secondary": "sec",
    "heal-sort": "health",
    "econ_reference": "econ_reference",
}

SCHEMAS = {
    "ag", "banking", "census", "cftc", "crime", "cyber_threat", "cyber_vuln", "disasters",
    "econ", "econ_reference", "edu", "energy", "environment", "fec", "fedregister", "fiscal",
    "geo", "health", "housing", "lands", "officials", "patents", "ref", "research", "sec",
    "transport", "weather",
}

# Text that means "this job is not safe to run yet" — see docs/ops/defect-tracking.md on why
# status:needs-dq exists. In the artifacts this was prose; here it decides a label, and every
# match is reported so it can be reviewed rather than trusted blindly.
NEEDS_DQ = re.compile(
    r"do not run this against production|PREREQUISITE|DQ-bucket first|"
    r"DQ-bucket run with a private jar must confirm|has never been exercised against real data",
    re.I,
)


# --------------------------------------------------------------------------- html -> markdown

def md(node, depth=0):
    """Convert a fragment of the artifact's HTML to markdown. Handles the tags the two
    artifacts actually use; anything else degrades to its text."""
    if node is None:
        return ""
    if isinstance(node, NavigableString):
        return re.sub(r"\s+", " ", str(node))
    if not isinstance(node, Tag):
        return ""

    name = node.name
    inner = "".join(md(c, depth + 1) for c in node.children)

    if name == "code":
        return "`" + inner.strip() + "`"
    if name in ("strong", "b"):
        return "**" + inner.strip() + "**"
    if name in ("em", "i"):
        return "_" + inner.strip() + "_"
    if name == "br":
        return "\n"
    if name == "p":
        return inner.strip() + "\n\n"
    if name == "li":
        return "- " + inner.strip() + "\n"
    if name in ("ul", "ol"):
        return "\n" + inner + "\n"
    if name == "a":
        href = node.get("href", "")
        text = inner.strip()
        if href.startswith("#D-"):
            # Intra-register cross-reference. The target's issue number is not known until it
            # is created, so keep the human-readable id and let search resolve it.
            return text
        if href.startswith("http"):
            return "[%s](%s)" % (text, href)
        return text
    return inner


def clean(text):
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# --------------------------------------------------------------------------- parsing

# The register was hand-edited over months and carries three markup generations. Newest uses
# .cell divs labelled by a .rowlabel; older ones name the section by class directly.
SECTION_CLASSES = [
    ("effect", "Effect as reported"),
    ("problem", "Effect as reported"),
    ("analysis", "Analysis"),
    ("solution", "Suggested solution"),
]


def extract_section(node, heading):
    """Pull evidence chips and the numbers grid out of a section, then render the rest."""
    evidence = [e.get_text(" ", strip=True) for e in node.select(".ev")]
    for e in node.select(".evidence"):
        e.extract()
    numbers = []
    for grid in node.select(".numbers"):
        spans = grid.find_all("span")
        for i in range(0, len(spans) - 1, 2):
            numbers.append((spans[i].get_text(" ", strip=True),
                            spans[i + 1].get_text(" ", strip=True)))
        grid.extract()
    return {
        "heading": heading,
        "body": clean(md(node)),
        "evidence": evidence,
        "numbers": numbers,
    }


def component_of(art, did):
    node = art.select_one(".defect .id") or art.select_one(".id")
    if node:
        return node.get_text(" ", strip=True)
    meta = art.select_one(".meta")
    text = meta.get_text(" ", strip=True) if meta else art.get_text(" ", strip=True)
    match = re.search(r"\b([a-z][a-z_0-9]*\.[a-z][a-z_0-9]*)\b", text)
    return match.group(1) if match else did


def parse_register(path):
    with open(path, encoding="utf-8") as fh:
        soup = BeautifulSoup(fh.read(), "html.parser")

    summaries = index_summaries(soup)
    out = []
    for art in soup.select("article.row[data-defect-id]"):
        did = art["data-defect-id"]
        disposition = art.get("data-disposition", "open")
        classes = art.get("class", [])
        category = next((c for c in classes if c in CATEGORY), None)

        component = component_of(art, did)
        magnitude = art.select_one(".magnitude")
        magnitude = clean(md(magnitude)) if magnitude else ""

        sections = []
        cells = art.select(".cell")
        if cells:
            for cell in cells:
                label = cell.select_one(".rowlabel")
                heading = label.get_text(strip=True) if label else "Notes"
                if label:
                    label.extract()
                sections.append(extract_section(cell, heading))
        else:
            for cls, heading in SECTION_CLASSES:
                for div in art.select("div." + cls):
                    sections.append(extract_section(div, heading))
        if not sections:
            sections.append(extract_section(art, "Notes"))

        out.append({
            "id": did,
            "component": component,
            "category": category,
            "disposition": disposition,
            "magnitude": magnitude,
            "sections": sections,
            "schemas": schemas_in(component),
            "summary": summaries.get(did, ""),
        })
    return out


def schemas_in(text):
    """Every govdata schema named in a string. A defect can span several — D-213 covers six
    lands tables across two dispatch paths — and a core defect names none."""
    found = []
    for token in re.findall(r"[a-z][a-z_0-9]*", text or ""):
        if token in SCHEMAS and token not in found:
            found.append(token)
    return found


def index_summaries(soup):
    """The register's index table carries a real one-line summary per active defect. Much
    better title material than the first sentence of the effect paragraph."""
    out = {}
    for row in soup.select("#idx-tbody tr"):
        link = row.select_one("a.idx-num-link")
        cell = row.select_one("td.idx-summary")
        if link and cell:
            out[link.get_text(strip=True)] = cell.get_text(" ", strip=True)
    return out


def parse_sourcing(path):
    """The register's 'Dataset Availability Research' table — what was looked for and not found.
    These are the sourcing-runner's backlog, not defects: no table exists to be wrong."""
    with open(path, encoding="utf-8") as fh:
        soup = BeautifulSoup(fh.read(), "html.parser")

    out = []
    table = soup.select_one("table.avail-table")
    if not table:
        return out
    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        note = clean(md(cells[3]))
        badge = row.select_one("span[class$='-badge']")
        out.append({
            "sought": cells[0].get_text(" ", strip=True),
            "needed_for": cells[1].get_text(" ", strip=True),
            "confirmed_via": cells[2].get_text(" ", strip=True),
            "note": note,
            "badge": badge.get("class")[0].replace("-badge", "") if badge else "",
        })
    return out


def sourcing_issue(row, index):
    """Open by default: an availability finding stays open until the dataset is onboarded or
    established as genuinely unavailable. A pending badge means a table already exists and only
    needs a backfill — that is ready work, not research."""
    badge = row["badge"]
    if badge == "resolved":
        state, status, resolution = "completed", None, "resolution:fixed"
    elif badge == "notavailable":
        state, status, resolution = "not planned", None, "resolution:not-available"
    elif badge == "pending":
        state, status, resolution = "open", "status:ready-to-run", None
    else:
        state, status, resolution = "open", "status:open", None

    labels = ["type:sourcing", "kind:gap"]
    if status:
        labels.append(status)
    if resolution:
        labels.append(resolution)
    for schema in schemas_in(row["note"] + " " + row["sought"]):
        labels.append("schema:" + schema)

    return {
        "kind": "sourcing",
        "key": "S-%02d" % index,
        "title": shorten("sourcing: %s" % row["sought"], 150),
        "body": "\n".join([
            "**Needed for** — %s" % (row["needed_for"] or "—"),
            "",
            "**Confirmed via** — %s" % (row["confirmed_via"] or "—"),
            "",
            "## Finding",
            "",
            row["note"],
            "",
            "---",
            "",
            "Migrated from the [Govdata Defect Register](%s) artifact, "
            "Dataset Availability Research table." % REGISTER_URL,
        ]),
        "labels": labels,
        "state": state,
        "flagged_needs_dq": False,
    }


def parse_ledger(path):
    with open(path, encoding="utf-8") as fh:
        soup = BeautifulSoup(fh.read(), "html.parser")

    units, infra = [], []
    body = soup.select_one("#ledger-body")
    if body:
        group = None
        for row in body.find_all("tr", recursive=False):
            classes = row.get("class", [])
            if "grouphead" in classes:
                group = row.get_text(" ", strip=True)
                continue
            if "data-row" not in classes:
                continue
            cells = {c: row.select_one("td." + c) for c in
                     ("slot", "year", "tables", "issue", "evidence")}
            slot = cells["slot"].get_text(" ", strip=True) if cells["slot"] else ""
            parent = None
            m = re.match(r"(D-\d+)", group or "")
            if m:
                parent = m.group(1)
            schema = SLOT_TO_SCHEMA.get(slot, slot)
            units.append({
                "parent": parent,
                "group": group or "",
                "slot": slot,
                "schema": schema if schema in SCHEMAS else None,
                "year": cells["year"].get_text(" ", strip=True) if cells["year"] else "",
                "tables": cells["tables"].get_text(" ", strip=True) if cells["tables"] else "",
                "issue": clean(md(cells["issue"])) if cells["issue"] else "",
                "evidence": clean(md(cells["evidence"])) if cells["evidence"] else "",
                "status": row.get("data-status", "run-no"),
            })

    section = soup.select_one("section.infra")
    if section:
        for row in section.select("tbody tr"):
            fix = row.select_one("td.slot")
            affects = row.select_one("td.tables")
            issue = row.select_one("td.issue")
            status = row.select_one("td.num")
            if not fix:
                continue
            infra.append({
                "fix": fix.get_text(" ", strip=True),
                "affects": affects.get_text(" ", strip=True) if affects else "",
                "issue": clean(md(issue)) if issue else "",
                "status": clean(md(status)) if status else "",
            })
    return units, infra


# --------------------------------------------------------------------------- issue building

def defect_issue(entry):
    component = shorten(entry["component"], 60)
    title = shorten("%s %s — %s" % (entry["id"], component, summary_of(entry)), 150)
    labels = ["type:defect"]
    if entry["category"]:
        labels.append(CATEGORY[entry["category"]])
    for schema in entry["schemas"]:
        labels.append("schema:" + schema)

    state, status, resolution = DISPOSITION.get(entry["disposition"], ("open", "status:open", None))
    full_text = " ".join(s["body"] for s in entry["sections"])
    if status == "status:ready-to-run" and NEEDS_DQ.search(full_text):
        status = "status:needs-dq"
        flagged = True
    else:
        flagged = False
    if status:
        labels.append(status)
    if resolution:
        labels.append(resolution)

    lines = []
    if entry["magnitude"]:
        lines.append("**Magnitude** — " + entry["magnitude"])
        lines.append("")
    for sec in entry["sections"]:
        lines.append("## " + sec["heading"])
        lines.append("")
        if sec["evidence"]:
            lines.append("Evidence: " + " · ".join("`%s`" % e for e in sec["evidence"]))
            lines.append("")
        lines.append(sec["body"])
        lines.append("")
        if sec["numbers"]:
            lines.append("| | |")
            lines.append("|---|---|")
            for k, v in sec["numbers"]:
                lines.append("| %s | %s |" % (k, v))
            lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("Migrated from the [Govdata Defect Register](%s) artifact, entry `%s`."
                 % (REGISTER_URL, entry["id"]))

    return {
        "kind": "defect",
        "key": entry["id"],
        "title": title,
        "body": "\n".join(lines),
        "labels": labels,
        "state": state,
        "flagged_needs_dq": flagged,
    }


def shorten(text, limit):
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[:limit - 1].rsplit(" ", 1)[0] + "…"


def summary_of(entry):
    """Best available one-liner: the index table's summary, then the magnitude line's first
    clause, then the opening sentence of the effect. The effect's first sentence is the worst
    of the three — several entries open with boilerplate about which sweep found them."""
    if entry["summary"]:
        return shorten(entry["summary"], 90)
    if entry["magnitude"]:
        text = re.sub(r"[*`]", "", entry["magnitude"])
        text = re.split(r"(?<=[a-z0-9)])\.\s|\bFound \d", text)[0]
        return shorten(text, 90)
    for sec in entry["sections"]:
        if sec["body"]:
            text = re.sub(r"[*`]", "", sec["body"].split("\n")[0])
            text = re.sub(r"^(Effect as reported|Notes)\.?\s*", "", text.strip())
            return shorten(text, 90)
    return entry["component"]


def remediation_issue(unit, index):
    tables = unit["tables"]
    short = tables if len(tables) <= 60 else tables[:57].rsplit(",", 1)[0] + ", …"
    title = shorten("%s:%s %s" % (unit["slot"], unit["year"].split(" ")[0] or "n/a", short), 150)

    labels = ["type:remediation"]
    if unit["schema"]:
        labels.append("schema:" + unit["schema"])
    state, status, resolution = LEDGER_STATUS.get(unit["status"], ("open", "status:ready-to-run", None))
    flagged = False
    if status == "status:ready-to-run" and NEEDS_DQ.search(unit["evidence"] + unit["issue"]):
        status, flagged = "status:needs-dq", True
    if status:
        labels.append(status)
    if resolution:
        labels.append(resolution)

    body = [
        "**Slot** `%s` · **Year** %s" % (unit["slot"], unit["year"] or "n/a"),
        "",
        "**Tables** — %s" % tables,
        "",
        "## Issue addressed",
        "",
        unit["issue"],
        "",
        "## Evidence, run command and verification",
        "",
        unit["evidence"],
        "",
        "---",
        "",
        "Migrated from the [Govdata Remediation Ledger](%s) artifact." % LEDGER_URL,
    ]
    if unit["parent"]:
        body.insert(0, "Remediation job for **%s**.\n" % unit["parent"])

    return {
        "kind": "remediation",
        "key": "L-%02d" % index,
        "parent": unit["parent"],
        "title": title,
        "body": "\n".join(body),
        "labels": labels,
        "state": state,
        "flagged_needs_dq": flagged,
    }


# A fix can be committed and verified and still have work left — several infra rows read
# "Fixed, verified" followed by "Not yet promoted to shared jar". That is an open task.
OUTSTANDING = re.compile(r"not yet promoted|not yet staged|waits for|blocked only on", re.I)


def infra_issue(row, index):
    done = "Fixed" in row["status"] and not OUTSTANDING.search(row["status"])
    return {
        "kind": "infra",
        "key": "I-%02d" % index,
        "title": shorten("infra: %s" % row["fix"], 150),
        "body": "\n".join([
            "**Affects** — %s" % row["affects"],
            "",
            "## Issue",
            "",
            row["issue"],
            "",
            "## Status at migration",
            "",
            row["status"],
            "",
            "---",
            "",
            "Migrated from the [Govdata Remediation Ledger](%s) artifact, "
            "infrastructure table." % LEDGER_URL,
        ]),
        "labels": (["type:infra", "kind:core"] +
                   ([] if done else ["status:partial" if "Fixed" in row["status"]
                                     else "status:investigating"])),
        "state": "completed" if done else "open",
        "flagged_needs_dq": False,
    }


# --------------------------------------------------------------------------- output

def write_plan(issues, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    rows = ["key\tkind\tstate\tlabels\ttitle"]
    for iss in issues:
        path = os.path.join(out_dir, "%s.md" % iss["key"])
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("# %s\n\n" % iss["title"])
            fh.write("labels: %s\n" % ", ".join(iss["labels"]))
            fh.write("state: %s\n" % iss["state"])
            if iss.get("parent"):
                fh.write("parent: %s\n" % iss["parent"])
            fh.write("\n---\n\n")
            fh.write(iss["body"])
            fh.write("\n")
        rows.append("\t".join([iss["key"], iss["kind"], iss["state"],
                               ",".join(iss["labels"]), iss["title"]]))
    with open(os.path.join(out_dir, "plan.tsv"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(rows) + "\n")


def gh(args, repo):
    # `gh api` takes the repo in the endpoint path and rejects --repo; every other
    # subcommand needs it.
    if args[0] != "api":
        args = args + ["--repo", repo]
    result = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError("gh %s failed: %s" % (" ".join(args), result.stderr.strip()))
    return result.stdout.strip()


def existing_issues(repo):
    """Title -> (number, state) for everything already in the repo, so a re-run resumes
    instead of duplicating. Titles are distinctive enough to key on."""
    raw = gh(["issue", "list", "--state", "all", "--limit", "500",
              "--json", "number,title,state"], repo)
    out = {}
    for row in json.loads(raw or "[]"):
        out[row["title"]] = (row["number"], row["state"].lower())
    return out


def create_issues(issues, repo):
    already = existing_issues(repo)
    numbers = {}
    closed_state = {}
    for iss in issues:
        if iss["title"] in already:
            number, state = already[iss["title"]]
            numbers[iss["key"]] = number
            closed_state[iss["key"]] = state
            print("  #%-5d %s  (exists, skipped)" % (number, iss["key"]))
            continue
        args = ["issue", "create", "--title", iss["title"], "--body", iss["body"]]
        for label in iss["labels"]:
            args += ["--label", label]
        url = gh(args, repo)
        number = int(url.rstrip("/").rsplit("/", 1)[-1])
        numbers[iss["key"]] = number
        closed_state[iss["key"]] = "open"
        print("  #%-5d %s  %s" % (number, iss["key"], iss["title"][:70]))

    # Link remediation jobs to the defect they serve, before closing anything.
    for iss in issues:
        parent_key = iss.get("parent")
        if not parent_key or parent_key not in numbers:
            continue
        child = numbers[iss["key"]]
        parent = numbers[parent_key]
        try:
            linked = json.loads(gh(["api", "repos/%s/issues/%d/sub_issues" % (repo, parent)],
                                   repo) or "[]")
        except RuntimeError:
            linked = []
        if any(sub.get("number") == child for sub in linked):
            print("  linked #%d -> #%d (already)" % (child, parent))
            continue
        child_id = gh(["api", "repos/%s/issues/%d" % (repo, child), "--jq", ".id"], repo)
        try:
            gh(["api", "repos/%s/issues/%d/sub_issues" % (repo, parent),
                "-F", "sub_issue_id=%s" % child_id], repo)
            print("  linked #%d -> #%d (%s)" % (child, parent, parent_key))
        except RuntimeError as exc:
            print("  WARN could not link #%d to #%d: %s" % (child, parent, exc))

    for iss in issues:
        if iss["state"] == "open":
            continue
        if closed_state.get(iss["key"]) == "closed":
            continue
        gh(["issue", "close", str(numbers[iss["key"]]), "--reason", iss["state"]], repo)
        print("  closed #%d as %s" % (numbers[iss["key"]], iss["state"]))
    return numbers


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--register", required=True, help="saved Defect Register HTML")
    ap.add_argument("--ledger", required=True, help="saved Remediation Ledger HTML")
    ap.add_argument("--repo", default=os.environ.get("GH_OPS_REPO", "kenstott/govdata-ops"))
    ap.add_argument("--out", default="/tmp/govdata-issue-migration",
                    help="directory for the reviewable plan (default mode)")
    ap.add_argument("--create", action="store_true",
                    help="actually file the issues (default is to write the plan only)")
    args = ap.parse_args()

    entries = parse_register(args.register)
    sourcing = parse_sourcing(args.register)
    units, infra = parse_ledger(args.ledger)

    issues = [defect_issue(e) for e in entries]
    issues += [remediation_issue(u, i + 1) for i, u in enumerate(units)]
    issues += [infra_issue(r, i + 1) for i, r in enumerate(infra)]
    issues += [sourcing_issue(r, i + 1) for i, r in enumerate(sourcing)]

    open_count = sum(1 for i in issues if i["state"] == "open")
    print("Parsed %d defects, %d remediation units, %d infra rows, %d sourcing rows"
          % (len(entries), len(units), len(infra), len(sourcing)))
    print("  %d open, %d closed" % (open_count, len(issues) - open_count))

    flagged = [i for i in issues if i["flagged_needs_dq"]]
    if flagged:
        print("\n  Downgraded to status:needs-dq (text says the job is not safe to run yet)"
              " — review each:")
        for i in flagged:
            print("    %s  %s" % (i["key"], i["title"][:80]))

    # A defect closed while its own job is still outstanding is exactly the Register/Ledger
    # disagreement the two-document setup made easy to miss. Surface it rather than migrate it
    # silently — as a parent/child pair it is visible from either end from now on.
    states = {i["key"]: i["state"] for i in issues}
    contradictory = [i for i in issues
                     if i.get("parent") and i["state"] == "open"
                     and states.get(i["parent"], "open") != "open"]
    if contradictory:
        print("\n  Job still open under a defect recorded as closed — reconcile before or"
              " after migrating:")
        for i in contradictory:
            print("    %s (parent %s, closed)  %s" % (i["key"], i["parent"], i["title"][:70]))

    unlinked = [i for i in issues if i["kind"] == "remediation" and not i.get("parent")]
    if unlinked:
        print("\n  Remediation units with no parent defect — will be created standalone:")
        for i in unlinked:
            print("    %s  %s" % (i["key"], i["title"][:80]))

    if not args.create:
        write_plan(issues, args.out)
        print("\nPlan written to %s — review it, then re-run with --create "
              "(or --apply, via setup-issue-tracking.sh)." % args.out)
        return 0

    print("\nCreating in %s" % args.repo)
    numbers = create_issues(issues, args.repo)
    with open(os.path.join(args.out or ".", "created.json"), "w", encoding="utf-8") as fh:
        json.dump(numbers, fh, indent=2)
    print("\nDone. %d issues created." % len(numbers))
    return 0


if __name__ == "__main__":
    sys.exit(main())
