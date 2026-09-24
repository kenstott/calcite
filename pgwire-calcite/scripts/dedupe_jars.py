#!/usr/bin/env python3
"""Keep only the highest version of each artifact in a flat jars/ directory.

The release workflow collects jars from two independently-resolved Gradle
classpaths (the adapter's own runtimeClasspath, plus a shared Arrow classpath)
via `cp -n`, which dedupes only by exact filename. Two classpaths can legally
resolve the same artifact (e.g. jackson-core) to two different versions, and
both then land in the same flat directory, coexisting on one classpath with
no guarantee the JVM picks compatible versions of related artifacts (observed
live: jackson-dataformat-yaml-2.18.4 calling a jackson-core method only
present in jackson-core-2.18.4.1, while jackson-core-2.17.1 also sat on the
classpath and could be the one actually linked). This keeps exactly one
version per artifact base name, always the highest.
"""
import os
import re
import sys

VERSION_RE = re.compile(r'^(.+)-([0-9][0-9A-Za-z_.]*)\.jar$')


def version_key(ver):
    # Every element is a (kind, value) pair so tuple comparison never has to compare an
    # int to a str directly (Python 3 raises TypeError for that) -- it short-circuits on
    # the kind tag first. Numeric segments sort before non-numeric ones at the same
    # position, which is good enough for "highest version wins" without needing full
    # semver parsing for every dependency's ad-hoc version-string format.
    return [(0, int(p)) if p.isdigit() else (1, p) for p in re.split(r'[._-]', ver)]


def dedupe(directory):
    groups = {}
    for name in sorted(os.listdir(directory)):
        if not name.endswith('.jar'):
            continue
        m = VERSION_RE.match(name)
        if not m:
            continue
        base, ver = m.group(1), m.group(2)
        groups.setdefault(base, []).append((version_key(ver), name))
    removed = 0
    for base, entries in groups.items():
        if len(entries) < 2:
            continue
        entries.sort(key=lambda e: e[0])
        kept = entries[-1][1]
        for _, name in entries[:-1]:
            path = os.path.join(directory, name)
            print(f"dedupe: removing {name} (superseded by {kept})")
            os.remove(path)
            removed += 1
    print(f"dedupe: removed {removed} superseded jar(s)")


if __name__ == '__main__':
    dedupe(sys.argv[1])
