#!/usr/bin/env python3
"""Inject the "this page is retired" banner into a saved artifact HTML file.

The Defect Register and Remediation Ledger artifacts are archives now — tracking moved to
GitHub issues (see docs/ops/defect-tracking.md). This puts a banner at the top of each so a
reader who lands on one knows it is no longer written to.

    # inject, writing a new file next to the original
    ./inject-retirement-banner.py <artifact.html>

    # in place, or to a path you choose
    ./inject-retirement-banner.py <artifact.html> --in-place
    ./inject-retirement-banner.py <artifact.html> --out /tmp/bannered.html

    # is a file already bannered?
    ./inject-retirement-banner.py <artifact.html> --check

Which banner to use is chosen from the file's own <title>; the two differ in wording and in
which CSS custom properties they borrow, because the two artifacts define different ones.
Banner text lives in artifacts/banner-*.html — edit those, not this script.

    # inject and copy to the clipboard, ready to paste
    ./inject-retirement-banner.py <artifact.html> --clip

To get the banner onto the live artifact, either paste the output into the artifact editor,
or ask Claude to publish it (which costs a full read of the file first).
"""

import argparse
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BANNERS = os.path.join(HERE, "artifacts")

# <title> substring -> banner file. Matched case-insensitively.
KNOWN = [
    ("Govdata Defect Register", "banner-defect-register.html"),
    ("Govdata Remediation Ledger", "banner-remediation-ledger.html"),
]

MARKER = 'id="retired-banner"'

# Both artifacts open their body with this. The banner goes immediately after it, so it is
# the first thing on the page — above each artifact's own pre-existing callout.
ANCHOR = '<div class="wrap">'


def title_of(html):
    match = re.search(r"<title>(.*?)</title>", html, re.S | re.I)
    return match.group(1).strip() if match else ""


def pick_banner(html, override):
    if override:
        return override
    title = title_of(html)
    for needle, filename in KNOWN:
        if needle.lower() in title.lower():
            return os.path.join(BANNERS, filename)
    raise SystemExit(
        "cannot tell which banner this file wants — its <title> is %r, which matches none of:\n"
        "  %s\nPass --banner explicitly."
        % (title, "\n  ".join(n for n, _ in KNOWN))
    )


def inject(html, banner):
    count = html.count(ANCHOR)
    if count == 0:
        raise SystemExit("no %s in this file — is it really an artifact page?" % ANCHOR)
    if count > 1:
        raise SystemExit(
            "%d occurrences of %s — refusing to guess which one opens the body."
            % (count, ANCHOR)
        )
    # Anchor on the first newline after the opening div so the banner lands inside it,
    # whatever the artifact puts next.
    at = html.index(ANCHOR) + len(ANCHOR)
    body = banner if banner.endswith("\n") else banner + "\n"
    return html[:at] + "\n\n" + body + html[at:].lstrip("\n")


def copy_to_clipboard(text):
    """Put the page on the system clipboard, ready to paste into the artifact editor.

    Under WSL that means clip.exe, which reads UTF-16 — piping UTF-8 straight in mangles
    every em-dash and check mark in the file. Python's "utf-16" codec emits the BOM
    clip.exe keys off, so encode explicitly rather than letting the pipe guess."""
    candidates = [
        (["clip.exe"], "utf-16"),
        (["wl-copy"], "utf-8"),
        (["xclip", "-selection", "clipboard"], "utf-8"),
        (["pbcopy"], "utf-8"),
    ]
    for argv, encoding in candidates:
        if not shutil.which(argv[0]):
            continue
        try:
            subprocess.run(argv, input=text.encode(encoding), check=True)
        except (subprocess.CalledProcessError, OSError) as exc:
            print("clip   : %s failed (%s)" % (argv[0], exc))
            continue
        return argv[0]
    return None


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("file", help="saved artifact HTML")
    ap.add_argument("--banner", help="banner HTML to inject (default: chosen from <title>)")
    ap.add_argument("--out", help="write here (default: <file>-bannered.html)")
    ap.add_argument("--in-place", action="store_true", help="overwrite the input file")
    ap.add_argument("--check", action="store_true", help="report whether it is already bannered")
    ap.add_argument("--force", action="store_true", help="inject even if already bannered")
    ap.add_argument("--clip", action="store_true",
                    help="also copy the result to the clipboard, ready to paste")
    args = ap.parse_args()

    with open(args.file, encoding="utf-8") as fh:
        html = fh.read()

    already = MARKER in html
    if args.check:
        print("%s: %s" % (args.file, "bannered" if already else "NOT bannered"))
        return 0 if already else 1

    print("title  : %s" % (title_of(html) or "(none)"))
    if already and not args.force:
        print("Already bannered — nothing to do. Pass --force to inject a second one.")
        return 0

    banner_path = pick_banner(html, args.banner)
    with open(banner_path, encoding="utf-8") as fh:
        banner = fh.read()
    print("banner : %s (%d bytes)" % (os.path.relpath(banner_path, HERE), len(banner)))

    out_html = inject(html, banner)

    if args.in_place:
        out_path = args.file
    elif args.out:
        out_path = args.out
    else:
        root, ext = os.path.splitext(args.file)
        out_path = root + "-bannered" + (ext or ".html")

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(out_html)

    print("wrote  : %s (%d -> %d bytes)" % (out_path, len(html), len(out_html)))
    if MARKER not in out_html:
        raise SystemExit("post-check failed: marker missing from the output")
    print("checked: banner present, %s intact" % ANCHOR)
    if args.clip:
        tool = copy_to_clipboard(out_html)
        if tool:
            print("clip   : copied via %s (%d chars)" % (tool, len(out_html)))
        else:
            print("clip   : no clipboard tool found (looked for clip.exe, wl-copy, xclip, pbcopy)")

    print()
    if args.clip:
        print("Now open the artifact's editor, select all (Ctrl+A) and paste (Ctrl+V).")
    else:
        print("To publish it: paste this file's contents into the artifact editor (--clip puts")
        print("it on the clipboard for you), or ask Claude to publish it (Claude must read the")
        print("whole file first, which is not cheap).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
