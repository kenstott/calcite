"""Print failed tests (class, method, message) found in Gradle JUnit XML reports under a root."""
import os
import sys
import xml.etree.ElementTree as ET


def main(root):
    failed = []
    for dirpath, _dirs, files in os.walk(root):
        if os.sep + "build" + os.sep + "test-results" + os.sep not in dirpath + os.sep:
            continue
        for name in files:
            if not name.endswith(".xml"):
                continue
            path = os.path.join(dirpath, name)
            for case in ET.parse(path).getroot().iter("testcase"):
                for kind in ("failure", "error"):
                    node = case.find(kind)
                    if node is not None:
                        msg = (node.get("message") or "").strip().splitlines()
                        failed.append((path, case.get("classname"), case.get("name"),
                                       msg[0][:500] if msg else ""))
    print("### Failed tests (%d)" % len(failed))
    for path, cls, name, msg in failed:
        print("- `%s.%s` — %s  \n  (%s)" % (cls, name, msg, path))


if __name__ == "__main__":
    main(sys.argv[1])
