#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
