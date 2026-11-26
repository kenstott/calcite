# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables a model declares that its warehouse does not hold yet.

They are listed in a directory beside the model, one file per table named ``<schema>.<table>``,
so changes that add different tables never touch the same file. The files' contents are unused.
"""

from __future__ import annotations

import os
from typing import List, Set, Tuple

#: The directory's name; it sits beside model.json and the shipped catalog cache.
DIR_NAME = "unmaterialized"

_README = "README.md"


def entries(directory: str) -> Set[Tuple[str, str]]:
    """The ``(schema, table)`` pairs ``directory`` lists. It must exist."""
    found = set()
    for name in os.listdir(directory):
        if name == _README:
            continue
        schema, dot, table = name.partition(".")
        if not dot or not schema or not table or "." in table:
            raise ValueError(f"{os.path.join(directory, name)}: expected a file named schema.table")
        found.add((schema, table))
    return found


def prune(directory: str, materialized: Set[Tuple[str, str]]) -> List[str]:
    """Delete the entries of the ``materialized`` tables; return their ``schema.table`` names."""
    removed = sorted(f"{schema}.{table}" for schema, table in entries(directory) & materialized)
    for name in removed:
        os.remove(os.path.join(directory, name))
    return removed
