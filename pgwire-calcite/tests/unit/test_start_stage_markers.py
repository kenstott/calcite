# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server logs ``STAGE <name>`` lines the AskAmerica engine tail-parses to say what a start
is doing. A catalog cache miss must be marked, with the model hash and every path looked for,
because a production install is meant to ship its cache and never rebuild the catalog."""

from __future__ import annotations

import logging
import re

from pgwire_calcite import catalog_populate
from test_catalog_cache_partial import _Connection, _DatabaseMetaData, _TABLES, _model


def _stages(caplog):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("STAGE")]


def test_a_missing_cache_is_marked_as_a_miss_naming_hash_and_paths(tmp_path, monkeypatch, caplog):
    model_path = _model(tmp_path)
    monkeypatch.setattr(catalog_populate, "install_catalog", lambda *a, **k: None)
    caplog.set_level(logging.INFO, logger=catalog_populate.__name__)
    catalog_populate.populate_state_cached(
        _Connection(_DatabaseMetaData(_TABLES)), object(), model_path)
    stages = _stages(caplog)
    assert len(stages) == 2, stages
    miss = stages[0]
    assert miss.startswith("STAGE catalog-cache-miss ")
    file_name = catalog_populate._cache_file_name(model_path)
    assert f"hash={file_name[len('catalog-cache-'):-len('.pkl')]}" in miss
    assert re.search(r"looked=\S*" + re.escape(file_name), miss)
    assert "unloadable=none" in miss
    assert stages[1] == "STAGE catalog-walked tables=3"


def test_a_cache_on_disk_is_marked_as_a_hit_and_nothing_is_walked(tmp_path, monkeypatch, caplog):
    model_path = _model(tmp_path)
    catalog_populate.build_and_cache_context(
        _Connection(_DatabaseMetaData(_TABLES)), model_path)
    monkeypatch.setattr(catalog_populate, "install_catalog", lambda *a, **k: None)
    caplog.set_level(logging.INFO, logger=catalog_populate.__name__)
    catalog_populate.populate_state_cached(None, object(), model_path)
    stages = _stages(caplog)
    assert stages == [
        f"STAGE catalog-cache-hit path={catalog_populate.catalog_cache_path(model_path)} tables=3"]


def test_an_unloadable_cache_is_a_miss_that_names_the_file(tmp_path, monkeypatch, caplog):
    model_path = _model(tmp_path)
    bad = catalog_populate.catalog_cache_path(model_path)
    with open(bad, "wb") as f:
        f.write(b"not a pickle")
    monkeypatch.setattr(catalog_populate, "install_catalog", lambda *a, **k: None)
    caplog.set_level(logging.INFO, logger=catalog_populate.__name__)
    catalog_populate.populate_state_cached(
        _Connection(_DatabaseMetaData(_TABLES)), object(), model_path)
    assert f"unloadable={bad}" in _stages(caplog)[0]
