# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog cache is keyed by model.json AND the engine release that walked it.

The cache's column types come from the engine as much as from model.json: engine 0.119.4
served govdata's year partitions as INTEGER while the cache it shipped, walked by an earlier
engine for the same model.json, still said VARCHAR, and DuckDB read the INTEGER bytes as text
("Invalid unicode"). A cache another engine wrote must never be loaded.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys

from pgwire_calcite import catalog_cache_key, catalog_populate
from test_catalog_cache_partial import _TABLES, _Connection, _DatabaseMetaData, _model


def _stages(caplog):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("STAGE")]


def test_a_cache_another_engine_release_wrote_is_not_loaded(tmp_path, monkeypatch, caplog):
    model_path = _model(tmp_path)
    monkeypatch.setattr(catalog_populate, "engine_version", lambda: "0.119.3")
    catalog_populate.build_and_cache_context(_Connection(_DatabaseMetaData(_TABLES)), model_path)
    old = catalog_populate.catalog_cache_path(model_path)
    monkeypatch.setattr(catalog_populate, "engine_version", lambda: "0.119.4")
    monkeypatch.setattr(catalog_populate, "install_catalog", lambda *a, **k: None)
    caplog.set_level(logging.INFO, logger=catalog_populate.__name__)

    catalog_populate.populate_state_cached(
        _Connection(_DatabaseMetaData(_TABLES)), object(), model_path)

    assert catalog_populate.catalog_cache_path(model_path) != old
    stages = _stages(caplog)
    assert stages[0].startswith("STAGE catalog-cache-miss "), stages
    assert stages[1] == "STAGE catalog-walked tables=3"


def test_the_same_model_and_engine_release_load_the_cache(tmp_path, monkeypatch, caplog):
    model_path = _model(tmp_path)
    monkeypatch.setattr(catalog_populate, "engine_version", lambda: "0.119.4")
    catalog_populate.build_and_cache_context(_Connection(_DatabaseMetaData(_TABLES)), model_path)
    monkeypatch.setattr(catalog_populate, "install_catalog", lambda *a, **k: None)
    caplog.set_level(logging.INFO, logger=catalog_populate.__name__)

    catalog_populate.populate_state_cached(None, object(), model_path)

    assert _stages(caplog) == [
        f"STAGE catalog-cache-hit path={catalog_populate.catalog_cache_path(model_path)} tables=3"]


def test_the_key_covers_both_the_model_and_the_engine_release(tmp_path):
    model_path = _model(tmp_path)
    key = catalog_cache_key.cache_key(model_path, "0.121.3")
    assert key == catalog_cache_key.cache_key(model_path, "0.121.3")
    assert key != catalog_cache_key.cache_key(model_path, "0.121.4")
    with open(model_path, "a", encoding="utf-8") as f:
        f.write(" ")
    assert key != catalog_cache_key.cache_key(model_path, "0.121.3")


def test_a_bundle_is_keyed_by_its_stamped_release_and_a_checkout_by_version_txt(
    tmp_path, monkeypatch
):
    stamp = tmp_path / "ENGINE_VERSION"
    monkeypatch.setattr(catalog_cache_key, "ENGINE_VERSION_STAMP", str(stamp))
    with open(catalog_cache_key._CHECKOUT_VERSION, encoding="utf-8") as f:
        assert catalog_cache_key.engine_version() == f.read().strip()
    stamp.write_text("9.9.9\n", encoding="utf-8")
    assert catalog_cache_key.engine_version() == "9.9.9"


def test_the_key_needs_only_the_standard_library(tmp_path):
    """The pack step computes the key with a runner's bare python3, which has none of the
    server's dependencies; -S keeps site-packages out."""
    model_path = _model(tmp_path)
    src = os.path.dirname(os.path.dirname(catalog_cache_key.__file__))
    r = subprocess.run(
        [sys.executable, "-S", "-c",
         ("import sys; from pgwire_calcite.catalog_cache_key import cache_key; "
          "print(cache_key(sys.argv[1], sys.argv[2]))"), model_path, "0.121.3"],
        check=False, capture_output=True, text=True, env=dict(os.environ, PYTHONPATH=src))
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == catalog_cache_key.cache_key(model_path, "0.121.3")
