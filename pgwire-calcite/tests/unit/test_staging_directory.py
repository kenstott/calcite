# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1992: the file adapter's staging directory has no default, so the server names one under
its own state directory when it starts the JVM. No JVM is started here."""

from __future__ import annotations

import os

from pgwire_calcite.calcite_backend import CalciteBackend


def _backend(model_path):
    backend = object.__new__(CalciteBackend)
    backend._model_path = model_path
    return backend


def _staging(backend, args):
    return backend._state_dir_arg(args, CalciteBackend._STAGING_DIRECTORY, "staging")


def test_staging_goes_under_the_state_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("PGWIRE_CALCITE_STATE_DIR", str(tmp_path / "state"))
    arg = _staging(_backend(str(tmp_path / "model.json")), [])
    assert arg == "-Dcalcite.file.staging.directory=" + os.path.join(
        str(tmp_path / "state"), "staging"
    )


def test_staging_without_a_state_directory_goes_beside_the_model(monkeypatch, tmp_path):
    monkeypatch.delenv("PGWIRE_CALCITE_STATE_DIR", raising=False)
    arg = _staging(_backend(str(tmp_path / "model.json")), [])
    assert arg == "-Dcalcite.file.staging.directory=" + os.path.join(str(tmp_path), "staging")


def test_an_operator_staging_setting_is_kept(monkeypatch, tmp_path):
    monkeypatch.setenv("PGWIRE_CALCITE_STATE_DIR", str(tmp_path))
    mine = ["-Dcalcite.file.staging.directory=/mine"]
    assert _staging(_backend(str(tmp_path / "model.json")), mine) is None
