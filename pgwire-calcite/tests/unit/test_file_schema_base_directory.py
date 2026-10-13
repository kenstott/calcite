# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1992: a file schema whose model names no base directory (a bundle's own model, in an
install tree that may be read-only) writes under the server's state directory. No JVM here."""

from __future__ import annotations

import json
import os

from pgwire_calcite.calcite_backend import CalciteBackend

FACTORY = "org.apache.calcite.adapter.file.FileSchemaFactory"


def _backend(model_path):
    backend = object.__new__(CalciteBackend)
    backend._model_path = model_path
    return backend


def _model(tmp_path, operand):
    install = tmp_path / "install"
    install.mkdir()
    path = install / "model.json"
    path.write_text(
        json.dumps(
            {
                "version": "1.0",
                "schemas": [
                    {"name": "FILES", "type": "custom", "factory": FACTORY, "operand": operand},
                    {"name": "OTHER", "type": "map", "tables": []},
                ],
            }
        )
    )
    return str(path)


def test_an_unset_base_goes_under_the_state_directory(monkeypatch, tmp_path):
    state = tmp_path / "state"
    monkeypatch.setenv("PGWIRE_CALCITE_STATE_DIR", str(state))
    model_path = _model(tmp_path, {"directory": "data", "recursive": True})
    used = _backend(model_path)._connection_model_path()
    assert os.path.dirname(used) == str(state)
    with open(used, encoding="utf-8") as f:
        operand = json.load(f)["schemas"][0]["operand"]
    assert operand["baseDirectory"] == os.path.join(str(state), "file-base", "FILES")
    # the copy sits in the state directory, so the relative source directory is made absolute
    assert operand["directory"] == os.path.join(str(tmp_path / "install"), "data")
    assert operand["recursive"] is True


def test_a_model_that_names_its_base_is_used_as_it_is(monkeypatch, tmp_path):
    monkeypatch.setenv("PGWIRE_CALCITE_STATE_DIR", str(tmp_path / "state"))
    model_path = _model(tmp_path, {"directory": "data", "baseDirectory": "/srv/files"})
    assert _backend(model_path)._connection_model_path() == model_path


def test_without_a_state_directory_the_model_is_used_as_it_is(monkeypatch, tmp_path):
    monkeypatch.delenv("PGWIRE_CALCITE_STATE_DIR", raising=False)
    model_path = _model(tmp_path, {"directory": "data"})
    assert _backend(model_path)._connection_model_path() == model_path
