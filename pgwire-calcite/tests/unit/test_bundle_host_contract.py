# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a bundle asks of its host: nothing written into the install tree at run time, and
the Java runtime the bundle carries is the one the Calcite JVM starts from."""

import os
import pickle

import pytest

from pgwire_calcite import calcite_backend, catalog_populate, classpath


# --- classpath read from the jars directory ---------------------------------------------


def test_classpath_dir_lists_every_jar_sorted(tmp_path, monkeypatch):
    jars = tmp_path / "jars"
    jars.mkdir()
    for name in ("b.jar", "a.jar", "notes.txt"):
        (jars / name).write_bytes(b"")
    monkeypatch.delenv("PGWIRE_CALCITE_CLASSPATH", raising=False)
    monkeypatch.setenv("PGWIRE_CALCITE_CLASSPATH_DIR", str(jars))
    monkeypatch.setattr(classpath, "_vendored_jars", lambda: [])

    assert classpath.resolve_classpath() == [str(jars / "a.jar"), str(jars / "b.jar")]
    assert sorted(os.listdir(jars)) == ["a.jar", "b.jar", "notes.txt"], "nothing was written"


def test_classpath_dir_that_is_missing_is_an_error_by_name(tmp_path, monkeypatch):
    monkeypatch.delenv("PGWIRE_CALCITE_CLASSPATH", raising=False)
    monkeypatch.setenv("PGWIRE_CALCITE_CLASSPATH_DIR", str(tmp_path / "absent"))
    with pytest.raises(classpath.ClasspathError, match="PGWIRE_CALCITE_CLASSPATH_DIR"):
        classpath.resolve_classpath()


def test_classpath_dir_without_jars_is_an_error(tmp_path, monkeypatch):
    monkeypatch.delenv("PGWIRE_CALCITE_CLASSPATH", raising=False)
    monkeypatch.setenv("PGWIRE_CALCITE_CLASSPATH_DIR", str(tmp_path))
    monkeypatch.setattr(classpath, "_vendored_jars", lambda: [])
    with pytest.raises(classpath.ClasspathError, match="empty"):
        classpath.resolve_classpath()


# --- catalog cache written to the state directory ----------------------------------------


class _Ctx:
    """Stands in for a catalog context: the loader reads only ``tables``."""

    def __init__(self, tables):
        self.tables = tables


def _model(tmp_path):
    install = tmp_path / "install" / "model"
    install.mkdir(parents=True)
    model = install / "model.json"
    model.write_text('{"version": "1.0", "schemas": []}')
    return str(model)


def test_cache_is_written_to_the_state_directory_not_beside_the_model(tmp_path, monkeypatch):
    model_path = _model(tmp_path)
    state = tmp_path / "state"
    monkeypatch.setenv(catalog_populate.STATE_DIR_ENV, str(state))

    written = catalog_populate.catalog_cache_write_path(model_path)

    assert os.path.dirname(written) == str(state)
    assert os.path.basename(written) == os.path.basename(
        catalog_populate.catalog_cache_path(model_path))
    assert os.listdir(os.path.dirname(model_path)) == ["model.json"]


def test_two_servers_with_their_own_state_directory_do_not_share_a_cache(tmp_path, monkeypatch):
    model_path = _model(tmp_path)
    monkeypatch.setenv(catalog_populate.STATE_DIR_ENV, str(tmp_path / "org-a"))
    first = catalog_populate.catalog_cache_write_path(model_path)
    monkeypatch.setenv(catalog_populate.STATE_DIR_ENV, str(tmp_path / "org-b"))
    second = catalog_populate.catalog_cache_write_path(model_path)
    assert first != second


def test_without_a_state_directory_the_cache_stays_beside_the_model(tmp_path, monkeypatch):
    model_path = _model(tmp_path)
    monkeypatch.delenv(catalog_populate.STATE_DIR_ENV, raising=False)
    assert catalog_populate.catalog_cache_write_path(model_path) == (
        catalog_populate.catalog_cache_path(model_path))


def test_a_cache_shipped_beside_the_model_is_read_when_the_state_directory_has_none(
        tmp_path, monkeypatch):
    model_path = _model(tmp_path)
    monkeypatch.setenv(catalog_populate.STATE_DIR_ENV, str(tmp_path / "state"))
    shipped = catalog_populate.catalog_cache_path(model_path)
    with open(shipped, "wb") as f:
        pickle.dump((catalog_populate._CACHE_FORMAT_VERSION, _Ctx(["t"]), {"c": 1}), f)
    installed = []
    monkeypatch.setattr(catalog_populate, "install_catalog",
                        lambda state, c, types: installed.append((c, types)))
    monkeypatch.setattr(catalog_populate, "build_and_cache_context",
                        lambda conn, path: pytest.fail("a shipped cache must not be rebuilt"))

    catalog_populate.populate_state_cached(object(), object(), model_path)

    assert [(c.tables, types) for c, types in installed] == [(["t"], {"c": 1})]
    assert not os.path.exists(os.path.join(str(tmp_path / "state"), os.path.basename(shipped)))


# --- the Calcite JVM starts from the runtime the bundle carries --------------------------


def test_no_java_home_named_leaves_jvm_discovery_to_jpype(monkeypatch):
    monkeypatch.delenv(calcite_backend.JAVA_HOME_ENV, raising=False)
    assert calcite_backend.bundled_jvm_path() is None


@pytest.mark.parametrize("relative", [
    os.path.join("lib", "server", "libjvm.so"),
    os.path.join("lib", "server", "libjvm.dylib"),
    os.path.join("bin", "server", "jvm.dll"),
])
def test_java_home_named_resolves_its_jvm_library(tmp_path, monkeypatch, relative):
    library = tmp_path / "jre" / relative
    library.parent.mkdir(parents=True)
    library.write_bytes(b"")
    monkeypatch.setenv(calcite_backend.JAVA_HOME_ENV, str(tmp_path / "jre"))
    assert calcite_backend.bundled_jvm_path() == str(library)


def test_java_home_named_without_a_jvm_library_is_an_error_by_name(tmp_path, monkeypatch):
    monkeypatch.setenv(calcite_backend.JAVA_HOME_ENV, str(tmp_path))
    with pytest.raises(RuntimeError, match=calcite_backend.JAVA_HOME_ENV):
        calcite_backend.bundled_jvm_path()
