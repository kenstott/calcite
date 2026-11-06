# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An ErrorResponse carries what the database said, not only the wrapper Calcite put round it.

A govdata replica read failed with "While executing SQL [...] on JDBC sub-schema" and nothing
more: DuckDB's reason was that exception's cause, and the ErrorResponse held ``str(exc)``.
"""

from __future__ import annotations

import io
import json

from pgwire_calcite.errors import error_text

_SQL = 'SELECT "rate" FROM "health"."cdc_county_injury_mortality"'
_DUCKDB = "Conversion Error: Could not convert string 'n/a' to DOUBLE"


def _wrapped_failure():
    """A JPype exception shaped as Calcite raises it: a SQLException whose cause holds the reason."""
    import jpype

    sql_exception = jpype.JClass("java.sql.SQLException")
    runtime = jpype.JClass("java.lang.RuntimeException")
    duckdb = sql_exception(_DUCKDB)
    wrapper = runtime(f"While executing SQL [{_SQL}] on JDBC sub-schema", duckdb)
    outer = sql_exception(f'Error while executing SQL "{_SQL}": {wrapper.getMessage()}', wrapper)
    try:
        raise outer
    except Exception as exc:  # the JPype exception, as the backend sees it
        return exc


def test_each_java_cause_follows_the_message(calcite_backend):
    text = error_text(_wrapped_failure())
    assert text.startswith("java.sql.SQLException: Error while executing SQL")
    assert text.endswith("\nCaused by: java.sql.SQLException: " + _DUCKDB)
    # The wrapper's own text is already in the message, so it is not repeated as a cause.
    assert text.count("on JDBC sub-schema") == 1


def test_a_python_wrapper_reports_the_java_causes_it_was_raised_from(calcite_backend):
    java = _wrapped_failure()
    try:
        raise RuntimeError(str(java)) from java  # as the session's execute path re-raises
    except RuntimeError as exc:
        assert _DUCKDB in error_text(exc)


def test_a_python_exception_is_its_message():
    assert error_text(ValueError("no such table")) == "no such table"


def test_the_error_response_holds_the_cause(calcite_backend):
    from pgwire_calcite.server import CalciteHandler

    sink = io.BytesIO()
    handler = CalciteHandler.__new__(CalciteHandler)
    handler.wfile = sink  # type: ignore[attr-defined]
    CalciteHandler.send_error(handler, _wrapped_failure())
    assert _DUCKDB.encode() in sink.getvalue()


def test_the_sidecar_payload_holds_the_cause(calcite_backend):
    from pgwire_calcite.sidecar import _error_payload

    assert _DUCKDB in json.loads(_error_payload(_wrapped_failure()))["message"]
