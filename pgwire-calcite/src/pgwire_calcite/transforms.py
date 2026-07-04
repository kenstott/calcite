# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Targeted transform layer for PG-only constructs (PGW-018).

PostgreSQL constructs that have no clean Calcite mapping are either rewritten
here to an equivalent Calcite form, or rejected **explicitly** with a clear
error — never silently mistranslated (PGW-018, and CLAUDE.md rule 6: no silent
error handling). The transforms operate on the sqlglot AST after PG parse and
before Calcite generation.

Convert:
- ``string_agg(x, sep)``   -> ``LISTAGG(x, sep)``

Reject loudly (clear message, client sees a normal ErrorResponse):
- ``DISTINCT ON (...)``     — no faithful Calcite equivalent
- POSIX regex ``~`` ``~*`` ``!~`` ``!~*``
- array / JSON operators ``@>`` ``<@`` ``->`` ``->>`` ``#>`` ``#>>``
- ``generate_series(...)``  — not in Calcite core
"""

from __future__ import annotations

from sqlglot import exp


class UnsupportedConstruct(ValueError):
    """A PG construct with no safe Calcite mapping. Rejected, never guessed."""


_REJECTED_FUNCTIONS = {
    "generate_series": "generate_series() has no Calcite-core equivalent",
    "string_to_array": "array constructors are not mapped to Calcite",
    "array_agg": "array_agg() is not mapped to Calcite (use LISTAGG or reject)",
}

# Binary operator tokens that sqlglot may surface as Anonymous/Binary ops.
_REJECTED_OPERATORS = {
    "~": "POSIX regex operator ~",
    "~*": "case-insensitive POSIX regex operator ~*",
    "!~": "negated POSIX regex operator !~",
    "!~*": "negated case-insensitive POSIX regex operator !~*",
    "@>": "array/range contains operator @>",
    "<@": "array/range contained-by operator <@",
    "->": "JSON field operator ->",
    "->>": "JSON field-as-text operator ->>",
    "#>": "JSON path operator #>",
    "#>>": "JSON path-as-text operator #>>",
}


def _reject(reason: str) -> None:
    raise UnsupportedConstruct(
        reason
        + " — this PostgreSQL construct is not translated to Calcite. "
        "Rewrite the query or file a mapping request; it will not be silently mistranslated."
    )


def _convert_json_ops(tree: exp.Expression) -> None:
    """Map PG JSON operators to Calcite (JSON extension surface, PGW-049).

    ``data->>'k'`` -> ``JSON_VALUE(data, 'lax $.k')`` (scalar text)
    ``data->'k'``  -> ``JSON_QUERY(data, 'lax $.k')`` (json subtree)
    """
    scalar_t = getattr(exp, "JSONExtractScalar", None)
    extract_t = getattr(exp, "JSONExtract", None)
    types = tuple(t for t in (scalar_t, extract_t) if t is not None)
    if not types:
        return
    for node in list(tree.find_all(*types)):
        col = node.this
        keys = [str(k.this) for k in node.find_all(exp.JSONPathKey)]
        if not keys:
            continue
        path = "lax $" + "".join(f".{k}" for k in keys)
        fn = "JSON_VALUE" if (scalar_t and isinstance(node, scalar_t)) else "JSON_QUERY"
        node.replace(exp.func(fn, col, exp.Literal.string(path)))


def apply(tree: exp.Expression, json_enabled: bool = False) -> exp.Expression:
    """Apply conversions and reject-rules to a parsed PG AST. Returns the AST.

    Raises ``UnsupportedConstruct`` on any reject-listed construct. When the JSON
    extension surface is enabled (PGW-049), ``->``/``->>`` are converted instead
    of rejected.
    """
    if json_enabled:
        _convert_json_ops(tree)
    # DISTINCT ON — sqlglot models it as exp.Distinct with an ``on`` arg.
    for distinct in tree.find_all(exp.Distinct):
        if distinct.args.get("on") is not None:
            _reject("DISTINCT ON")

    # generate_series() — sqlglot parses this into a first-class node, not Anonymous.
    _gen_types = tuple(
        t for t in (getattr(exp, "GenerateSeries", None), getattr(exp, "ExplodingGenerateSeries", None))
        if t is not None
    )
    if _gen_types:
        for _node in tree.walk():
            if isinstance(_node, _gen_types):
                _reject("generate_series() has no Calcite-core equivalent")

    # POSIX regex / array / JSON operators.
    for node in tree.walk():
        name = type(node).__name__
        if name in ("RegexpLike", "Glob"):
            # sqlglot may parse ~ into a regex node depending on version.
            _reject("POSIX regex match")
        if isinstance(node, exp.Binary):
            op = node.args.get("this")
        # Textual operator surfaces (version-dependent): check raw sql tokens.
    # Fallback textual scan for operator tokens sqlglot passes through opaquely.
    sql_text = tree.sql(dialect="postgres")
    for token, reason in _REJECTED_OPERATORS.items():
        # word-ish boundary: these are punctuation, so a plain containment check
        # is adequate and avoids false positives on quoted string literals below.
        if _contains_operator(sql_text, token):
            _reject(reason)

    # Rejected functions + string_agg -> LISTAGG conversion.
    for func in list(tree.find_all(exp.Anonymous)):
        fname = (func.name or "").lower()
        if fname in _REJECTED_FUNCTIONS:
            _reject(_REJECTED_FUNCTIONS[fname])
        if fname == "string_agg":
            _convert_string_agg(func)

    # sqlglot has a first-class GroupConcat/StringAgg node in some versions.
    for node in list(tree.find_all(exp.GroupConcat)):
        node.replace(_to_listagg(node.this, node.args.get("separator")))

    return tree


def _contains_operator(sql_text: str, token: str) -> bool:
    """True if ``token`` appears outside single-quoted string literals."""
    out = []
    in_str = False
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "'":
            in_str = not in_str
            out.append(" ")
        else:
            out.append(" " if in_str else ch)
        i += 1
    return token in "".join(out)


def _convert_string_agg(func: exp.Anonymous) -> None:
    args = func.expressions
    value = args[0] if args else None
    sep = args[1] if len(args) > 1 else None
    func.replace(_to_listagg(value, sep))


def _to_listagg(value, separator) -> exp.Expression:
    params = [value] if value is not None else []
    if separator is not None:
        params.append(separator)
    return exp.func("LISTAGG", *params)
