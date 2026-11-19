# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's array text form (array_in / array_out), ported from Provisa's
provisa/transpiler/pg_literals.py (REQ-2255)."""

from __future__ import annotations


class NotAnArrayLiteral(ValueError):
    """The string is not array_in's text form of an array."""


def array_elements(text: str) -> list:
    """The elements of an array's text form (array_in), nested lists for its dimensions: each
    element its own text, or None for an unquoted NULL."""
    elements, end = _array(text, _skip_space(text, 0))
    if _skip_space(text, end) != len(text):
        raise NotAnArrayLiteral(f"junk after the array in {text!r}")
    return elements


def array_text(elements: list) -> str:
    """An array's text form (array_out): each element double-quoted with its quotes and
    backslashes escaped, NULL bare, a nested list a nested dimension."""
    parts = []
    for e in elements:
        if e is None:
            parts.append("NULL")
        elif isinstance(e, (list, tuple)):
            parts.append(array_text(list(e)))
        else:
            parts.append('"' + str(e).replace("\\", "\\\\").replace('"', '\\"') + '"')
    return "{" + ",".join(parts) + "}"


def _skip_space(text: str, i: int) -> int:
    while i < len(text) and text[i].isspace():
        i += 1
    return i


def _array(text: str, i: int) -> tuple:
    if i >= len(text) or text[i] != "{":
        raise NotAnArrayLiteral(f"no '{{' at {i} in {text!r}")
    out: list = []
    i = _skip_space(text, i + 1)
    if i < len(text) and text[i] == "}":
        return out, i + 1
    while True:
        i = _skip_space(text, i)
        if i < len(text) and text[i] == "{":
            element, i = _array(text, i)
        else:
            element, i = _element(text, i)
        out.append(element)
        i = _skip_space(text, i)
        if i >= len(text):
            raise NotAnArrayLiteral(f"unterminated array in {text!r}")
        if text[i] == "}":
            return out, i + 1
        if text[i] != ",":
            raise NotAnArrayLiteral(f"no ',' at {i} in {text!r}")
        i += 1


def _element(text: str, i: int) -> tuple:
    if i < len(text) and text[i] == '"':
        chars: list = []
        i += 1
        while i < len(text) and text[i] != '"':
            if text[i] == "\\":
                i += 1
            if i >= len(text):
                break
            chars.append(text[i])
            i += 1
        if i >= len(text):
            raise NotAnArrayLiteral(f"unterminated quoted element in {text!r}")
        return "".join(chars), i + 1
    chars = []
    escaped = False
    while i < len(text) and text[i] not in ",}{":
        if text[i] == "\\":
            escaped = True
            i += 1
            if i >= len(text):
                break
        if text[i] == '"':
            raise NotAnArrayLiteral(f"a quote inside an unquoted element in {text!r}")
        chars.append(text[i])
        i += 1
    element = "".join(chars).rstrip()
    if not element:
        raise NotAnArrayLiteral(f"an empty element in {text!r}")
    if not escaped and element.upper() == "NULL":
        return None, i
    return element, i
