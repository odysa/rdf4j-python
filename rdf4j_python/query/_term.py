"""Term serialization for SPARQL query building."""

from __future__ import annotations

import re
from typing import Union

import pyoxigraph as og

Term = Union[str, og.NamedNode, og.Variable, og.Literal, og.BlankNode]

_PREFIXED_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.-]*:[a-zA-Z_][a-zA-Z0-9_.-]*$")


def serialize_term(term: Term) -> str:
    """Convert a term to its SPARQL string representation.

    Handles:
    - str starting with ``?`` — pass through as variable
    - str ``"a"`` — pass through (rdf:type shorthand)
    - str matching ``prefix:local`` — pass through
    - str wrapped in ``<>`` — pass through as full IRI
    - str wrapped in ``"`` or ``'`` — pass through as literal
    - str that is a SPARQL expression (e.g. aggregates) — pass through
    - ``IRI`` (NamedNode) — ``<value>``
    - ``Variable`` — ``?value``
    - ``Literal`` — ``"value"`` with optional ``@lang`` or ``^^<datatype>``
    - ``BlankNode`` — ``_:value``
    """
    if isinstance(term, og.NamedNode):
        return f"<{term.value}>"
    if isinstance(term, og.Variable):
        return f"?{term.value}"
    if isinstance(term, og.Literal):
        value = term.value.replace("\\", "\\\\").replace('"', '\\"')
        if term.language:
            return f'"{value}"@{term.language}'
        if term.datatype and term.datatype.value != "http://www.w3.org/2001/XMLSchema#string":
            return f'"{value}"^^<{term.datatype.value}>'
        return f'"{value}"'
    if isinstance(term, og.BlankNode):
        return f"_:{term.value}"
    if isinstance(term, str):
        return term
    raise TypeError(f"Unsupported term type: {type(term)}")
