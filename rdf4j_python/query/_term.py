"""Term serialization for SPARQL query building."""

from __future__ import annotations

from typing import Union

import pyoxigraph as og

Term = Union[str, og.NamedNode, og.Variable, og.Literal, og.BlankNode]

_XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"


def serialize_term(term: Term) -> str:
    """Convert a term to its SPARQL string representation.

    Strings are passed through as-is (variables, prefixed names, full IRIs, etc.).
    Typed objects (NamedNode, Variable, Literal, BlankNode) are formatted accordingly.
    """
    if isinstance(term, str):
        return term
    if isinstance(term, og.NamedNode):
        return f"<{term.value}>"
    if isinstance(term, og.Variable):
        return f"?{term.value}"
    if isinstance(term, og.Literal):
        value = term.value.replace("\\", "\\\\").replace('"', '\\"')
        if term.language:
            return f'"{value}"@{term.language}'
        if term.datatype and term.datatype.value != _XSD_STRING:
            return f'"{value}"^^<{term.datatype.value}>'
        return f'"{value}"'
    if isinstance(term, og.BlankNode):
        return f"_:{term.value}"
    raise TypeError(f"Unsupported term type: {type(term)}")
