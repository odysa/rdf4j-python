from typing import TypeAlias

import pyoxigraph as og

IRI: TypeAlias = og.NamedNode
BlankNode: TypeAlias = og.BlankNode
Literal: TypeAlias = og.Literal
DefaultGraph: TypeAlias = og.DefaultGraph
Variable: TypeAlias = og.Variable

Quad: TypeAlias = og.Quad
Triple: TypeAlias = og.Triple

Subject: TypeAlias = IRI | BlankNode | Triple
Predicate: TypeAlias = IRI
Object: TypeAlias = IRI | BlankNode | Literal
Context: TypeAlias = IRI | BlankNode | DefaultGraph | None

# Type for RDF terms used in SPARQL variable bindings
Term: TypeAlias = IRI | BlankNode | Literal
# Type for SPARQL variable bindings (variable name -> RDF term)
QueryBindings: TypeAlias = dict[str, Term]

QuadResultSet: TypeAlias = og.QuadParser

__all__ = [
    "IRI",
    "BlankNode",
    "Literal",
    "DefaultGraph",
    "Variable",
    "Quad",
    "Triple",
    "Subject",
    "Predicate",
    "Object",
    "Context",
    "Term",
    "QueryBindings",
    "QuadResultSet",
]
