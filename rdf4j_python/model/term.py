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


QuadResultSet: TypeAlias = og.QuadParser
