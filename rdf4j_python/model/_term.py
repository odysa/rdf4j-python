"""
Wrapper for rdflib.term classes.
"""

from typing import Any, Optional

from rdflib.term import BNode as _BNode
from rdflib.term import IdentifiedNode as _IdentifiedNode
from rdflib.term import Identifier as _Identifier
from rdflib.term import Literal as _Literal
from rdflib.term import Node as _Node
from rdflib.term import URIRef as _URIRef
from rdflib.term import Variable as _Variable


class IRI(_URIRef):
    """
    Public-facing wrapper for rdflib.term.URIRef.
    """

    def __new__(cls, iri: str) -> "IRI":
        return super().__new__(cls, iri)

    def as_rdflib(self) -> _URIRef:
        return _URIRef(self)


class Variable(_Variable):
    """
    Public-facing wrapper for rdflib.term.Variable.
    """

    def __new__(cls, variable: str) -> "Variable":
        return super().__new__(cls, variable)

    def as_rdflib(self) -> _Variable:
        return _Variable(self)


class Node(_Node):
    """
    Public-facing wrapper for rdflib.term.Node.
    """

    def __new__(cls, node: str) -> "Node":
        return super().__new__(cls, node)

    def as_rdflib(self) -> _Node:
        return _Node(self)


class Literal(_Literal):
    """
    Public-facing wrapper for rdflib.term.Literal.
    """

    def __new__(
        cls,
        value: Any,
        lang: Optional[str] = None,
        datatype: Optional[str] = None,
    ) -> "Literal":
        return super().__new__(cls, value, lang=lang, datatype=datatype)

    def as_rdflib(self) -> _Literal:
        return _Literal(self, lang=self.language, datatype=self.datatype)


class BlankNode(_BNode):
    """
    Public-facing wrapper for rdflib.term.BNode.
    """

    def __new__(cls, identifier: Optional[str] = None) -> "BlankNode":
        return super().__new__(cls, identifier)

    def as_rdflib(self) -> _BNode:
        return _BNode(self)


class Identifier(_Identifier):
    """
    Public-facing wrapper for rdflib.term.Identifier.
    """

    def __new__(cls, identifier: str) -> "Identifier":
        return super().__new__(cls, identifier)

    def as_rdflib(self) -> _Identifier:
        return _Identifier(self)


class IdentifiedNode(_IdentifiedNode):
    """
    Public-facing wrapper for rdflib.term.IdentifiedNode.
    """

    def __new__(cls, identifier: str) -> "IdentifiedNode":
        return super().__new__(cls, identifier)

    def as_rdflib(self) -> _IdentifiedNode:
        return _IdentifiedNode(self)
