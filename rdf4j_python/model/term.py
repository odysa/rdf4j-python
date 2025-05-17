from typing import Optional, Tuple, TypeAlias

from rdflib import URIRef as _URIRef
from rdflib.term import IdentifiedNode, Node

IRI: TypeAlias = _URIRef


Subject: TypeAlias = Node
Predicate: TypeAlias = Node
Object: TypeAlias = Node
Context: TypeAlias = Optional[IdentifiedNode]

RDFStatement: TypeAlias = Tuple[Subject, Predicate, Object, Context]
