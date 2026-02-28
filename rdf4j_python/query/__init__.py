"""SPARQL query builder for RDF4J Python."""

from ._builder import AskQuery, ConstructQuery, DescribeQuery, SelectQuery
from ._pattern import GraphPattern


def select(*variables: str) -> SelectQuery:
    """Create a new ``SELECT`` query builder."""
    return SelectQuery(*variables)


def ask() -> AskQuery:
    """Create a new ``ASK`` query builder."""
    return AskQuery()


def construct(*templates: tuple) -> ConstructQuery:
    """Create a new ``CONSTRUCT`` query builder."""
    return ConstructQuery(*templates)


def describe(*resources) -> DescribeQuery:
    """Create a new ``DESCRIBE`` query builder."""
    return DescribeQuery(*resources)


__all__ = [
    "select",
    "ask",
    "construct",
    "describe",
    "GraphPattern",
    "SelectQuery",
    "AskQuery",
    "ConstructQuery",
    "DescribeQuery",
]
