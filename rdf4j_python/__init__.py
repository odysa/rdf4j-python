"""
RDF4J Python is a Python library for interacting with RDF4J repositories.
"""

from ._driver import AsyncNamedGraph, AsyncRdf4j, AsyncRdf4JRepository
from .exception import (
    NamespaceException,
    NetworkError,
    QueryError,
    Rdf4jError,
    RepositoryCreationException,
    RepositoryDeletionException,
    RepositoryError,
    RepositoryInternalException,
    RepositoryNotFoundException,
    RepositoryUpdateException,
)
from .model import (
    IRI,
    BlankNode,
    Context,
    DefaultGraph,
    Literal,
    Namespace,
    Object,
    Predicate,
    Quad,
    QuadResultSet,
    RepositoryMetadata,
    Subject,
    Triple,
    Variable,
)

__all__ = [
    # Main classes
    "AsyncRdf4j",
    "AsyncRdf4JRepository",
    "AsyncNamedGraph",
    # Exceptions
    "Rdf4jError",
    "RepositoryError",
    "RepositoryCreationException",
    "RepositoryDeletionException",
    "RepositoryNotFoundException",
    "RepositoryInternalException",
    "RepositoryUpdateException",
    "NamespaceException",
    "NetworkError",
    "QueryError",
    # Model types
    "Namespace",
    "RepositoryMetadata",
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
    "QuadResultSet",
]
