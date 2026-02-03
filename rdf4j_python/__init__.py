"""
RDF4J Python is a Python library for interacting with RDF4J repositories.
"""

try:
    from ._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"

from ._driver import (
    AsyncNamedGraph,
    AsyncRdf4j,
    AsyncRdf4JRepository,
    AsyncTransaction,
    IsolationLevel,
    TransactionState,
)
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
    TransactionError,
    TransactionStateError,
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
    # Version
    "__version__",
    # Main classes
    "AsyncRdf4j",
    "AsyncRdf4JRepository",
    "AsyncNamedGraph",
    # Transaction
    "AsyncTransaction",
    "IsolationLevel",
    "TransactionState",
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
    "TransactionError",
    "TransactionStateError",
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
