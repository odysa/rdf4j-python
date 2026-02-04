class Rdf4jError(Exception):
    """Base exception for all RDF4J SDK errors.

    All exceptions raised by the RDF4J Python SDK inherit from this class,
    allowing users to catch all SDK-related errors with a single except clause.

    Example:
        try:
            await repo.query("SELECT * WHERE { ?s ?p ?o }")
        except Rdf4jError as e:
            # Catches any RDF4J SDK error
            print(f"RDF4J error: {e}")
    """


class RepositoryError(Rdf4jError):
    """Base exception for repository-related errors."""


class RepositoryCreationException(RepositoryError):
    """Exception raised when a repository creation fails."""


class RepositoryDeletionException(RepositoryError):
    """Exception raised when a repository deletion fails."""


class RepositoryNotFoundException(RepositoryError):
    """Exception raised when a repository is not found."""


class RepositoryInternalException(RepositoryError):
    """Exception raised when a repository internal error occurs."""


class RepositoryUpdateException(RepositoryError):
    """Exception raised when a repository update fails."""


class NamespaceException(Rdf4jError):
    """Exception raised when a namespace operation fails."""


class NetworkError(Rdf4jError):
    """Exception raised when a network/connection error occurs."""


class QueryError(Rdf4jError):
    """Exception raised when a SPARQL query is invalid or fails."""


class QueryTypeMismatchError(QueryError):
    """Exception raised when query type doesn't match the method called.

    For example, calling select() with an ASK query when strict=True.

    Attributes:
        expected: The expected query type (e.g., "SELECT").
        actual: The detected query type (e.g., "ASK").
        query: The original query string.
    """

    def __init__(self, expected: str, actual: str, query: str):
        self.expected = expected
        self.actual = actual
        self.query = query
        truncated = query[:100] + "..." if len(query) > 100 else query
        super().__init__(
            f"Expected {expected} query but detected {actual}. Query: {truncated}"
        )


class TransactionError(Rdf4jError):
    """Base exception for transaction-related errors."""


class TransactionStateError(TransactionError):
    """Exception raised when a transaction operation is invalid for the current state.

    For example, trying to commit an already committed transaction,
    or trying to add statements to a closed transaction.
    """
