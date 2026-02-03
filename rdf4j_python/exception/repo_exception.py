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
