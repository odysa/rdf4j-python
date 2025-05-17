class RepositoryCreationException(Exception):
    """
    Exception raised when a repository creation fails.
    """


class RepositoryDeletionException(Exception):
    """
    Exception raised when a repository deletion fails.
    """


class NamespaceException(Exception):
    """
    Exception raised when a namespace operation fails.
    """


class RepositoryNotFoundException(Exception):
    """
    Exception raised when a repository is not found.
    """


class RepositoryInternalException(Exception):
    """
    Exception raised when a repository internal error occurs.
    """


class RepositoryUpdateException(Exception):
    """
    Exception raised when a repository update fails.
    """
