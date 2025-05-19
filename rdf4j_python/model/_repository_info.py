from dataclasses import dataclass

import pyoxigraph as og


@dataclass
class RepositoryMetadata:
    """
    Represents a repository metadata RDF4J.
    """

    id: str  # The repository identifier
    uri: str  # The full URI to the repository
    title: str  # A human-readable title (currently reusing id)
    readable: bool  # Whether the repository is readable
    writable: bool  # Whether the repository is writable

    def __str__(self):
        """
        Returns a string representation of the RepositoryMetadata.

        Returns:
            str: A string representation of the RepositoryMetadata.
        """
        return f"Repository(id={self.id}, title={self.title}, uri={self.uri})"

    @classmethod
    def from_sparql_query_solution(
        cls, query_solution: og.QuerySolution
    ) -> "RepositoryMetadata":
        """
        Create a RepositoryMetadata instance from a SPARQL query result.

        Args:
            query_solution (og.QuerySolution): The SPARQL query result.

        Returns:
            RepositoryMetadata: The RepositoryMetadata instance.

        Raises:
            ValueError: If the query solution is missing required fields.
        """

        # Construct and return the Repository object
        if query_solution["id"] is None:
            raise ValueError("id is required")
        if query_solution["uri"] is None:
            raise ValueError("uri is required")
        if query_solution["title"] is None:
            raise ValueError("title is required")
        if query_solution["readable"] is None:
            raise ValueError("readable is required")
        if query_solution["writable"] is None:
            raise ValueError("writable is required")

        return cls(
            id=query_solution["id"].value,
            uri=query_solution["uri"].value,
            title=query_solution["title"].value,
            readable=bool(query_solution["readable"].value),
            writable=bool(query_solution["writable"].value),
        )
