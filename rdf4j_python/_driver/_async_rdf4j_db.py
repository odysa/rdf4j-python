import httpx
import pyoxigraph as og

from rdf4j_python._client import AsyncApiClient
from rdf4j_python.exception.repo_exception import (
    RepositoryCreationException,
    RepositoryDeletionException,
)
from rdf4j_python.model._repository_info import RepositoryMetadata
from rdf4j_python.model.repository_config import RepositoryConfig
from rdf4j_python.utils.const import Rdf4jContentType

from ._async_repository import AsyncRdf4JRepository


class AsyncRdf4j:
    """Asynchronous entry point for interacting with an RDF4J server."""

    _client: AsyncApiClient
    _base_url: str

    def __init__(self, base_url: str):
        """Initializes the RDF4J API client.

        Args:
            base_url (str): Base URL of the RDF4J server.
        """
        self._base_url = base_url.rstrip("/")

    async def __aenter__(self):
        """Enters the async context and initializes the HTTP client.

        Returns:
            AsyncRdf4j: The initialized RDF4J interface.
        """
        self._client = await AsyncApiClient(base_url=self._base_url).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Closes the HTTP client when exiting the async context."""
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def get_protocol_version(self) -> str:
        """Fetches the RDF4J protocol version.

        Returns:
            str: The protocol version string.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        response = await self._client.get("/protocol")
        response.raise_for_status()
        return response.text

    async def list_repositories(self) -> list[RepositoryMetadata]:
        """Lists all available RDF4J repositories.

        Returns:
            list[RepositoryMetadata]: A list of repository metadata objects.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        response = await self._client.get(
            "/repositories",
            headers={"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON},
        )
        query_solutions = og.parse_query_results(
            response.text, format=og.QueryResultsFormat.JSON
        )
        return [
            RepositoryMetadata.from_sparql_query_solution(query_solution)
            for query_solution in query_solutions
        ]

    async def get_repository(self, repository_id: str) -> AsyncRdf4JRepository:
        """Gets an interface to a specific RDF4J repository.

        Args:
            repository_id (str): The ID of the repository.

        Returns:
            AsyncRdf4JRepository: An async interface for the repository.
        """
        return AsyncRdf4JRepository(self._client, repository_id)

    async def create_repository(
        self,
        config: RepositoryConfig,
    ) -> AsyncRdf4JRepository:
        """Creates a new RDF4J repository using RDF configuration.

        Args:
            repository_id (str): The repository ID to create.
            config (RepositoryConfig): RDF configuration.

        Returns:
            AsyncRdf4JRepository: An async interface to the newly created repository.

        Raises:
            RepositoryCreationException: If repository creation fails.
        """
        path = f"/repositories/{config.repo_id}"
        headers = {"Content-Type": Rdf4jContentType.TURTLE}
        response: httpx.Response = await self._client.put(
            path, content=config.to_turtle(), headers=headers
        )
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryCreationException(
                f"Repository creation failed: {response.status_code} - {response.text}"
            )
        return AsyncRdf4JRepository(self._client, config.repo_id)

    async def delete_repository(self, repository_id: str):
        """Deletes a repository and all its data and configuration.

        Args:
            repository_id (str): The ID of the repository to delete.

        Raises:
            RepositoryDeletionException: If the deletion fails.
        """
        path = f"/repositories/{repository_id}"
        response = await self._client.delete(path)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryDeletionException(
                f"Failed to delete repository '{repository_id}': {response.status_code} - {response.text}"
            )
