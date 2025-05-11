from typing import Union

import httpx
import rdflib

from rdf4j_python import AsyncApiClient
from rdf4j_python.exception.repo_exception import (
    RepositoryCreationException,
    RepositoryDeletionException,
)
from rdf4j_python.model._repository_info import RepositoryMetadata
from rdf4j_python.utils.const import Rdf4jContentType

from ._async_repository import AsyncRdf4JRepository


class AsyncRdf4j:
    _client: AsyncApiClient
    _base_url: str

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")

    async def __aenter__(self):
        self._client = await AsyncApiClient(base_url=self._base_url).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def get_protocol_version(self) -> str:
        response = await self._client.get("/protocol")
        response.raise_for_status()
        return response.text

    async def list_repositories(self) -> list[RepositoryMetadata]:
        """
        List all RDF4J repositories.

        :return: List of repository information.
        """
        response = await self._client.get(
            "/repositories",
            headers={"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON},
        )
        result = rdflib.query.Result.parse(
            response, format=Rdf4jContentType.SPARQL_RESULTS_JSON
        )

        return [
            RepositoryMetadata.from_rdflib_binding(binding)
            for binding in result.bindings
        ]

    async def get_repository(self, repository_id: str) -> AsyncRdf4JRepository:
        """
        Get an AsyncRepository instance for the specified repository ID.

        :param repository_id: The ID of the repository.
        :return: An instance of AsyncRepository.
        """
        return AsyncRdf4JRepository(self._client, repository_id)

    async def create_repository(
        self,
        repository_id: str,
        rdf_config_data: str,
        content_type: Union[Rdf4jContentType, str] = Rdf4jContentType.TURTLE,
    ) -> AsyncRdf4JRepository:
        """
        Create a new RDF4J repository.

        :param repository_id: Repository ID to create.
        :param rdf_config_data: RDF config in Turtle, RDF/XML, etc.
        :param content_type: MIME type of RDF config.
        """
        path = f"/repositories/{repository_id}"

        if isinstance(content_type, Rdf4jContentType):
            content_type = content_type.value
        headers = {"Content-Type": content_type}

        response: httpx.Response = await self._client.put(
            path, content=rdf_config_data, headers=headers
        )
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryCreationException(
                f"Repository creation failed: {response.status_code} - {response.text}"
            )
        return AsyncRdf4JRepository(self._client, repository_id)

    async def delete_repository(self, repository_id: str):
        """
        Delete an RDF4J repository and its data/config.

        :param repository_id: The repository ID to delete.
        """
        path = f"/repositories/{repository_id}"
        response = await self._client.delete(path)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryDeletionException(
                f"Failed to delete repository '{repository_id}': {response.status_code} - {response.text}"
            )
