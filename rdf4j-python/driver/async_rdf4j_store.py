import httpx
from async_repository import AsyncRepository
from client.client import AsyncApiClient
from utils.const import Rdf4jContentType


class AsyncRdf4jStore:
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

    async def list_repositories(self):
        """
        List all RDF4J repositories.
        :return: List of repository IDs.
        """
        response = await self._client.get("/repositories")
        content_type = response.headers.get("Content-Type", "")
        if Rdf4jContentType.SPARQL_RESULTS_JSON in content_type:
            return response.json()
        return response.text

    def get_repository(self, repository_id: str) -> AsyncRepository:
        return AsyncRepository(self._client, repository_id)

    async def create_repository(
        self,
        repository_id: str,
        rdf_config_data: str,
        content_type: Rdf4jContentType = Rdf4jContentType.TURTLE,
    ):
        """
        Create a new RDF4J repository.

        :param repository_id: Repository ID to create.
        :param rdf_config_data: RDF config in Turtle, RDF/XML, etc.
        :param content_type: MIME type of RDF config.
        """
        path = f"/repositories/{repository_id}"
        headers = {"Content-Type": content_type.value}
        response: httpx.Response = await self._client.put(
            path, data=rdf_config_data, headers=headers
        )
        if response.status_code != httpx.codes.CREATED:
            raise Exception(
                f"Repository creation failed: {response.status_code} - {response.text}"
            )

    async def delete_repository(self, repository_id: str):
        """
        Delete an RDF4J repository and its data/config.

        :param repository_id: The repository ID to delete.
        """
        path = f"/repositories/{repository_id}"
        response = await self._client.delete(path)
        if response.status_code != 204:
            raise Exception(
                f"Failed to delete repository '{repository_id}': {response.status_code} - {response.text}"
            )
