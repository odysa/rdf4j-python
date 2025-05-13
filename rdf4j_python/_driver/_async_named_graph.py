from rdf4j_python.utils.const import Rdf4jContentType


class AsyncNamedGraph:
    """Asynchronous interface for operations on a specific RDF4J named graph."""

    def __init__(self, client, repository_id: str, graph_uri: str):
        """Initializes the AsyncNamedGraph.

        Args:
            client (AsyncApiClient): The RDF4J HTTP client.
            repository_id (str): The ID of the RDF4J repository.
            graph_uri (str): The URI identifying the named graph.
        """
        self._client = client
        self._repository_id = repository_id
        self._graph_uri = graph_uri

    async def get(self, accept: Rdf4jContentType = Rdf4jContentType.TURTLE) -> str:
        """Fetches all RDF statements from this named graph.

        Args:
            accept (Rdf4jContentType, optional): The desired RDF response format.
                Defaults to Turtle.

        Returns:
            str: RDF data serialized in the requested format.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/rdf-graphs/{self._graph_uri}"
        headers = {"Accept": accept.value}
        response = await self._client.get(path, headers=headers)
        response.raise_for_status()
        return response.text

    async def add(
        self, rdf_data: str, content_type: Rdf4jContentType = Rdf4jContentType.TURTLE
    ):
        """Adds RDF statements to this named graph.

        Args:
            rdf_data (str): RDF content to add.
            content_type (Rdf4jContentType, optional): Serialization format of input RDF data.
                Defaults to Turtle.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/rdf-graphs/{self._graph_uri}"
        headers = {"Content-Type": content_type.value}
        response = await self._client.post(path, data=rdf_data, headers=headers)
        response.raise_for_status()

    async def clear(self):
        """Deletes all statements from this named graph.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/rdf-graphs/{self._graph_uri}"
        response = await self._client.delete(path)
        response.raise_for_status()

    @property
    def uri(self) -> str:
        """Returns the URI of the named graph.

        Returns:
            str: The graph URI.
        """
        return self._graph_uri
