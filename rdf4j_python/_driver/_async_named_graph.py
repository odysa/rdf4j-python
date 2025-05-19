from typing import Iterable

import pyoxigraph as og

from rdf4j_python._client import AsyncApiClient
from rdf4j_python.model.term import IRI, Quad, QuadResultSet, Triple
from rdf4j_python.utils.const import Rdf4jContentType
from rdf4j_python.utils.helpers import serialize_statements


class AsyncNamedGraph:
    """Asynchronous interface for operations on a specific RDF4J named graph."""

    def __init__(self, client: AsyncApiClient, repository_id: str, graph_uri: str):
        """Initializes the AsyncNamedGraph.

        Args:
            client (AsyncApiClient): The RDF4J HTTP client.
            repository_id (str): The ID of the RDF4J repository.
            graph_uri (str): The URI identifying the named graph.
        """
        self._client = client
        self._repository_id = repository_id
        self._graph_uri = graph_uri

    async def get(self) -> QuadResultSet:
        """Fetches all RDF statements from this named graph.

        Returns:
            QuadResultSet: RDF data serialized in the requested format.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/rdf-graphs/{self._graph_uri}"
        headers = {"Accept": Rdf4jContentType.NQUADS}
        response = await self._client.get(path, headers=headers)
        response.raise_for_status()
        return og.parse(response.content, format=og.RdfFormat.N_QUADS)

    async def add(self, statements: Iterable[Quad] | Iterable[Triple]):
        """Adds RDF statements to this named graph.

        Args:
            statements (Iterable[Quad] | Iterable[Triple]): RDF statements to add.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/rdf-graphs/{self._graph_uri}"
        headers = {"Content-Type": Rdf4jContentType.NQUADS}
        response = await self._client.post(
            path, content=serialize_statements(statements), headers=headers
        )
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
    def iri(self) -> IRI:
        """Returns the IRI of the named graph.

        Returns:
            str: The graph IRI.
        """
        return IRI(
            f"{self._client.get_base_url()}/repositories/{self._repository_id}/rdf-graphs/{self._graph_uri}"
        )
