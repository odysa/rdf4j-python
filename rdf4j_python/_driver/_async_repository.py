from typing import Iterable, Optional

import httpx
import rdflib
import rdflib.resource

from rdf4j_python import AsyncApiClient
from rdf4j_python._driver._async_named_graph import AsyncNamedGraph
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryInternalException,
    RepositoryNotFoundException,
)
from rdf4j_python.model import IdentifiedNode, Namespace, Node
from rdf4j_python.utils.const import Rdf4jContentType


class AsyncRdf4JRepository:
    """Asynchronous interface for interacting with an RDF4J repository."""

    def __init__(self, client: AsyncApiClient, repository_id: str):
        """Initializes the repository interface.

        Args:
            client (AsyncApiClient): The RDF4J API client.
            repository_id (str): The ID of the RDF4J repository.
        """
        self._client = client
        self._repository_id = repository_id

    async def query(
        self,
        sparql_query: str,
        infer: bool = True,
        accept: Rdf4jContentType = Rdf4jContentType.SPARQL_RESULTS_JSON,
    ):
        """Executes a SPARQL SELECT query.

        Args:
            sparql_query (str): The SPARQL query string.
            infer (bool): Whether to include inferred statements. Defaults to True.
            accept (Rdf4jContentType): The expected response format.

        Returns:
            dict or str: Parsed JSON results or raw response text.
        """
        path = f"/repositories/{self._repository_id}"
        params = {"query": sparql_query, "infer": str(infer).lower()}
        headers = {"Accept": accept.value}
        response = await self._client.get(path, params=params, headers=headers)
        self._handle_repo_not_found_exception(response)
        if "json" in response.headers.get("Content-Type", ""):
            return response.json()
        return response.text

    async def update(self, sparql_update: str):
        """Executes a SPARQL UPDATE command.

        Args:
            sparql_update (str): The SPARQL update string.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If the update fails.
        """
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.SPARQL_UPDATE.value}
        response = await self._client.post(path, data=sparql_update, headers=headers)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def get_namespaces(self):
        """Retrieves all namespaces in the repository.

        Returns:
            list[Namespace]: A list of namespace objects.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
        """
        path = f"/repositories/{self._repository_id}/namespaces"
        headers = {"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON}
        response = await self._client.get(path, headers=headers)
        result = rdflib.query.Result.parse(
            response, format=Rdf4jContentType.SPARQL_RESULTS_JSON
        )
        self._handle_repo_not_found_exception(response)
        return [Namespace.from_rdflib_binding(binding) for binding in result.bindings]

    async def set_namespace(self, prefix: str, namespace: str):
        """Sets a namespace prefix.

        Args:
            prefix (str): The namespace prefix.
            namespace (str): The namespace URI.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            NamespaceException: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.put(path, content=namespace, headers=headers)
        self._handle_repo_not_found_exception(response)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise NamespaceException(f"Failed to set namespace: {response.text}")

    async def get_namespace(self, prefix: str) -> Namespace:
        """Gets a namespace by its prefix.

        Args:
            prefix (str): The namespace prefix.

        Returns:
            Namespace: The namespace object.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            NamespaceException: If retrieval fails.
        """
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        headers = {"Accept": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.get(path, headers=headers)
        self._handle_repo_not_found_exception(response)

        if response.status_code != httpx.codes.OK:
            raise NamespaceException(f"Failed to get namespace: {response.text}")

        return Namespace(prefix, response.text)

    async def delete_namespace(self, prefix: str):
        """Deletes a namespace by prefix.

        Args:
            prefix (str): The namespace prefix.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If deletion fails.
        """
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        response = await self._client.delete(path)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def clear_all_namespaces(self):
        """Removes all namespaces from the repository.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If clearing fails.
        """
        path = f"/repositories/{self._repository_id}/namespaces"
        response = await self._client.delete(path)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def size(self) -> int:
        """Gets the number of statements in the repository.

        Returns:
            int: The total number of RDF statements.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            RepositoryInternalException: If retrieval fails.
        """
        path = f"/repositories/{self._repository_id}/size"
        response = await self._client.get(path)
        self._handle_repo_not_found_exception(response)

        if response.status_code != httpx.codes.OK:
            raise RepositoryInternalException(f"Failed to get size: {response.text}")

        return int(response.text.strip())

    async def add_statement(
        self,
        subject: Node,
        predicate: Node,
        object: Node,
        context: Optional[IdentifiedNode] = None,
    ):
        """Adds a single RDF statement to the repository.

        Args:
            subject (Node): The subject of the triple.
            predicate (Node): The predicate of the triple.
            object (Node): The object of the triple.
            context (IdentifiedNode): The context (named graph).

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If addition fails.
        """
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.post(
            path,
            data=f"{subject} {predicate} {object} {context}.",
            headers=headers,
        )
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def add_statements(
        self, statements: Iterable[tuple[Node, Node, Node, Optional[IdentifiedNode]]]
    ):
        """Adds a list of RDF statements to the repository.

        Args:
            statements (list[tuple[Node, Node, Node, Optional[IdentifiedNode]]]): A list of RDF statements.
        """
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.post(path, data=statements, headers=headers)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def replace_statements(
        self, rdf_data: str, content_type: Rdf4jContentType = Rdf4jContentType.TURTLE
    ):
        """Replaces all repository statements with the given RDF data.

        Args:
            rdf_data (str): RDF statements to load.
            content_type (Rdf4jContentType): RDF serialization format.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If the operation fails.
        """
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": content_type.value}
        response = await self._client.put(path, data=rdf_data, headers=headers)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def get_named_graph(self, graph: str) -> AsyncNamedGraph:
        """Retrieves a named graph in the repository.

        Returns:
            AsyncNamedGraph: A named graph object.
        """
        return AsyncNamedGraph(self._client, self._repository_id, graph)

    def _handle_repo_not_found_exception(self, response: httpx.Response):
        """Raises a RepositoryNotFoundException if response is 404.

        Args:
            response (httpx.Response): HTTP response object.

        Raises:
            RepositoryNotFoundException: If repository is not found.
        """
        if response.status_code == httpx.codes.NOT_FOUND:
            raise RepositoryNotFoundException(
                f"Repository {self._repository_id} not found"
            )
