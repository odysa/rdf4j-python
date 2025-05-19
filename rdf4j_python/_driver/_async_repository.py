from typing import Iterable, Optional

import httpx
import pyoxigraph as og

from rdf4j_python._client import AsyncApiClient
from rdf4j_python._driver._async_named_graph import AsyncNamedGraph
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryInternalException,
    RepositoryNotFoundException,
    RepositoryUpdateException,
)
from rdf4j_python.model import Namespace
from rdf4j_python.model.term import (
    IRI,
    Context,
    Object,
    Predicate,
    Quad,
    QuadResultSet,
    Subject,
    Triple,
)
from rdf4j_python.utils.const import Rdf4jContentType
from rdf4j_python.utils.helpers import serialize_statements


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
        headers = {"Accept": accept}
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
        self._handle_repo_not_found_exception(response)

        query_solutions = og.parse_query_results(
            response.text, format=og.QueryResultsFormat.JSON
        )
        return [
            Namespace.from_sparql_query_solution(query_solution)
            for query_solution in query_solutions
        ]

    async def set_namespace(self, prefix: str, namespace: IRI):
        """Sets a namespace prefix.

        Args:
            prefix (str): The namespace prefix.
            namespace (IRI): The namespace URI.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            NamespaceException: If the request fails.
        """
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES}
        response = await self._client.put(
            path, content=namespace.value, headers=headers
        )
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
        headers = {"Accept": Rdf4jContentType.NTRIPLES}
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

    async def get_statements(
        self,
        subject: Optional[Subject] = None,
        predicate: Optional[Predicate] = None,
        object_: Optional[Object] = None,
        contexts: Optional[list[Context]] = None,
        infer: bool = True,
    ) -> QuadResultSet:
        """Retrieves statements matching the given pattern.

        Args:
            subject (Optional[Subject]): Filter by subject.
            predicate (Optional[Predicate]): Filter by predicate.
            object_ (Optional[Object]): Filter by object.
            contexts (Optional[list[Context]]): Filter by context (named graph).

        Returns:
            QuadResultSet: QuadResultSet of matching RDF statements.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
        """
        path = f"/repositories/{self._repository_id}/statements"
        params = {}

        if subject:
            params["subj"] = str(subject)
        if predicate:
            params["pred"] = str(predicate)
        if object_:
            params["obj"] = str(object_)
        if contexts:
            params["context"] = [str(ctx) for ctx in contexts]
        params["infer"] = str(infer).lower()

        headers = {"Accept": Rdf4jContentType.NQUADS}
        response = await self._client.get(path, params=params, headers=headers)
        return og.parse(response.content, format=og.RdfFormat.N_QUADS)

    async def delete_statements(
        self,
        subject: Optional[Subject] = None,
        predicate: Optional[Predicate] = None,
        object_: Optional[Object] = None,
        contexts: Optional[list[Context]] = None,
    ):
        """Deletes statements from the repository matching the given pattern.

        Args:
            subject (Optional[Subject]): Filter by subject (N-Triples encoded).
            predicate (Optional[Predicate]): Filter by predicate (N-Triples encoded).
            object_ (Optional[Object]): Filter by object (N-Triples encoded).
            contexts (Optional[list[Context]]): One or more specific contexts to restrict deletion to.
                Use 'null' as a string to delete context-less statements.

        Raises:
            RepositoryNotFoundException: If the repository does not exist.
            RepositoryUpdateException: If the deletion fails.
        """
        path = f"/repositories/{self._repository_id}/statements"
        params = {}

        if subject:
            params["subj"] = str(subject)
        if predicate:
            params["pred"] = str(predicate)
        if object_:
            params["obj"] = str(object_)
        if contexts:
            params["context"] = [str(ctx) for ctx in contexts]

        response = await self._client.delete(path, params=params)
        self._handle_repo_not_found_exception(response)

        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryUpdateException(
                f"Failed to delete statements: {response.text}"
            )

    async def add_statement(
        self,
        subject: Subject,
        predicate: Predicate,
        object: Object,
        context: Optional[Context] = None,
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
        statement: Triple | Quad
        if context is None:
            statement = Triple(subject, predicate, object)
        else:
            statement = Quad(subject, predicate, object, context)

        response = await self._client.post(
            path,
            content=serialize_statements([statement]),
            headers={"Content-Type": Rdf4jContentType.NQUADS},
        )
        self._handle_repo_not_found_exception(response)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryUpdateException(f"Failed to add statement: {response.text}")

    async def add_statements(self, statements: Iterable[Quad] | Iterable[Triple]):
        """Adds a list of RDF statements to the repository.

        Args:
            statements (Iterable[Quad] | Iterable[Triple]): A list of RDF statements.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If addition fails.
        """
        path = f"/repositories/{self._repository_id}/statements"
        response = await self._client.post(
            path,
            content=serialize_statements(statements),
            headers={"Content-Type": Rdf4jContentType.NQUADS},
        )
        self._handle_repo_not_found_exception(response)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryUpdateException(
                f"Failed to add statements: {response.text}"
            )

    async def replace_statements(
        self,
        statements: Iterable[Quad] | Iterable[Triple],
        contexts: Optional[Iterable[Context]] = None,
        base_uri: Optional[str] = None,
    ):
        """Replaces all repository statements with the given RDF data.

        Args:
            statements (Iterable[Quad] | Iterable[Triple]): RDF statements to load.
            contexts (Optional[Iterable[Context]]): One or more specific contexts to restrict deletion to.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If the operation fails.
        """
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.NQUADS}

        params = {}
        if contexts:
            params["context"] = [str(ctx) for ctx in contexts]
        if base_uri:
            params["baseUri"] = base_uri

        response = await self._client.put(
            path,
            content=serialize_statements(statements),
            headers=headers,
            params=params,
        )
        self._handle_repo_not_found_exception(response)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryUpdateException(
                f"Failed to replace statements: {response.text}"
            )

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

    @property
    def repository_id(self) -> str:
        return self._repository_id
