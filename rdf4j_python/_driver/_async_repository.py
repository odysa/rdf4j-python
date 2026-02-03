import re
from pathlib import Path
from typing import Iterable, Optional, Union

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

try:
    from SPARQLWrapper import SPARQLWrapper  # type: ignore[import-untyped]

    _has_sparql_wrapper = True
except ImportError:
    _has_sparql_wrapper = False


# Pattern to match PREFIX declarations (handles URIs with # fragments)
_PREFIX_PATTERN = re.compile(r"PREFIX\s+\w*:\s*<[^>]*>", re.IGNORECASE)
# Pattern to match BASE declarations
_BASE_PATTERN = re.compile(r"BASE\s*<[^>]*>", re.IGNORECASE)


def _remove_sparql_comments(query: str) -> str:
    """Removes SPARQL comments while preserving # inside URIs and strings.

    Args:
        query (str): The SPARQL query string.

    Returns:
        str: The query with comments removed.
    """
    result = []
    i = 0
    in_uri = False
    in_string = False
    string_char = None

    while i < len(query):
        char = query[i]

        if in_string:
            result.append(char)
            if char == string_char and (i == 0 or query[i - 1] != "\\"):
                in_string = False
            i += 1
        elif in_uri:
            result.append(char)
            if char == ">":
                in_uri = False
            i += 1
        elif char == "<":
            in_uri = True
            result.append(char)
            i += 1
        elif char in ('"', "'"):
            in_string = True
            string_char = char
            result.append(char)
            i += 1
        elif char == "#":
            # Skip until end of line
            while i < len(query) and query[i] != "\n":
                i += 1
        else:
            result.append(char)
            i += 1

    return "".join(result)


def _detect_query_type(query: str) -> str:
    """Detects the SPARQL query type, ignoring prefixes, base, and comments.

    Args:
        query (str): The SPARQL query string.

    Returns:
        str: The query type in uppercase (SELECT, ASK, CONSTRUCT, DESCRIBE, INSERT, DELETE, etc.)
             or empty string if unable to determine.
    """
    # Remove comments (preserving # inside URIs)
    cleaned = _remove_sparql_comments(query)
    # Remove all PREFIX declarations
    cleaned = _PREFIX_PATTERN.sub("", cleaned)
    # Remove all BASE declarations
    cleaned = _BASE_PATTERN.sub("", cleaned)
    # Get the first word
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    first_word = cleaned.split()[0].upper()
    return first_word


class AsyncRdf4JRepository:
    """Asynchronous interface for interacting with an RDF4J repository."""

    _client: AsyncApiClient
    _repository_id: str
    _sparql_wrapper: Optional["SPARQLWrapper"] = None

    def __init__(self, client: AsyncApiClient, repository_id: str):
        """Initializes the repository interface.

        Args:
            client (AsyncApiClient): The RDF4J API client.
            repository_id (str): The ID of the RDF4J repository.
        """
        self._client = client
        self._repository_id = repository_id

    async def get_sparql_wrapper(self) -> "SPARQLWrapper":
        """Returns the SPARQLWrapper for the repository.

        Returns:
            SPARQLWrapper: The SPARQLWrapper for the repository.
        """
        if not _has_sparql_wrapper:
            raise ImportError(
                "SPARQLWrapper is not installed. Please install it with `pip install rdf4j-python[sparqlwrapper]`"
            )

        if self._sparql_wrapper is None:
            self._sparql_wrapper = SPARQLWrapper(
                f"{self._client.get_base_url()}/repositories/{self._repository_id}"
            )
        return self._sparql_wrapper

    async def query(
        self,
        sparql_query: str,
        infer: bool = True,
    ) -> og.QuerySolutions | og.QueryBoolean | og.QueryTriples:
        """Executes a SPARQL query (SELECT, ASK, CONSTRUCT, or DESCRIBE).

        Args:
            sparql_query (str): The SPARQL query string.
            infer (bool): Whether to include inferred statements. Defaults to True.

        Returns:
            og.QuerySolutions | og.QueryBoolean | og.QueryTriples: Parsed query results.

        Note:
            This method correctly handles queries with PREFIX declarations,
            BASE URIs, and comments before the query keyword.
        """
        path = f"/repositories/{self._repository_id}"
        params = {"query": sparql_query, "infer": str(infer).lower()}

        # Detect query type (handles PREFIX, BASE, comments)
        query_type = _detect_query_type(sparql_query)

        if query_type == "SELECT":
            headers = {"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON}
            response = await self._client.get(path, params=params, headers=headers)
            self._handle_repo_not_found_exception(response)
            return og.parse_query_results(
                response.text, format=og.QueryResultsFormat.JSON
            )
        elif query_type == "ASK":
            headers = {"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON}
            response = await self._client.get(path, params=params, headers=headers)
            self._handle_repo_not_found_exception(response)
            return og.parse_query_results(
                response.text, format=og.QueryResultsFormat.JSON
            )
        elif query_type in ("CONSTRUCT", "DESCRIBE"):
            headers = {"Accept": Rdf4jContentType.NTRIPLES}
            response = await self._client.get(path, params=params, headers=headers)
            self._handle_repo_not_found_exception(response)
            # Create temporary store to convert N-Triples response to QueryTriples
            store = og.Store()
            for quad in og.parse(response.text, format=og.RdfFormat.N_TRIPLES):
                store.add(quad)
            return store.query("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
        else:
            # Default to JSON for unknown query types
            headers = {"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON}
            response = await self._client.get(path, params=params, headers=headers)
            self._handle_repo_not_found_exception(response)
            return og.parse_query_results(
                response.text, format=og.QueryResultsFormat.JSON
            )

    async def update(
        self, sparql_update_query: str, content_type: Rdf4jContentType
    ) -> None:
        """Executes a SPARQL UPDATE command.

        Args:
            sparql_update (str): The SPARQL update string.

        Raises:
            RepositoryNotFoundException: If the repository doesn't exist.
            httpx.HTTPStatusError: If the update fails.
        """
        # SPARQL UPDATE operations return HTTP 204 No Content on success.
        # No result data is returned as per SPARQL 1.1 UPDATE specification.
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": content_type}
        response = await self._client.post(
            path, content=sparql_update_query, headers=headers
        )
        self._handle_repo_not_found_exception(response)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise RepositoryUpdateException(f"Failed to update: {response.text}")

    async def get_namespaces(self) -> list[Namespace]:
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
        if not isinstance(query_solutions, og.QuerySolutions):
            raise TypeError(
                f"Expected QuerySolutions, got {type(query_solutions).__name__}"
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

    async def delete_namespace(self, prefix: str) -> None:
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

    async def clear_all_namespaces(self) -> None:
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
    ) -> None:
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
    ) -> None:
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

    async def add_statements(self, statements: Iterable[Quad] | Iterable[Triple]) -> None:
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
    ) -> None:
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

    async def upload_file(
        self,
        file_path: Union[str, Path],
        rdf_format: Optional[og.RdfFormat] = None,
        context: Optional[Context] = None,
        base_uri: Optional[str] = None,
    ) -> None:
        """Uploads an RDF file to the repository.

        This method reads an RDF file from disk and uploads its contents to the repository.
        The file can be in various RDF formats such as Turtle, N-Triples, N-Quads, RDF/XML, JSON-LD, TriG, or N3.

        Args:
            file_path (Union[str, Path]): Path to the RDF file to upload.
            rdf_format (Optional[og.RdfFormat]): The RDF format of the file.
                If None, the format is automatically detected from the file extension.
                Supported formats include:
                - og.RdfFormat.TURTLE (.ttl)
                - og.RdfFormat.N_TRIPLES (.nt)
                - og.RdfFormat.N_QUADS (.nq)
                - og.RdfFormat.RDF_XML (.rdf, .xml)
                - og.RdfFormat.JSON_LD (.jsonld)
                - og.RdfFormat.TRIG (.trig)
                - og.RdfFormat.N3 (.n3)
            context (Optional[Context]): The named graph (context) to upload statements into.
                If None, statements are added to the default graph.
            base_uri (Optional[str]): The base URI for resolving relative URIs in the file.
                If None, relative URIs are resolved based on the file path.

        Raises:
            FileNotFoundError: If the specified file doesn't exist.
            RepositoryNotFoundException: If the repository doesn't exist.
            RepositoryUpdateException: If the upload fails.
            ValueError: If the RDF format is not supported.
            SyntaxError: If the file contains invalid RDF data.

        Example:
            >>> repo = await db.get_repository("my-repo")
            >>> # Upload a Turtle file (format auto-detected)
            >>> await repo.upload_file("data.ttl")
            >>> # Upload to a specific named graph
            >>> await repo.upload_file("data.ttl", context=IRI("http://example.com/graph"))
            >>> # Upload with explicit format
            >>> await repo.upload_file("data.txt", rdf_format=og.RdfFormat.N_TRIPLES)
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Parse the RDF file using pyoxigraph
        try:
            # If base_uri is not provided, use the file path as base
            if base_uri is None:
                base_uri = file_path.as_uri()

            # Parse the file
            quads = list(
                og.parse(path=str(file_path), format=rdf_format, base_iri=base_uri)
            )

            # If a context is specified, wrap all statements in that context
            # Note: This overrides any named graph information in the file (e.g., from N-Quads)
            if context is not None:
                statements = [
                    Quad(q.subject, q.predicate, q.object, context) for q in quads
                ]
            else:
                statements = quads

            # Upload the statements to the repository
            await self.add_statements(statements)

        except (ValueError, SyntaxError) as e:
            raise type(e)(f"Failed to parse RDF file '{file_path}': {e}") from e
        except Exception as e:
            raise RepositoryUpdateException(
                f"Failed to upload file '{file_path}': {e}"
            ) from e

    async def get_named_graph(self, graph: str) -> AsyncNamedGraph:
        """Retrieves a named graph in the repository.

        Returns:
            AsyncNamedGraph: A named graph object.
        """
        return AsyncNamedGraph(self._client, self._repository_id, graph)

    def _handle_repo_not_found_exception(self, response: httpx.Response) -> None:
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
