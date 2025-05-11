import httpx
import rdflib

from rdf4j_python import AsyncApiClient
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryInternalException,
    RepositoryNotFoundException,
)
from rdf4j_python.model._namespace import Namespace
from rdf4j_python.utils.const import Rdf4jContentType


class AsyncRdf4JRepository:
    def __init__(self, client: AsyncApiClient, repository_id: str):
        self._client = client
        self._repository_id = repository_id

    async def query(
        self,
        sparql_query: str,
        infer: bool = True,
        accept: Rdf4jContentType = Rdf4jContentType.SPARQL_RESULTS_JSON,
    ):
        path = f"/repositories/{self._repository_id}"
        params = {"query": sparql_query, "infer": str(infer).lower()}
        headers = {"Accept": accept.value}
        response = await self._client.get(path, params=params, headers=headers)
        self._handle_repo_not_found_exception(response)
        if "json" in response.headers.get("Content-Type", ""):
            return response.json()
        return response.text

    async def update(self, sparql_update: str):
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.SPARQL_UPDATE.value}
        response = await self._client.post(path, data=sparql_update, headers=headers)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def replace_statements(
        self, rdf_data: str, content_type: Rdf4jContentType = Rdf4jContentType.TURTLE
    ):
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": content_type.value}
        response = await self._client.put(path, data=rdf_data, headers=headers)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def get_namespaces(self):
        path = f"/repositories/{self._repository_id}/namespaces"
        headers = {"Accept": Rdf4jContentType.SPARQL_RESULTS_JSON}
        response = await self._client.get(path, headers=headers)

        result = rdflib.query.Result.parse(
            response, format=Rdf4jContentType.SPARQL_RESULTS_JSON
        )
        self._handle_repo_not_found_exception(response)
        return [Namespace.from_rdflib_binding(binding) for binding in result.bindings]

    async def set_namespace(self, prefix: str, namespace: str):
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.put(path, content=namespace, headers=headers)
        self._handle_repo_not_found_exception(response)
        if response.status_code != httpx.codes.NO_CONTENT:
            raise NamespaceException(f"Failed to set namespace: {response.text}")

    async def get_namespace(self, prefix: str) -> Namespace:
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        headers = {"Accept": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.get(path, headers=headers)
        self._handle_repo_not_found_exception(response)

        if response.status_code != httpx.codes.OK:
            raise NamespaceException(f"Failed to get namespace: {response.text}")

        return Namespace(prefix, response.text)

    async def delete_namespace(self, prefix: str):
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        response = await self._client.delete(path)
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    async def size(self) -> int:
        path = f"/repositories/{self._repository_id}/size"
        response = await self._client.get(path)
        self._handle_repo_not_found_exception(response)

        if response.status_code != httpx.codes.OK:
            raise RepositoryInternalException(f"Failed to get size: {response.text}")

        return int(response.text.strip())

    async def add_statement(self, subject: str, predicate: str, object: str):
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.post(
            path, data=f"{subject} {predicate} {object}.", headers=headers
        )
        self._handle_repo_not_found_exception(response)
        response.raise_for_status()

    def _handle_repo_not_found_exception(self, response: httpx.Response):
        if response.status_code == httpx.codes.NOT_FOUND:
            raise RepositoryNotFoundException(
                f"Repository {self._repository_id} not found"
            )
