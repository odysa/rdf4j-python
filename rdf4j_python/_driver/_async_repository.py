from rdf4j_python import AsyncApiClient
from rdf4j_python.utils.const import Rdf4jContentType


class AsyncRepository:
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
        if "json" in response.headers.get("Content-Type", ""):
            return response.json()
        return response.text

    async def update(self, sparql_update: str):
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": Rdf4jContentType.SPARQL_UPDATE.value}
        response = await self._client.post(path, data=sparql_update, headers=headers)
        response.raise_for_status()

    async def replace_statements(
        self, rdf_data: str, content_type: Rdf4jContentType = Rdf4jContentType.TURTLE
    ):
        path = f"/repositories/{self._repository_id}/statements"
        headers = {"Content-Type": content_type.value}
        response = await self._client.put(path, data=rdf_data, headers=headers)
        response.raise_for_status()

    async def get_namespaces(self):
        path = f"/repositories/{self._repository_id}/namespaces"
        response = await self._client.get(path)
        if Rdf4jContentType.SPARQL_RESULTS_JSON in response.headers.get(
            "Content-Type", ""
        ):
            return response.json()
        return response.text

    async def set_namespace(self, prefix: str, namespace: str):
        path = f"/repositories/{self._repository_id}/namespaces/{prefix}"
        headers = {"Content-Type": Rdf4jContentType.NTRIPLES.value}
        response = await self._client.put(path, data=namespace, headers=headers)
        response.raise_for_status()
