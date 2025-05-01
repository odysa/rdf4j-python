import httpx
from typing import Optional, Any, Dict


class BaseClient:
    def __init__(
        self, base_url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 10
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout

    def _build_url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"


class SyncApiClient(BaseClient):
    def __enter__(self):
        self.client = httpx.Client(timeout=self.timeout, headers=self.headers).__enter__
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.__exit__(exc_type, exc_value, traceback)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        return self.client.get(self._build_url(path), params=params)

    def post(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
    ) -> httpx.Response:
        return self.client.post(self._build_url(path), data=data, json=json)

    def put(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
    ) -> httpx.Response:
        return self.client.put(self._build_url(path), data=data, json=json)

    def delete(self, path: str) -> httpx.Response:
        return self.client.delete(self._build_url(path))


class AsyncApiClient(BaseClient):
    async def __aenter__(self):
        self.client = await httpx.AsyncClient(
            timeout=self.timeout, headers=self.headers
        ).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.client.__aexit__(exc_type, exc_value, traceback)

    async def get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        return await self.client.get(self._build_url(path), params=params)

    async def post(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
    ) -> httpx.Response:
        return await self.client.post(self._build_url(path), data=data, json=json)

    async def put(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
    ) -> httpx.Response:
        return await self.client.put(self._build_url(path), data=data, json=json)

    async def delete(self, path: str) -> httpx.Response:
        return await self.client.delete(self._build_url(path))
