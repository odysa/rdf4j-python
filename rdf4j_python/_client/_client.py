from typing import Any, Dict, Optional

import httpx


class BaseClient:
    def __init__(self, base_url: str, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _build_url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"


class SyncApiClient(BaseClient):
    def __enter__(self):
        self.client = httpx.Client(timeout=self.timeout).__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.__exit__(exc_type, exc_value, traceback)

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        return self.client.get(self._build_url(path), params=params, headers=headers)

    def post(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        return self.client.post(
            self._build_url(path), data=data, json=json, headers=headers
        )

    def put(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        return self.client.put(
            self._build_url(path), data=data, json=json, headers=headers
        )

    def delete(
        self, path: str, headers: Optional[Dict[str, str]] = None
    ) -> httpx.Response:
        return self.client.delete(self._build_url(path), headers=headers)


class AsyncApiClient(BaseClient):
    async def __aenter__(self):
        self.client = await httpx.AsyncClient(timeout=self.timeout).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.client.__aexit__(exc_type, exc_value, traceback)

    async def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        return await self.client.get(
            self._build_url(path), params=params, headers=headers
        )

    async def post(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        return await self.client.post(
            self._build_url(path), data=data, json=json, headers=headers
        )

    async def put(
        self,
        path: str,
        content: Optional[bytes] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        return await self.client.put(
            self._build_url(path), content=content, json=json, headers=headers
        )

    async def delete(
        self, path: str, headers: Optional[Dict[str, str]] = None
    ) -> httpx.Response:
        return await self.client.delete(self._build_url(path), headers=headers)
