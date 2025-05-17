from typing import Any, Dict, Optional

import httpx


class BaseClient:
    """Base HTTP client that provides shared URL building functionality."""

    def __init__(self, base_url: str, timeout: int = 10):
        """
        Initializes a BaseClient.

        Args:
            base_url (str): The base URL for the API endpoints.
            timeout (int, optional): Request timeout in seconds. Defaults to 10.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _build_url(self, path: str) -> str:
        """
        Builds a full URL by combining the base URL and the given path.

        Args:
            path (str): The path to append to the base URL.

        Returns:
            str: The full URL.
        """
        return f"{self.base_url}/{path.lstrip('/')}"

    def get_base_url(self) -> str:
        """
        Returns the base URL.

        Returns:
            str: The base URL.
        """
        return self.base_url


class SyncApiClient(BaseClient):
    """Synchronous API client using httpx.Client."""

    def __enter__(self):
        """
        Enters the context and initializes the HTTP client.

        Returns:
            SyncApiClient: The instance of the client.
        """
        self.client = httpx.Client(timeout=self.timeout).__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exits the context and closes the HTTP client.
        """
        self.client.__exit__(exc_type, exc_value, traceback)

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Sends a GET request.

        Args:
            path (str): API endpoint path.
            params (Optional[Dict[str, Any]]): Query parameters.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return self.client.get(self._build_url(path), params=params, headers=headers)

    def post(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Sends a POST request.

        Args:
            path (str): API endpoint path.
            data (Optional[Dict[str, Any]]): Form-encoded body data.
            json (Optional[Any]): JSON-encoded body data.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
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
        """
        Sends a PUT request.

        Args:
            path (str): API endpoint path.
            data (Optional[Dict[str, Any]]): Form-encoded body data.
            json (Optional[Any]): JSON-encoded body data.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return self.client.put(
            self._build_url(path), data=data, json=json, headers=headers
        )

    def delete(
        self, path: str, headers: Optional[Dict[str, str]] = None
    ) -> httpx.Response:
        """
        Sends a DELETE request.

        Args:
            path (str): API endpoint path.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return self.client.delete(self._build_url(path), headers=headers)


class AsyncApiClient(BaseClient):
    """Asynchronous API client using httpx.AsyncClient."""

    async def __aenter__(self):
        """
        Enters the async context and initializes the HTTP client.

        Returns:
            AsyncApiClient: The instance of the client.
        """
        self.client = await httpx.AsyncClient(timeout=self.timeout).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Exits the async context and closes the HTTP client.
        """
        await self.client.__aexit__(exc_type, exc_value, traceback)

    async def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous GET request.

        Args:
            path (str): API endpoint path.
            params (Optional[Dict[str, Any]]): Query parameters.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return await self.client.get(
            self._build_url(path), params=params, headers=headers
        )

    async def post(
        self,
        path: str,
        content: Optional[str] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous POST request.

        Args:
            path (str): API endpoint path.
            content (Optional[str]): Raw string content to include in the request body.
            json (Optional[Any]): JSON-encoded body data.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return await self.client.post(
            self._build_url(path), content=content, json=json, headers=headers
        )

    async def put(
        self,
        path: str,
        content: Optional[bytes] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous PUT request.

        Args:
            path (str): API endpoint path.
            content (Optional[bytes]): Raw bytes to include in the request body.
            params (Optional[Dict[str, Any]]): Query parameters.
            json (Optional[Any]): JSON-encoded body data.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return await self.client.put(
            self._build_url(path),
            content=content,
            json=json,
            headers=headers,
            params=params,
        )

    async def delete(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous DELETE request.

        Args:
            path (str): API endpoint path.
            params (Optional[Dict[str, Any]]): Query parameters.
            headers (Optional[Dict[str, str]]): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        return await self.client.delete(
            self._build_url(path), params=params, headers=headers
        )
