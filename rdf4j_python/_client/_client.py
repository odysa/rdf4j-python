import logging
from types import TracebackType
from typing import Any, Self

import httpx

logger = logging.getLogger("rdf4j_python")


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

    def __init__(self, base_url: str, timeout: int = 10, retries: int = 3) -> None:
        """
        Initializes the SyncApiClient.

        Args:
            base_url (str): The base URL for the API endpoints.
            timeout (int, optional): Request timeout in seconds. Defaults to 10.
            retries (int, optional): Number of retries for failed requests. Defaults to 3.
        """
        super().__init__(base_url, timeout)
        transport = httpx.HTTPTransport(retries=retries)
        self.client = httpx.Client(timeout=self.timeout, transport=transport)

    def __enter__(self) -> Self:
        """
        Enters the context and initializes the HTTP client.

        Returns:
            SyncApiClient: The instance of the client.
        """
        self.client.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Exits the context and closes the HTTP client.
        """
        self.client.__exit__(exc_type, exc_value, traceback)

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """
        Sends a GET request.

        Args:
            path (str): API endpoint path.
            params (dict[str, Any] | None): Query parameters.
            headers (dict[str, str] | None): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("GET %s params=%s", url, params)
        response = self.client.get(url, params=params, headers=headers)
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    def post(
        self,
        path: str,
        content: str | bytes | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """
        Sends a POST request.

        Args:
            path (str): API endpoint path.
            content (str | bytes | None): Raw content to include in the request body.
            json (Any | None): JSON-encoded body data.
            headers (dict[str, str] | None): Request headers.
            params (dict[str, Any] | None): Query parameters.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("POST %s params=%s", url, params)
        response = self.client.post(
            url, content=content, json=json, headers=headers, params=params
        )
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    def put(
        self,
        path: str,
        content: str | bytes | None = None,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """
        Sends a PUT request.

        Args:
            path (str): API endpoint path.
            content (str | bytes | None): Raw content to include in the request body.
            params (dict[str, Any] | None): Query parameters.
            json (Any | None): JSON-encoded body data.
            headers (dict[str, str] | None): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("PUT %s params=%s", url, params)
        response = self.client.put(
            url,
            content=content,
            json=json,
            headers=headers,
            params=params,
        )
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """
        Sends a DELETE request.

        Args:
            path (str): API endpoint path.
            params (dict[str, Any] | None): Query parameters.
            headers (dict[str, str] | None): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("DELETE %s params=%s", url, params)
        response = self.client.delete(url, params=params, headers=headers)
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response


class AsyncApiClient(BaseClient):
    """Asynchronous API client using httpx.AsyncClient."""

    def __init__(self, base_url: str, timeout: int = 10, retries: int = 3) -> None:
        """
        Initializes the AsyncApiClient.

        Args:
            base_url (str): The base URL for the API endpoints.
            timeout (int, optional): Request timeout in seconds. Defaults to 10.
            retries (int, optional): Number of retries for failed requests. Defaults to 3.
        """
        super().__init__(base_url, timeout)
        transport = httpx.AsyncHTTPTransport(retries=retries)
        self.client = httpx.AsyncClient(timeout=self.timeout, transport=transport)

    async def __aenter__(self) -> Self:
        """
        Enters the async context and initializes the HTTP client.

        Returns:
            AsyncApiClient: The instance of the client.
        """
        await self.client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Exits the async context and closes the HTTP client.
        """
        await self.client.__aexit__(exc_type, exc_value, traceback)

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous GET request.

        Args:
            path (str): API endpoint path.
            params (dict[str, Any] | None): Query parameters.
            headers (dict[str, str] | None): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("GET %s params=%s", url, params)
        response = await self.client.get(url, params=params, headers=headers)
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    async def post(
        self,
        path: str,
        content: str | bytes | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous POST request.

        Args:
            path (str): API endpoint path.
            content (str | bytes | None): Raw content to include in the request body.
            json (Any | None): JSON-encoded body data.
            headers (dict[str, str] | None): Request headers.
            params (dict[str, Any] | None): Query parameters.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("POST %s params=%s", url, params)
        response = await self.client.post(
            url, content=content, json=json, headers=headers, params=params
        )
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    async def put(
        self,
        path: str,
        content: str | bytes | None = None,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous PUT request.

        Args:
            path (str): API endpoint path.
            content (str | bytes | None): Raw content to include in the request body.
            params (dict[str, Any] | None): Query parameters.
            json (Any | None): JSON-encoded body data.
            headers (dict[str, str] | None): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("PUT %s params=%s", url, params)
        response = await self.client.put(
            url,
            content=content,
            json=json,
            headers=headers,
            params=params,
        )
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    async def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """
        Sends an asynchronous DELETE request.

        Args:
            path (str): API endpoint path.
            params (dict[str, Any] | None): Query parameters.
            headers (dict[str, str] | None): Request headers.

        Returns:
            httpx.Response: The HTTP response.
        """
        url = self._build_url(path)
        logger.debug("DELETE %s params=%s", url, params)
        response = await self.client.delete(url, params=params, headers=headers)
        logger.debug("Response %s: %s bytes", response.status_code, len(response.content))
        return response

    async def aclose(self) -> None:
        """
        Asynchronously closes the client connection.
        """
        logger.debug("Closing async client connection")
        await self.client.aclose()
