"""Test client initialization behavior."""

import pytest
import httpx
from rdf4j_python._client import AsyncApiClient, SyncApiClient


class TestSyncApiClientInitialization:
    """Test SyncApiClient initialization behavior."""

    def test_client_attribute_available_after_init(self):
        """Test that httpx.Client is available after __init__."""
        client = SyncApiClient("http://localhost:8080")

        # The client attribute should be available after initialization
        assert hasattr(client, "client"), (
            "client attribute should be available after __init__"
        )
        assert client.client is not None, "client should not be None after __init__"

        # The client should be an httpx.Client instance
        assert isinstance(client.client, httpx.Client), (
            "client should be httpx.Client instance"
        )

    def test_context_management_still_works(self):
        """Test that context management still works properly."""
        with SyncApiClient("http://localhost:8080") as client:
            # Client should still be available in context
            assert hasattr(client, "client")
            assert client.client is not None

            # Should be the same client instance
            assert isinstance(client.client, httpx.Client)

    def test_client_cleanup_on_exit(self):
        """Test that client is properly cleaned up on context exit."""
        client = SyncApiClient("http://localhost:8080")

        # Enter and exit context
        client.__enter__()
        client.__exit__(None, None, None)

        # Client should still exist but should have been properly closed
        assert hasattr(client, "client")
        assert client.client is not None


class TestAsyncApiClientInitialization:
    """Test AsyncApiClient initialization behavior."""

    def test_client_attribute_available_after_init(self):
        """Test that httpx.AsyncClient is available after __init__."""
        client = AsyncApiClient("http://localhost:8080")

        # The client attribute should be available after initialization
        assert hasattr(client, "client"), (
            "client attribute should be available after __init__"
        )
        assert client.client is not None, "client should not be None after __init__"

        # The client should be an httpx.AsyncClient instance
        assert isinstance(client.client, httpx.AsyncClient), (
            "client should be httpx.AsyncClient instance"
        )

    @pytest.mark.asyncio
    async def test_context_management_still_works(self):
        """Test that async context management still works properly."""
        async with AsyncApiClient("http://localhost:8080") as client:
            # Client should still be available in context
            assert hasattr(client, "client")
            assert client.client is not None

            # Should be the same client instance
            assert isinstance(client.client, httpx.AsyncClient)

    @pytest.mark.asyncio
    async def test_client_cleanup_on_exit(self):
        """Test that client is properly cleaned up on context exit."""
        client = AsyncApiClient("http://localhost:8080")

        # Enter and exit context
        await client.__aenter__()
        await client.__aexit__(None, None, None)

        # Client should still exist but should have been properly closed
        assert hasattr(client, "client")
        assert client.client is not None
