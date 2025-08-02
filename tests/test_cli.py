"""Tests for the CLI module."""

import pytest
from unittest.mock import AsyncMock, patch

from rdf4j_python.cli import RDF4JCLIContext, list_repositories
from rdf4j_python.model._repository_info import RepositoryMetadata


@pytest.mark.asyncio
async def test_list_repositories():
    """Test the list_repositories function."""
    ctx = RDF4JCLIContext()
    ctx.server_url = "http://test-server:7200/rdf4j-server"
    ctx.connected = True
    
    # Mock repository data
    mock_repos = [
        RepositoryMetadata(
            id="test-repo",
            uri="http://test-server:7200/rdf4j-server/repositories/test-repo",
            title="Test Repository",
            readable=True,
            writable=True
        )
    ]
    
    # Mock the AsyncRdf4j client
    with patch('rdf4j_python.cli.AsyncRdf4j') as mock_rdf4j:
        mock_client = AsyncMock()
        mock_client.list_repositories.return_value = mock_repos
        mock_rdf4j.return_value.__aenter__.return_value = mock_client
        
        # This should not raise an exception
        await list_repositories(ctx)


def test_cli_context():
    """Test the CLI context object."""
    ctx = RDF4JCLIContext()
    assert ctx.rdf4j_client is None
    assert ctx.server_url is None
    assert ctx.connected is False
    
    ctx.server_url = "http://test:7200/rdf4j-server"
    ctx.connected = True
    
    assert ctx.server_url == "http://test:7200/rdf4j-server"
    assert ctx.connected is True