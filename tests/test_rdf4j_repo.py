import pytest

from rdf4j_python import AsyncRdf4j
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryNotFoundException,
)
from rdf4j_python.model import IRI, RepositoryConfig
from rdf4j_python.utils.const import Rdf4jContentType


@pytest.mark.asyncio
async def test_repo_size(rdf4j_service: str, random_mem_repo_config: RepositoryConfig):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
        size = await repo.size()
        assert size == 0


@pytest.mark.asyncio
async def test_repo_size_not_found(rdf4j_service: str):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.size()


@pytest.mark.asyncio
async def test_repo_set_namespace(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
        await repo.set_namespace("ex", "http://example.org/")


@pytest.mark.asyncio
async def test_repo_set_namespace_not_found(rdf4j_service: str):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(NamespaceException):
            await repo.set_namespace("ex", "http://example.org/")


@pytest.mark.asyncio
async def test_repo_get_namespaces(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
        await repo.set_namespace("ex", "http://example.org/")
        await repo.set_namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        namespaces = await repo.get_namespaces()
        assert len(namespaces) == 2
        assert namespaces[0].prefix == "ex"
        assert namespaces[0].namespace == IRI("http://example.org/")
        assert namespaces[1].prefix == "rdf"
        assert namespaces[1].namespace == IRI(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        )


@pytest.mark.asyncio
async def test_repo_get_namespace_not_found(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.get_namespace("ex")


@pytest.mark.asyncio
async def test_repo_get_namespace(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
        await repo.set_namespace("ex", "http://example.org/")
        namespace = await repo.get_namespace("ex")
        assert namespace.prefix == "ex"
        assert namespace.namespace == IRI("http://example.org/")


@pytest.mark.asyncio
async def test_repo_delete_namespace_not_found(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.delete_namespace("ex")


@pytest.mark.asyncio
async def test_repo_delete_namespace(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
        await repo.set_namespace("ex", "http://example.org/")
        await repo.delete_namespace("ex")
        namespaces = await repo.get_namespaces()
        assert len(namespaces) == 0
