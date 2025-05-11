import pytest

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model import RepositoryConfig
from rdf4j_python.utils.const import Rdf4jContentType


def get_repo_config(name: str):
    return f"""
        @prefix config: <tag:rdf4j.org,2023:config/>.

        [] a config:Repository ;
        config:rep.id "{name}" ;
        rdfs:label "{name}" ;
        config:rep.impl [
            config:rep.type "openrdf:SailRepository" ;
            config:sail.impl [
                config:sail.type "openrdf:MemoryStore" ;
            ]
        ] .
    """


@pytest.mark.asyncio
async def test_create_repo(rdf4j_service: str):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo_id = "test_create_repo"
        await db.create_repository(
            repository_id=repo_id,
            rdf_config_data=get_repo_config(repo_id),
            content_type=Rdf4jContentType.TURTLE,
        )
        repos = await db.list_repositories()
        assert len(repos) == 1
        assert repos[0].id == repo_id
        assert repos[0].title == repo_id
        await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_delete_repo(rdf4j_service: str):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo_id = "test_delete_repo"
        await db.create_repository(
            repository_id=repo_id,
            rdf_config_data=get_repo_config(repo_id),
            content_type=Rdf4jContentType.TURTLE,
        )
        repos = await db.list_repositories()
        assert len(repos) == 1
        assert repos[0].id == repo_id
        assert repos[0].title == repo_id
        await db.delete_repository(repo_id)
        repos = await db.list_repositories()
        assert len(repos) == 0


@pytest.mark.asyncio
async def test_list_repos(rdf4j_service: str):
    async with AsyncRdf4j(rdf4j_service) as db:
        repo_count = 10
        repos = await db.list_repositories()
        assert len(repos) == 0
        for repo in range(repo_count):
            repo_id = f"test_list_repos_{repo}"
            await db.create_repository(
                repository_id=repo_id,
                rdf_config_data=get_repo_config(repo_id),
                content_type=Rdf4jContentType.TURTLE,
            )
        repos = await db.list_repositories()
        assert len(repos) == repo_count
        for repo in range(repo_count):
            repo_id = f"test_list_repos_{repo}"
            assert repo_id in [repo.id for repo in repos]
            assert repo_id in [repo.title for repo in repos]


@pytest.mark.asyncio
async def test_create_memory_store_repo(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        await db.create_repository(
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
