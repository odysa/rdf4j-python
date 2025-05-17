import pytest

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model.repository_config import MemoryStoreConfig, RepositoryConfig


@pytest.mark.asyncio
async def test_create_repo(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        await db.create_repository(config=random_mem_repo_config)
        repos = await db.list_repositories()
        assert len(repos) == 1
        assert repos[0].id == random_mem_repo_config.repo_id
        assert repos[0].title == random_mem_repo_config.title
        await db.delete_repository(random_mem_repo_config.repo_id)


@pytest.mark.asyncio
async def test_delete_repo(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        await db.create_repository(
            config=random_mem_repo_config,
        )
        repos = await db.list_repositories()
        assert len(repos) == 1
        assert repos[0].id == random_mem_repo_config.repo_id
        assert repos[0].title == random_mem_repo_config.title
        await db.delete_repository(random_mem_repo_config.repo_id)
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
            title = f"test_list_repos_{repo}_title"
            repo_config = (
                RepositoryConfig.Builder()
                .repo_id(repo_id)
                .title(title)
                .sail_repository_impl(
                    MemoryStoreConfig.Builder().persist(False).build()
                )
                .build()
            )
            await db.create_repository(
                config=repo_config,
            )
        repo_list = await db.list_repositories()
        assert len(repo_list) == repo_count
        for repo in range(repo_count):
            assert f"test_list_repos_{repo}" in [repo.id for repo in repo_list]
            assert f"test_list_repos_{repo}_title" in [repo.title for repo in repo_list]
            await db.delete_repository(f"test_list_repos_{repo}")


@pytest.mark.asyncio
async def test_create_memory_store_repo(
    rdf4j_service: str, random_mem_repo_config: RepositoryConfig
):
    async with AsyncRdf4j(rdf4j_service) as db:
        await db.create_repository(
            config=random_mem_repo_config,
        )
        repo_list = await db.list_repositories()
        assert len(repo_list) == 1
        assert repo_list[0].id == random_mem_repo_config.repo_id
        assert repo_list[0].title == random_mem_repo_config.title
        await db.delete_repository(random_mem_repo_config.repo_id)
