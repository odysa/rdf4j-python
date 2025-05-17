import logging
from random import randint

import httpx
import pytest
import pytest_asyncio

from rdf4j_python._driver._async_rdf4j_db import AsyncRdf4j
from rdf4j_python.model._repository_config import (
    MemoryStoreConfig,
    RepositoryConfig,
)

LOGGER = logging.getLogger(__name__)


def is_responsive(url: str) -> bool:
    try:
        LOGGER.info(f"awaiting for docker {url} to be responsive....")
        response = httpx.get(url + "/protocol", timeout=2.0)
        if response.status_code == httpx.codes.OK:
            LOGGER.info(f"docker {url} is responsive")
            return True
    except Exception:
        return False
    return False


@pytest.fixture(scope="session")
def rdf4j_service(docker_ip: str, docker_services) -> str:
    port = docker_services.port_for("rdf4j", 8080)
    url = f"http://{docker_ip}:{port}/rdf4j-server"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(url)
    )
    return url


@pytest_asyncio.fixture(scope="function")
async def mem_repo(rdf4j_service: str, random_mem_repo_config: RepositoryConfig):
    """Fixture that yields a ready-to-use memory repository instance."""

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(
            config=random_mem_repo_config,
        )
        yield repo
        await db.delete_repository(random_mem_repo_config.repo_id)


@pytest.fixture(scope="function")
def random_mem_repo_config() -> RepositoryConfig:
    """Fixture that yields a random memory repository configuration."""
    repo_id = f"test_repo_{str(randint(1, 1000000))}"
    title = f"test_repo_{str(randint(1, 1000000))}_title"
    return (
        RepositoryConfig.Builder()
        .repo_id(repo_id)
        .title(title)
        .sail_repository_impl(
            MemoryStoreConfig.Builder().persist(False).build(),
        )
        .build()
    )
