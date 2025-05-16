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
from rdf4j_python.utils.const import Rdf4jContentType

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
            repository_id=random_mem_repo_config.repo_id,
            rdf_config_data=random_mem_repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )
        yield repo
        await db.delete_repository(random_mem_repo_config.repo_id)


@pytest.fixture(scope="function")
def random_mem_repo_config() -> RepositoryConfig:
    """Fixture that yields a random memory repository configuration."""
    repo_id = f"test_repo_{str(randint(1, 1000000))}"
    return (
        RepositoryConfig.builder_with_sail_repository(
            MemoryStoreConfig.Builder()
            .persist(False)
            .iteration_cache_sync_threshold(1000)
            .build(),
        )
        .repo_id(repo_id)
        .title(repo_id)
        .build()
    )
