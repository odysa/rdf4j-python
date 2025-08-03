import json
import logging
from random import randint
from typing import Any, Iterable, cast

import httpx
import pyoxigraph as og
import pytest
import pytest_asyncio

from rdf4j_python._driver._async_rdf4j_db import AsyncRdf4j
from rdf4j_python.model.repository_config import (
    MemoryStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
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
    return RepositoryConfig(
        repo_id=repo_id,
        title=title,
        impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False))
    )


@pytest.fixture
def valid_repo_list_result() -> og.QuerySolutions:
    return cast(
        og.QuerySolutions,
        og.parse_query_results(
            SparqlJsonResultBuilder()
            .add_variables(["id", "title", "uri", "readable", "writable"])
            .add_bindings(
                [
                    {
                        "id": {"type": "literal", "value": "example-repo1"},
                        "title": {"type": "literal", "value": "example-repo1"},
                        "uri": {
                            "type": "uri",
                            "value": "http://localhost:8080/rdf4j-server/repositories/example-repo1",
                        },
                        "readable": {"type": "literal", "value": "true"},
                        "writable": {"type": "literal", "value": "false"},
                    },
                    {
                        "id": {"type": "literal", "value": "example-repo2"},
                        "title": {"type": "literal", "value": "example-repo2"},
                        "uri": {
                            "type": "uri",
                            "value": "http://localhost:8080/rdf4j-server/repositories/example-repo2",
                        },
                        "readable": {"type": "literal", "value": "true"},
                        "writable": {"type": "literal", "value": "false"},
                    },
                ]
            )
            .to_json(),
            format=og.QueryResultsFormat.JSON,
        ),
    )


@pytest.fixture
def partial_result():
    return og.parse_query_results(
        SparqlJsonResultBuilder()
        .add_variables(["id", "title"])
        .add_bindings([{"id": {"type": "literal", "value": "partial-repo"}}])
        .to_json(),
        format=og.QueryResultsFormat.JSON,
    )


class SparqlJsonResultBuilder:
    def __init__(self):
        self._variables = []
        self._bindings = []

    def add_variable(self, name: str):
        if name not in self._variables:
            self._variables.append(name)
        return self

    def add_variables(self, names: Iterable[str]):
        for name in names:
            self.add_variable(name)
        return self

    def add_bindings(self, bindings: list[dict[str, Any]]):
        """
        Add one binding (a result row). The value must be a SPARQL result-compliant dict:
        Example:
            {
                "uri": {
                    "type": "uri",
                    "value": "http://localhost:8080/rdf4j-server/repositories/mem-rdf"
                },
                "id": {
                  "type": "literal",
                  "value": "mem-rdf"
                },
                "writable": {
                  "type": "literal",
                  "datatype": "http://www.w3.org/2001/XMLSchema#boolean",
                  "value": "false"
                }
            }
        """
        self._bindings.extend(bindings)
        return self

    def _build(self) -> dict:
        return {
            "head": {"vars": self._variables},
            "results": {"bindings": self._bindings},
        }

    def to_json(self, **kwargs) -> str:
        return json.dumps(self._build(), **kwargs)
