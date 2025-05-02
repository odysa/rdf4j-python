import logging

import httpx
import pytest

LOGGER = logging.getLogger(__name__)


def is_responsive(url: str) -> bool:
    try:
        LOGGER.info(f"awaiting for docker {url} to be responsive....")
        response = httpx.get(url + "/protocol", timeout=2.0)
        if response.status_code == 200:
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
