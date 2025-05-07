import pytest

from rdf4j_python import AsyncRdf4jDB
from rdf4j_python.utils.const import Rdf4jContentType


def get_repo_config(name: str):
    return f"""
        @prefix config: <tag:rdf4j.org,2023:config/>.

        [] a config:Repository ;
        config:rep.id "{name}" ;
        config:rep.impl [
            config:rep.type "openrdf:SailRepository" ;
            config:sail.impl [
                config:sail.type "openrdf:MemoryStore" ;
            ]
        ] .
    """


@pytest.mark.asyncio
async def test_create_repo(rdf4j_service: str):
    async with AsyncRdf4jDB(rdf4j_service) as db:
        await db.create_repository(
            repository_id="test-repo",
            rdf_config_data=get_repo_config("test-repo"),
            content_type=Rdf4jContentType.TURTLE,
        )


@pytest.mark.asyncio
async def test_list_repos(rdf4j_service: str):
    async with AsyncRdf4jDB(rdf4j_service) as db:
        repos = await db.list_repositories()
        print(repos)
