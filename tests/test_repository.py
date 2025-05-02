import pytest

from rdf4j_python import AsyncRdf4jDB, AsyncRepository
from rdf4j_python.utils.const import Rdf4jContentType


@pytest.mark.asyncio
async def test_url(rdf4j_service: str):
    repo_config = """
        @prefix config: <tag:rdf4j.org,2023:config/>.

        [] a config:Repository ;
        config:rep.id "test-repo" ;
        config:rep.impl [
            config:rep.type "openrdf:SailRepository" ;
            config:sail.impl [
                config:sail.type "openrdf:MemoryStore" ;
            ]
        ] .
    """
    async with AsyncRdf4jDB(rdf4j_service) as db:
        repo: AsyncRepository = await db.create_repository(
            repository_id="test-repo",
            rdf_config_data=repo_config,
            content_type=Rdf4jContentType.TURTLE,
        )
