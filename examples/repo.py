import asyncio

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model import MemoryStoreConfig, RepositoryConfig
from rdf4j_python.utils.const import Rdf4jContentType


async def main():
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repo_config = (
            RepositoryConfig.builder_with_sail_repository(
                MemoryStoreConfig.Builder().persist(False).build(),
            )
            .repo_id("example-repo")
            .title("Example Repository")
            .build()
        )
        await db.create_repository(
            repository_id=repo_config.repo_id,
            rdf_config_data=repo_config.to_turtle(),
            content_type=Rdf4jContentType.TURTLE,
        )


if __name__ == "__main__":
    asyncio.run(main())
