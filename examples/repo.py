import asyncio

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model.repository_config import MemoryStoreConfig, RepositoryConfig
from rdf4j_python.model.term import IRI, Literal


async def main():
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repo_config = (
            RepositoryConfig.Builder()
            .repo_id("example-repo-2")
            .title("Example Repository")
            .sail_repository_impl(
                MemoryStoreConfig.Builder().persist(False).build(),
            )
            .build()
        )
        repo = await db.create_repository(config=repo_config)
        await repo.add_statement(
            IRI("http://example.com/subject"),
            IRI("http://example.com/predicate"),
            Literal("test_object"),
        )
        await repo.get_statements(subject=IRI("http://example.com/subject"))


if __name__ == "__main__":
    asyncio.run(main())
