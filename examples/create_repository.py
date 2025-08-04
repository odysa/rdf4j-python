"""
Example: Creating RDF4J Repositories

This example demonstrates how to create different types of RDF4J repositories
using the rdf4j-python library. It shows creating both memory-based and
persistent repositories with various configurations.
"""

import asyncio

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model.repository_config import (
    MemoryStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
)


async def create_memory_repository():
    """Create an in-memory repository that doesn't persist data."""
    print("Creating in-memory repository...")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Create repository configuration for an in-memory store
        repo_config = RepositoryConfig(
            repo_id="memory-example",
            title="In-Memory Example Repository",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
        )

        # Create the repository
        await db.create_repository(config=repo_config)
        print(f"✅ Created repository: {repo_config.repo_id}")
        print(f"   Title: {repo_config.title}")
        print("   Type: In-memory (non-persistent)")

        return repo_config.repo_id


async def create_persistent_memory_repository():
    """Create a memory repository that persists data to disk."""
    print("\nCreating persistent memory repository...")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Create repository configuration for a persistent memory store
        repo_config = RepositoryConfig(
            repo_id="persistent-memory-example",
            title="Persistent Memory Example Repository",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=True)),
        )

        # Create the repository
        await db.create_repository(config=repo_config)
        print(f"✅ Created repository: {repo_config.repo_id}")
        print(f"   Title: {repo_config.title}")
        print("   Type: Memory with persistence")

        return repo_config.repo_id


async def create_repository_with_custom_config():
    """Create a repository with custom configuration options."""
    print("\nCreating repository with custom configuration...")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Create repository configuration with custom settings
        repo_config = RepositoryConfig(
            repo_id="custom-config-example",
            title="Custom Configuration Example Repository",
            impl=SailRepositoryConfig(
                sail_impl=MemoryStoreConfig(
                    persist=False,
                    sync_delay=1000,  # Custom sync delay
                )
            ),
        )

        # Create the repository
        await db.create_repository(config=repo_config)
        print(f"✅ Created repository: {repo_config.repo_id}")
        print(f"   Title: {repo_config.title}")
        print("   Type: Custom configured memory store")

        return repo_config.repo_id


async def main():
    """Main function demonstrating repository creation examples."""
    print("🚀 RDF4J Repository Creation Examples")
    print("=" * 50)

    try:
        # Create different types of repositories
        repo_ids = []

        # Example 1: In-memory repository
        repo_id1 = await create_memory_repository()
        repo_ids.append(repo_id1)

        # Example 2: Persistent memory repository
        repo_id2 = await create_persistent_memory_repository()
        repo_ids.append(repo_id2)

        # Example 3: Custom configuration
        repo_id3 = await create_repository_with_custom_config()
        repo_ids.append(repo_id3)

        print(f"\n🎉 Successfully created {len(repo_ids)} repositories!")
        print("\nCreated repositories:")
        for i, repo_id in enumerate(repo_ids, 1):
            print(f"  {i}. {repo_id}")

        print("\n💡 Tip: Use list_repositories.py to see all repositories")
        print("💡 Tip: Use delete_repository.py to clean up test repositories")

    except Exception as e:
        print(f"❌ Error creating repositories: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
