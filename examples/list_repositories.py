"""
Example: Listing RDF4J Repositories

This example demonstrates how to list all available repositories in an RDF4J server
and display their metadata including ID, title, URI, and access permissions.
"""

import asyncio

from rdf4j_python import AsyncRdf4j


async def list_all_repositories():
    """List all repositories and display their metadata."""
    print("📋 Listing all repositories...")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Get list of all repositories
        repositories = await db.list_repositories()

        if not repositories:
            print("No repositories found on the server.")
            return []

        print(f"Found {len(repositories)} repository(ies):")
        print("=" * 80)

        for i, repo in enumerate(repositories, 1):
            print(f"{i}. Repository Details:")
            print(f"   ID: {repo.id}")
            print(f"   Title: {repo.title}")
            print(f"   URI: {repo.uri}")
            print(f"   Readable: {'✅' if repo.readable else '❌'}")
            print(f"   Writable: {'✅' if repo.writable else '❌'}")
            print("-" * 40)

        return repositories


async def filter_repositories_by_access():
    """Filter and display repositories by their access permissions."""
    print("\n🔍 Filtering repositories by access permissions...")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repositories = await db.list_repositories()

        if not repositories:
            print("No repositories found on the server.")
            return

        # Filter readable repositories
        readable_repos = [repo for repo in repositories if repo.readable]
        writable_repos = [repo for repo in repositories if repo.writable]
        read_only_repos = [
            repo for repo in repositories if repo.readable and not repo.writable
        ]

        print(f"📖 Readable repositories ({len(readable_repos)}):")
        for repo in readable_repos:
            print(f"   • {repo.id} - {repo.title}")

        print(f"\n✏️  Writable repositories ({len(writable_repos)}):")
        for repo in writable_repos:
            print(f"   • {repo.id} - {repo.title}")

        print(f"\n👁️  Read-only repositories ({len(read_only_repos)}):")
        for repo in read_only_repos:
            print(f"   • {repo.id} - {repo.title}")


async def search_repositories_by_name(search_term):
    """Search repositories by name/ID containing a specific term."""
    print(f"\n🔎 Searching repositories containing '{search_term}'...")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repositories = await db.list_repositories()

        if not repositories:
            print("No repositories found on the server.")
            return []

        # Search repositories by ID or title
        matching_repos = [
            repo
            for repo in repositories
            if search_term.lower() in repo.id.lower()
            or search_term.lower() in repo.title.lower()
        ]

        if not matching_repos:
            print(f"No repositories found matching '{search_term}'")
            return []

        print(f"Found {len(matching_repos)} matching repository(ies):")
        for repo in matching_repos:
            print(f"   • {repo.id} - {repo.title}")
            print(f"     URI: {repo.uri}")

        return matching_repos


async def display_repository_summary():
    """Display a summary of repository statistics."""
    print("\n📊 Repository Summary:")

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repositories = await db.list_repositories()

        total_repos = len(repositories)
        readable_count = sum(1 for repo in repositories if repo.readable)
        writable_count = sum(1 for repo in repositories if repo.writable)
        read_only_count = sum(
            1 for repo in repositories if repo.readable and not repo.writable
        )

        print(f"   Total repositories: {total_repos}")
        print(f"   Readable: {readable_count}")
        print(f"   Writable: {writable_count}")
        print(f"   Read-only: {read_only_count}")

        if total_repos > 0:
            print(f"   Readable percentage: {readable_count / total_repos * 100:.1f}%")
            print(f"   Writable percentage: {writable_count / total_repos * 100:.1f}%")


async def main():
    """Main function demonstrating repository listing examples."""
    print("🚀 RDF4J Repository Listing Examples")
    print("=" * 50)

    try:
        # Example 1: List all repositories
        repositories = await list_all_repositories()

        # Example 2: Filter by access permissions (only if repositories exist)
        if repositories:
            await filter_repositories_by_access()

            # Example 3: Search repositories by name
            await search_repositories_by_name("example")

            # Example 4: Display summary statistics
            await display_repository_summary()
        else:
            print(
                "\n💡 Tip: Use create_repository.py to create some repositories first"
            )

        print("\n✅ Repository listing completed successfully!")

    except Exception as e:
        print(f"❌ Error listing repositories: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
