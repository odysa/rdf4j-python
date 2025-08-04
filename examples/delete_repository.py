"""
Example: Deleting RDF4J Repositories

This example demonstrates how to safely delete repositories from an RDF4J server.
It includes various deletion scenarios and safety checks.
"""
import asyncio

from rdf4j_python import AsyncRdf4j
from rdf4j_python.exception.repo_exception import RepositoryDeletionException


async def delete_repository_by_id(repository_id: str):
    """Delete a specific repository by its ID."""
    print(f"🗑️  Deleting repository '{repository_id}'...")
    
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        try:
            # Check if repository exists first
            repositories = await db.list_repositories()
            repo_exists = any(repo.id == repository_id for repo in repositories)
            
            if not repo_exists:
                print(f"❌ Repository '{repository_id}' does not exist.")
                return False
            
            # Delete the repository
            await db.delete_repository(repository_id)
            print(f"✅ Successfully deleted repository '{repository_id}'")
            return True
            
        except RepositoryDeletionException as e:
            print(f"❌ Failed to delete repository '{repository_id}': {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error deleting repository '{repository_id}': {e}")
            return False


async def delete_repositories_by_pattern(pattern: str):
    """Delete all repositories whose IDs match a pattern."""
    print(f"🗑️  Deleting repositories matching pattern '*{pattern}*'...")
    
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        try:
            # Get list of repositories
            repositories = await db.list_repositories()
            
            # Find repositories matching the pattern
            matching_repos = [
                repo for repo in repositories 
                if pattern.lower() in repo.id.lower()
            ]
            
            if not matching_repos:
                print(f"No repositories found matching pattern '*{pattern}*'")
                return []
            
            print(f"Found {len(matching_repos)} repository(ies) to delete:")
            for repo in matching_repos:
                print(f"   • {repo.id} - {repo.title}")
            
            # Confirm deletion
            print("\nProceeding with deletion...")
            deleted_repos = []
            failed_repos = []
            
            for repo in matching_repos:
                try:
                    await db.delete_repository(repo.id)
                    deleted_repos.append(repo.id)
                    print(f"✅ Deleted: {repo.id}")
                except Exception as e:
                    failed_repos.append((repo.id, str(e)))
                    print(f"❌ Failed to delete {repo.id}: {e}")
            
            print(f"\n📊 Deletion Summary:")
            print(f"   Successfully deleted: {len(deleted_repos)}")
            print(f"   Failed to delete: {len(failed_repos)}")
            
            return deleted_repos
            
        except Exception as e:
            print(f"❌ Error during pattern deletion: {e}")
            return []


async def safe_delete_with_confirmation(repository_id: str):
    """Delete a repository with extra safety checks and detailed information."""
    print(f"🔍 Preparing to delete repository '{repository_id}' with safety checks...")
    
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        try:
            # Get repository information
            repositories = await db.list_repositories()
            target_repo = next((repo for repo in repositories if repo.id == repository_id), None)
            
            if not target_repo:
                print(f"❌ Repository '{repository_id}' not found.")
                return False
            
            print(f"📋 Repository Information:")
            print(f"   ID: {target_repo.id}")
            print(f"   Title: {target_repo.title}")
            print(f"   URI: {target_repo.uri}")
            print(f"   Readable: {target_repo.readable}")
            print(f"   Writable: {target_repo.writable}")
            
            # Check if repository has data (optional - depends on use case)
            try:
                repo = await db.get_repository(repository_id)
                statements = await repo.get_statements()
                statement_count = len(list(statements))
                print(f"   Data: ~{statement_count} statements")
                
                if statement_count > 0:
                    print(f"⚠️  Warning: Repository contains {statement_count} statements that will be lost!")
            except Exception:
                print("   Data: Unable to check statement count")
            
            print(f"\n⚠️  This action will permanently delete the repository and all its data.")
            print(f"    Repository to delete: {repository_id}")
            
            # Simulate confirmation (in real use, you might want user input)
            print("    Proceeding with deletion (automatic confirmation)...")
            
            # Delete the repository
            await db.delete_repository(repository_id)
            print(f"✅ Repository '{repository_id}' has been successfully deleted.")
            return True
            
        except Exception as e:
            print(f"❌ Error during safe deletion: {e}")
            return False


async def cleanup_test_repositories():
    """Clean up repositories that appear to be test repositories."""
    print("🧹 Cleaning up test repositories...")
    
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        try:
            repositories = await db.list_repositories()
            
            # Identify test repositories (repositories with 'test', 'example', or 'temp' in their names)
            test_patterns = ['test', 'example', 'temp', 'demo']
            test_repos = [
                repo for repo in repositories 
                if any(pattern in repo.id.lower() for pattern in test_patterns)
            ]
            
            if not test_repos:
                print("No test repositories found to clean up.")
                return []
            
            print(f"Found {len(test_repos)} test repository(ies):")
            for repo in test_repos:
                print(f"   • {repo.id} - {repo.title}")
            
            deleted_repos = []
            for repo in test_repos:
                try:
                    await db.delete_repository(repo.id)
                    deleted_repos.append(repo.id)
                    print(f"✅ Cleaned up: {repo.id}")
                except Exception as e:
                    print(f"❌ Failed to clean up {repo.id}: {e}")
            
            print(f"\n🎉 Cleanup completed! Removed {len(deleted_repos)} test repositories.")
            return deleted_repos
            
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            return []


async def main():
    """Main function demonstrating repository deletion examples."""
    print("🚀 RDF4J Repository Deletion Examples")
    print("=" * 50)
    
    try:
        # First, let's see what repositories are available
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            repositories = await db.list_repositories()
            print(f"📋 Current repositories ({len(repositories)}):")
            for repo in repositories:
                print(f"   • {repo.id} - {repo.title}")
        
        if not repositories:
            print("No repositories found. Use create_repository.py to create some first.")
            return
        
        print("\n" + "=" * 50)
        
        # Example 1: Delete repositories by pattern (example repositories)
        await delete_repositories_by_pattern("example")
        
        # Example 2: Safe deletion with detailed checks (if there are still repositories)
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            remaining_repos = await db.list_repositories()
            if remaining_repos:
                # Try to delete the first remaining repository with safety checks
                await safe_delete_with_confirmation(remaining_repos[0].id)
        
        # Example 3: Cleanup test repositories
        await cleanup_test_repositories()
        
        # Show final state
        print("\n" + "=" * 50)
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            final_repos = await db.list_repositories()
            print(f"📋 Repositories after deletion ({len(final_repos)}):")
            for repo in final_repos:
                print(f"   • {repo.id} - {repo.title}")
        
        print("\n✅ Repository deletion examples completed!")
        
    except Exception as e:
        print(f"❌ Error in deletion examples: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())