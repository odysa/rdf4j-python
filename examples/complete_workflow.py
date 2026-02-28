"""
Example: Complete RDF4J Repository Workflow

This example demonstrates a complete workflow using RDF4J repositories:
1. Creating repositories
2. Adding data
3. Querying data
4. Listing repositories
5. Cleaning up (deleting repositories)

This serves as a comprehensive example showing the full lifecycle.
"""

import asyncio

from rdf4j_python import AsyncRdf4j, select
from rdf4j_python.model._namespace import Namespace
from rdf4j_python.model.repository_config import (
    MemoryStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
)
from rdf4j_python.model.term import IRI, Literal, Quad

# Define namespaces for query building
ecom = Namespace("ecom", "http://example.com/")


async def workflow_step_1_create_repositories():
    """Step 1: Create multiple repositories with different configurations."""
    print("📁 STEP 1: Creating Repositories")
    print("=" * 50)

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repositories_created = []

        # Repository 1: Customer data repository
        customer_repo_config = RepositoryConfig(
            repo_id="customer-data",
            title="Customer Data Repository",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
        )
        await db.create_repository(config=customer_repo_config)
        repositories_created.append(customer_repo_config.repo_id)
        print(f"✅ Created: {customer_repo_config.repo_id}")

        # Repository 2: Product catalog repository
        product_repo_config = RepositoryConfig(
            repo_id="product-catalog",
            title="Product Catalog Repository",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
        )
        await db.create_repository(config=product_repo_config)
        repositories_created.append(product_repo_config.repo_id)
        print(f"✅ Created: {product_repo_config.repo_id}")

        # Repository 3: Analytics repository
        analytics_repo_config = RepositoryConfig(
            repo_id="analytics-data",
            title="Analytics Data Repository",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=True)),
        )
        await db.create_repository(config=analytics_repo_config)
        repositories_created.append(analytics_repo_config.repo_id)
        print(f"✅ Created: {analytics_repo_config.repo_id}")

        print(f"\n🎉 Step 1 Complete: Created {len(repositories_created)} repositories")
        return repositories_created


async def workflow_step_2_add_data():
    """Step 2: Add sample data to the repositories."""
    print("\n📝 STEP 2: Adding Data to Repositories")
    print("=" * 50)

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Add customer data
        customer_repo = await db.get_repository("customer-data")
        customer_data = [
            Quad(
                IRI("http://example.com/customer/1"),
                IRI("http://example.com/name"),
                Literal("John Doe"),
                IRI("http://example.com/graph/customers"),
            ),
            Quad(
                IRI("http://example.com/customer/1"),
                IRI("http://example.com/email"),
                Literal("john.doe@email.com"),
                IRI("http://example.com/graph/customers"),
            ),
            Quad(
                IRI("http://example.com/customer/1"),
                IRI("http://example.com/age"),
                Literal("28", datatype=IRI("http://www.w3.org/2001/XMLSchema#integer")),
                IRI("http://example.com/graph/customers"),
            ),
            Quad(
                IRI("http://example.com/customer/2"),
                IRI("http://example.com/name"),
                Literal("Jane Smith"),
                IRI("http://example.com/graph/customers"),
            ),
            Quad(
                IRI("http://example.com/customer/2"),
                IRI("http://example.com/email"),
                Literal("jane.smith@email.com"),
                IRI("http://example.com/graph/customers"),
            ),
        ]
        await customer_repo.add_statements(customer_data)
        print(f"✅ Added {len(customer_data)} customer records")

        # Add product data
        product_repo = await db.get_repository("product-catalog")
        product_data = [
            Quad(
                IRI("http://example.com/product/laptop-123"),
                IRI("http://example.com/name"),
                Literal("Professional Laptop"),
                IRI("http://example.com/graph/products"),
            ),
            Quad(
                IRI("http://example.com/product/laptop-123"),
                IRI("http://example.com/price"),
                Literal(
                    "1299.99", datatype=IRI("http://www.w3.org/2001/XMLSchema#decimal")
                ),
                IRI("http://example.com/graph/products"),
            ),
            Quad(
                IRI("http://example.com/product/laptop-123"),
                IRI("http://example.com/category"),
                Literal("Electronics"),
                IRI("http://example.com/graph/products"),
            ),
            Quad(
                IRI("http://example.com/product/phone-456"),
                IRI("http://example.com/name"),
                Literal("Smartphone Pro"),
                IRI("http://example.com/graph/products"),
            ),
            Quad(
                IRI("http://example.com/product/phone-456"),
                IRI("http://example.com/price"),
                Literal(
                    "899.99", datatype=IRI("http://www.w3.org/2001/XMLSchema#decimal")
                ),
                IRI("http://example.com/graph/products"),
            ),
        ]
        await product_repo.add_statements(product_data)
        print(f"✅ Added {len(product_data)} product records")

        # Add analytics data
        analytics_repo = await db.get_repository("analytics-data")
        analytics_data = [
            Quad(
                IRI("http://example.com/purchase/1"),
                IRI("http://example.com/customer"),
                IRI("http://example.com/customer/1"),
                IRI("http://example.com/graph/purchases"),
            ),
            Quad(
                IRI("http://example.com/purchase/1"),
                IRI("http://example.com/product"),
                IRI("http://example.com/product/laptop-123"),
                IRI("http://example.com/graph/purchases"),
            ),
            Quad(
                IRI("http://example.com/purchase/1"),
                IRI("http://example.com/date"),
                Literal(
                    "2024-01-15", datatype=IRI("http://www.w3.org/2001/XMLSchema#date")
                ),
                IRI("http://example.com/graph/purchases"),
            ),
            Quad(
                IRI("http://example.com/purchase/2"),
                IRI("http://example.com/customer"),
                IRI("http://example.com/customer/2"),
                IRI("http://example.com/graph/purchases"),
            ),
            Quad(
                IRI("http://example.com/purchase/2"),
                IRI("http://example.com/product"),
                IRI("http://example.com/product/phone-456"),
                IRI("http://example.com/graph/purchases"),
            ),
        ]
        await analytics_repo.add_statements(analytics_data)
        print(f"✅ Added {len(analytics_data)} analytics records")

        print("\n🎉 Step 2 Complete: Added data to all repositories")


async def workflow_step_3_query_data():
    """Step 3: Query data from repositories and display results."""
    print("\n🔍 STEP 3: Querying Repository Data")
    print("=" * 50)

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Query 1: Customer information
        print("👥 Customer Information:")
        customer_repo = await db.get_repository("customer-data")
        customer_query = (
            select("?customer", "?name", "?email", "?age")
            .where("?customer", ecom.name, "?name")
            .optional("?customer", ecom.email, "?email")
            .optional("?customer", ecom.age, "?age")
            .order_by("?name")
            .build()
        )
        customer_results = await customer_repo.query(customer_query)
        for result in customer_results:
            name = result["name"].value if result["name"] else "N/A"
            email = result["email"].value if result["email"] else "N/A"
            age = result["age"].value if result["age"] else "N/A"
            print(f"   • {name} ({email}) - Age: {age}")

        # Query 2: Product catalog
        print("\n🛍️  Product Catalog:")
        product_repo = await db.get_repository("product-catalog")
        product_query = (
            select("?product", "?name", "?price", "?category")
            .where("?product", ecom.name, "?name")
            .optional("?product", ecom.price, "?price")
            .optional("?product", ecom.category, "?category")
            .order_by("?price")
            .build()
        )
        product_results = await product_repo.query(product_query)
        for result in product_results:
            name = result["name"].value if result["name"] else "N/A"
            price = result["price"].value if result["price"] else "N/A"
            category = result["category"].value if result["category"] else "N/A"
            print(f"   • {name} - ${price} ({category})")

        # Query 3: Purchase analytics
        print("\n📊 Purchase Analytics:")
        analytics_repo = await db.get_repository("analytics-data")
        analytics_query = (
            select("?purchase", "?customer", "?product", "?date")
            .where("?purchase", ecom.customer, "?customer")
            .where("?purchase", ecom.product, "?product")
            .optional("?purchase", ecom.date, "?date")
            .order_by("?date")
            .build()
        )
        analytics_results = await analytics_repo.query(analytics_query)
        for result in analytics_results:
            customer = result["customer"].value if result["customer"] else "N/A"
            product = result["product"].value if result["product"] else "N/A"
            date = result["date"].value if result["date"] else "N/A"
            customer_id = customer.split("/")[-1] if "/" in customer else customer
            product_id = product.split("/")[-1] if "/" in product else product
            print(f"   • Customer {customer_id} purchased {product_id} on {date}")

        print("\n🎉 Step 3 Complete: Queried all repositories successfully")


async def workflow_step_4_list_repositories():
    """Step 4: List all repositories and their metadata."""
    print("\n📋 STEP 4: Listing All Repositories")
    print("=" * 50)

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repositories = await db.list_repositories()

        print(f"Found {len(repositories)} repositories:")
        for i, repo in enumerate(repositories, 1):
            print(f"   {i}. {repo.id}")
            print(f"      Title: {repo.title}")
            print(f"      Readable: {'✅' if repo.readable else '❌'}")
            print(f"      Writable: {'✅' if repo.writable else '❌'}")
            print(f"      URI: {repo.uri}")
            print()

        # Filter our workflow repositories
        workflow_repos = [
            repo
            for repo in repositories
            if repo.id in ["customer-data", "product-catalog", "analytics-data"]
        ]
        print(f"📈 Workflow repositories: {len(workflow_repos)}")
        for repo in workflow_repos:
            print(f"   • {repo.id} - {repo.title}")

        print("\n🎉 Step 4 Complete: Listed all repositories")
        return repositories


async def workflow_step_5_cleanup():
    """Step 5: Clean up by deleting the workflow repositories."""
    print("\n🧹 STEP 5: Cleaning Up Repositories")
    print("=" * 50)

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        workflow_repo_ids = ["customer-data", "product-catalog", "analytics-data"]
        deleted_count = 0

        for repo_id in workflow_repo_ids:
            try:
                await db.delete_repository(repo_id)
                print(f"✅ Deleted: {repo_id}")
                deleted_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {repo_id}: {e}")

        print(f"\n🎉 Step 5 Complete: Deleted {deleted_count} repositories")


async def workflow_summary():
    """Display a summary of the complete workflow."""
    print("\n📊 WORKFLOW SUMMARY")
    print("=" * 50)
    print(
        "✅ Step 1: Created 3 repositories (customer-data, product-catalog, analytics-data)"
    )
    print("✅ Step 2: Added sample data to all repositories")
    print("✅ Step 3: Executed queries on all repositories")
    print("✅ Step 4: Listed and analyzed repository metadata")
    print("✅ Step 5: Cleaned up by deleting workflow repositories")
    print("\n🎉 Complete RDF4J workflow executed successfully!")
    print("\n💡 This example demonstrates:")
    print("   • Repository creation with different configurations")
    print("   • Data insertion using quads with named graphs")
    print("   • SPARQL querying with proper result handling")
    print("   • Repository management (listing and deletion)")
    print("   • Proper resource cleanup")


async def main():
    """Main function executing the complete RDF4J workflow."""
    print("🚀 Complete RDF4J Repository Workflow")
    print("=" * 60)
    print(
        "This example demonstrates the full lifecycle of working with RDF4J repositories."
    )
    print("=" * 60)

    try:
        # Execute the complete workflow
        await workflow_step_1_create_repositories()
        await workflow_step_2_add_data()
        await workflow_step_3_query_data()
        await workflow_step_4_list_repositories()
        await workflow_step_5_cleanup()

        # Display summary
        await workflow_summary()

    except Exception as e:
        print(f"❌ Error in workflow: {e}")

        # Attempt cleanup even if there was an error
        print("\n🆘 Attempting emergency cleanup...")
        try:
            async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
                for repo_id in ["customer-data", "product-catalog", "analytics-data"]:
                    try:
                        await db.delete_repository(repo_id)
                        print(f"🧹 Emergency cleanup: Deleted {repo_id}")
                    except Exception:
                        pass  # Repository might not exist
        except Exception:
            pass  # Best effort cleanup

        raise


if __name__ == "__main__":
    asyncio.run(main())
