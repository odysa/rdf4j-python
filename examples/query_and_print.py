"""
Example: Querying RDF4J Repositories and Printing Results

This example demonstrates how to execute various types of SPARQL queries
against RDF4J repositories and format the results in different ways.

Queries are built using the fluent SPARQL query builder.
"""

import asyncio
from typing import Any

from pyoxigraph import QuerySolutions, QueryTriples

from rdf4j_python import AsyncRdf4j, ask, construct, select
from rdf4j_python.model._namespace import Namespace
from rdf4j_python.model.repository_config import (
    MemoryStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
)
from rdf4j_python.model.term import IRI, Literal, Quad

# Define namespaces for query building
ex = Namespace("ex", "http://example.org/")


async def setup_test_data(repo):
    """Add some test data to the repository for querying."""
    print("📝 Adding test data to repository...")

    # Add some sample triples
    test_data = [
        Quad(
            IRI("http://example.org/person/alice"),
            IRI("http://example.org/name"),
            Literal("Alice"),
            IRI("http://example.org/graph/people"),
        ),
        Quad(
            IRI("http://example.org/person/alice"),
            IRI("http://example.org/age"),
            Literal("30", datatype=IRI("http://www.w3.org/2001/XMLSchema#integer")),
            IRI("http://example.org/graph/people"),
        ),
        Quad(
            IRI("http://example.org/person/alice"),
            IRI("http://example.org/email"),
            Literal("alice@example.org"),
            IRI("http://example.org/graph/people"),
        ),
        Quad(
            IRI("http://example.org/person/bob"),
            IRI("http://example.org/name"),
            Literal("Bob"),
            IRI("http://example.org/graph/people"),
        ),
        Quad(
            IRI("http://example.org/person/bob"),
            IRI("http://example.org/age"),
            Literal("25", datatype=IRI("http://www.w3.org/2001/XMLSchema#integer")),
            IRI("http://example.org/graph/people"),
        ),
        Quad(
            IRI("http://example.org/person/charlie"),
            IRI("http://example.org/name"),
            Literal("Charlie"),
            IRI("http://example.org/graph/people"),
        ),
        Quad(
            IRI("http://example.org/person/charlie"),
            IRI("http://example.org/age"),
            Literal("35", datatype=IRI("http://www.w3.org/2001/XMLSchema#integer")),
            IRI("http://example.org/graph/people"),
        ),
        # Add some organizational data
        Quad(
            IRI("http://example.org/org/company-a"),
            IRI("http://example.org/name"),
            Literal("Company A"),
            IRI("http://example.org/graph/organizations"),
        ),
        Quad(
            IRI("http://example.org/person/alice"),
            IRI("http://example.org/worksFor"),
            IRI("http://example.org/org/company-a"),
            IRI("http://example.org/graph/employment"),
        ),
        Quad(
            IRI("http://example.org/person/bob"),
            IRI("http://example.org/worksFor"),
            IRI("http://example.org/org/company-a"),
            IRI("http://example.org/graph/employment"),
        ),
    ]

    await repo.add_statements(test_data)
    print(f"✅ Added {len(test_data)} test statements")


def print_select_results(result: QuerySolutions, title: str):
    """Print SELECT query results in a formatted table."""
    print(f"\n📊 {title}")
    print("=" * 60)

    # Convert to list to get count and iterate multiple times
    results_list = list(result)

    if not results_list:
        print("No results found.")
        return

    # Get variable names from the first result
    variables = list(results_list[0].keys())

    # Print header
    header = " | ".join(f"{var:15}" for var in variables)
    print(header)
    print("-" * len(header))

    # Print rows
    for solution in results_list:
        row = " | ".join(
            f"{solution[var].value if solution[var] else 'NULL':15}"
            for var in variables
        )
        print(row)

    print(f"\nTotal results: {len(results_list)}")


def print_construct_results(result: QueryTriples, title: str):
    """Print CONSTRUCT query results."""
    print(f"\n🔗 {title}")
    print("=" * 60)

    # Convert to list to count
    triples_list = list(result)

    if not triples_list:
        print("No triples found.")
        return

    for i, triple in enumerate(triples_list, 1):
        print(f"{i}. Subject:   {triple.subject}")
        print(f"   Predicate: {triple.predicate}")
        print(f"   Object:    {triple.object}")
        print()

    print(f"Total triples: {len(triples_list)}")


def print_results_as_json(result: Any, title: str):
    """Print query results in a JSON-like format."""
    print(f"\n📄 {title} (JSON-like format)")
    print("=" * 60)

    if isinstance(result, QuerySolutions):
        results_list = list(result)
        print('{\n  "results": [')

        for i, solution in enumerate(results_list):
            print("    {")
            for j, (var, value) in enumerate(solution.items()):
                value_str = value.value if value else "null"
                comma = "," if j < len(solution) - 1 else ""
                print(f'      "{var}": "{value_str}"{comma}')
            comma = "," if i < len(results_list) - 1 else ""
            print(f"    }}{comma}")

        print("  ]")
        print(f'  "total": {len(results_list)}')
        print("}")

    elif isinstance(result, QueryTriples):
        triples_list = list(result)
        print('{\n  "triples": [')

        for i, triple in enumerate(triples_list):
            comma = "," if i < len(triples_list) - 1 else ""
            print("    {")
            print(f'      "subject": "{triple.subject}",')
            print(f'      "predicate": "{triple.predicate}",')
            print(f'      "object": "{triple.object}"')
            print(f"    }}{comma}")

        print("  ]")
        print(f'  "total": {len(triples_list)}')
        print("}")


async def execute_select_queries(repo):
    """Execute various SELECT queries and display results."""
    print("\n🔍 Executing SELECT Queries")
    print("=" * 50)

    # Query 1: Simple SELECT - get all people and their names
    query1 = (
        select("?person", "?name")
        .where("?person", ex.name, "?name")
        .build()
    )
    result1 = await repo.query(query1)
    print_select_results(result1, "All People and Their Names")

    # Query 2: SELECT with FILTER - people older than 30
    query2 = (
        select("?person", "?name", "?age")
        .where("?person", ex.name, "?name")
        .where("?person", ex.age, "?age")
        .filter("?age > 30")
        .build()
    )
    result2 = await repo.query(query2)
    print_select_results(result2, "People Older Than 30")

    # Query 3: SELECT with OPTIONAL - people and their email (if available)
    query3 = (
        select("?person", "?name", "?email")
        .where("?person", ex.name, "?name")
        .optional("?person", ex.email, "?email")
        .build()
    )
    result3 = await repo.query(query3)
    print_select_results(result3, "People and Their Email Addresses")

    # Query 4: SELECT with JOIN - people and their employers
    query4 = (
        select("?person", "?name", "?company")
        .where("?person", ex.name, "?name")
        .where("?person", ex.worksFor, "?org")
        .where("?org", ex.name, "?company")
        .build()
    )
    result4 = await repo.query(query4)
    print_select_results(result4, "People and Their Employers")

    # Show the same result in JSON format
    print_results_as_json(result4, "People and Their Employers")


async def execute_construct_queries(repo):
    """Execute CONSTRUCT queries and display results."""
    print("\n🏗️  Executing CONSTRUCT Queries")
    print("=" * 50)

    # Query 1: CONSTRUCT - create simplified person data
    query1 = (
        construct(
            ("?person", ex.hasName, "?name"),
            ("?person", ex.hasAge, "?age"),
        )
        .where("?person", ex.name, "?name")
        .where("?person", ex.age, "?age")
        .build()
    )
    result1 = await repo.query(query1)
    print_construct_results(result1, "Simplified Person Data")

    # Query 2: CONSTRUCT - create employment relationships
    query2 = (
        construct(("?person", ex.employedBy, "?company"))
        .where("?person", ex.worksFor, "?org")
        .where("?org", ex.name, "?company")
        .build()
    )
    result2 = await repo.query(query2)
    print_construct_results(result2, "Employment Relationships")


async def execute_ask_queries(repo):
    """Execute ASK queries and display results."""
    print("\n❓ Executing ASK Queries")
    print("=" * 50)

    # Query 1: ASK - check if Alice exists
    query1 = (
        ask()
        .where("?person", ex.name, '"Alice"')
        .build()
    )
    result1 = await repo.query(query1)
    print(f"Does Alice exist? {'✅ Yes' if result1 else '❌ No'}")

    # Query 2: ASK - check if anyone is older than 40
    query2 = (
        ask()
        .where("?person", ex.age, "?age")
        .filter("?age > 40")
        .build()
    )
    result2 = await repo.query(query2)
    print(f"Is anyone older than 40? {'✅ Yes' if result2 else '❌ No'}")

    # Query 3: ASK - check if there are any email addresses
    query3 = (
        ask()
        .where("?person", ex.email, "?email")
        .build()
    )
    result3 = await repo.query(query3)
    print(f"Are there any email addresses? {'✅ Yes' if result3 else '❌ No'}")


async def execute_aggregate_queries(repo):
    """Execute queries with aggregations and display results."""
    print("\n📈 Executing Aggregate Queries")
    print("=" * 50)

    # Query 1: COUNT - total number of people
    query1 = (
        select("(COUNT(?person) AS ?totalPeople)")
        .where("?person", ex.name, "?name")
        .build()
    )
    result1 = await repo.query(query1)
    print_select_results(result1, "Total Number of People")

    # Query 2: AVG - average age
    query2 = (
        select("(AVG(?age) AS ?averageAge)")
        .where("?person", ex.age, "?age")
        .build()
    )
    result2 = await repo.query(query2)
    print_select_results(result2, "Average Age")

    # Query 3: MIN/MAX - youngest and oldest person
    query3 = (
        select("(MIN(?age) AS ?minAge)", "(MAX(?age) AS ?maxAge)")
        .where("?person", ex.age, "?age")
        .build()
    )
    result3 = await repo.query(query3)
    print_select_results(result3, "Age Range (Min/Max)")


async def main():
    """Main function demonstrating query examples."""
    print("🚀 RDF4J Query and Print Results Examples")
    print("=" * 60)

    try:
        # Create a temporary repository for testing
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            # Create repository configuration
            repo_config = RepositoryConfig(
                repo_id="query-example-repo",
                title="Query Example Repository",
                impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
            )

            # Create the repository
            repo = await db.create_repository(config=repo_config)
            print(f"✅ Created temporary repository: {repo_config.repo_id}")

            try:
                # Setup test data
                await setup_test_data(repo)

                # Execute different types of queries
                await execute_select_queries(repo)
                await execute_construct_queries(repo)
                await execute_ask_queries(repo)
                await execute_aggregate_queries(repo)

                print("\n🎉 All query examples completed successfully!")

            finally:
                # Clean up: delete the temporary repository
                print("\n🧹 Cleaning up temporary repository...")
                await db.delete_repository(repo_config.repo_id)
                print("✅ Temporary repository deleted")

    except Exception as e:
        print(f"❌ Error in query examples: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
