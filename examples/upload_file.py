"""
Example: Uploading RDF Files to a Repository

This example demonstrates how to upload RDF files in various formats
(Turtle, N-Triples, N-Quads, RDF/XML, JSON-LD, etc.) to an RDF4J repository.
"""

import asyncio
import tempfile
from pathlib import Path

import pyoxigraph as og

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model.repository_config import (
    MemoryStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
)
from rdf4j_python.model.term import IRI


async def example_upload_turtle_file():
    """Example 1: Upload a Turtle file."""
    print("=" * 60)
    print("Example 1: Uploading a Turtle File")
    print("=" * 60)

    # Create a sample Turtle file
    turtle_content = """
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice a foaf:Person ;
    foaf:name "Alice Smith" ;
    foaf:age 28 ;
    foaf:knows ex:bob .

ex:bob a foaf:Person ;
    foaf:name "Bob Jones" ;
    foaf:age 32 .
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write(turtle_content)
        turtle_file = f.name

    try:
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            # Create a repository
            config = RepositoryConfig(
                repo_id="upload-example",
                title="File Upload Example Repository",
                impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
            )
            repo = await db.create_repository(config=config)

            # Upload the Turtle file (format auto-detected from extension)
            print(f"📤 Uploading file: {turtle_file}")
            await repo.upload_file(turtle_file)
            print("✅ File uploaded successfully!")

            # Query the data
            print("\n📊 Querying uploaded data:")
            result = await repo.query("""
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                SELECT ?person ?name ?age WHERE {
                    ?person a foaf:Person ;
                            foaf:name ?name .
                    OPTIONAL { ?person foaf:age ?age }
                }
                ORDER BY ?name
            """)

            for row in result:
                name = row["name"].value if row["name"] else "N/A"
                age = row["age"].value if row["age"] else "N/A"
                print(f"   • {name} - Age: {age}")

            # Clean up
            await db.delete_repository("upload-example")
            print("\n🧹 Repository cleaned up")

    finally:
        Path(turtle_file).unlink()


async def example_upload_to_named_graph():
    """Example 2: Upload a file to a specific named graph."""
    print("\n" + "=" * 60)
    print("Example 2: Uploading to a Named Graph")
    print("=" * 60)

    # Create sample data
    data_content = """
@prefix ex: <http://example.com/> .

ex:product1 ex:name "Laptop Pro" ;
           ex:price 1299.99 ;
           ex:category "Electronics" .

ex:product2 ex:name "Desk Chair" ;
           ex:price 299.99 ;
           ex:category "Furniture" .
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write(data_content)
        data_file = f.name

    try:
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            config = RepositoryConfig(
                repo_id="named-graph-example",
                title="Named Graph Example",
                impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
            )
            repo = await db.create_repository(config=config)

            # Upload to a specific named graph
            graph_uri = IRI("http://example.com/graphs/products")
            print(f"📤 Uploading file to graph: {graph_uri}")
            await repo.upload_file(data_file, context=graph_uri)
            print("✅ File uploaded to named graph!")

            # Query from the specific graph
            print("\n📊 Querying from named graph:")
            result = await repo.query(f"""
                PREFIX ex: <http://example.com/>
                SELECT ?product ?name ?price WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?product ex:name ?name ;
                                ex:price ?price .
                    }}
                }}
                ORDER BY ?price
            """)

            for row in result:
                name = row["name"].value if row["name"] else "N/A"
                price = row["price"].value if row["price"] else "N/A"
                print(f"   • {name} - ${price}")

            # Clean up
            await db.delete_repository("named-graph-example")
            print("\n🧹 Repository cleaned up")

    finally:
        Path(data_file).unlink()


async def example_upload_multiple_formats():
    """Example 3: Upload files in different RDF formats."""
    print("\n" + "=" * 60)
    print("Example 3: Uploading Multiple File Formats")
    print("=" * 60)

    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        config = RepositoryConfig(
            repo_id="multi-format-example",
            title="Multi-Format Example",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
        )
        repo = await db.create_repository(config=config)

        # 1. Upload N-Triples file
        nt_content = (
            '<http://example.com/person1> <http://xmlns.com/foaf/0.1/name> "Alice" .\n'
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".nt", delete=False) as f:
            f.write(nt_content)
            nt_file = f.name

        print("📤 Uploading N-Triples file...")
        await repo.upload_file(nt_file)
        Path(nt_file).unlink()
        print("✅ N-Triples file uploaded")

        # 2. Upload RDF/XML file
        rdfxml_content = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:foaf="http://xmlns.com/foaf/0.1/">
  <rdf:Description rdf:about="http://example.com/person2">
    <foaf:name>Bob</foaf:name>
  </rdf:Description>
</rdf:RDF>
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".rdf", delete=False) as f:
            f.write(rdfxml_content)
            rdf_file = f.name

        print("📤 Uploading RDF/XML file...")
        await repo.upload_file(rdf_file)
        Path(rdf_file).unlink()
        print("✅ RDF/XML file uploaded")

        # 3. Upload N-Quads file (with named graphs)
        nq_content = '<http://example.com/person3> <http://xmlns.com/foaf/0.1/name> "Charlie" <http://example.com/graph1> .\n'
        with tempfile.NamedTemporaryFile(mode="w", suffix=".nq", delete=False) as f:
            f.write(nq_content)
            nq_file = f.name

        print("📤 Uploading N-Quads file...")
        await repo.upload_file(nq_file)
        Path(nq_file).unlink()
        print("✅ N-Quads file uploaded")

        # Query all uploaded data
        print("\n📊 All uploaded data:")
        result = await repo.query("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT ?person ?name WHERE {
                ?person foaf:name ?name .
            }
            ORDER BY ?name
        """)

        for row in result:
            person = row["person"].value if row["person"] else "N/A"
            name = row["name"].value if row["name"] else "N/A"
            person_id = person.split("/")[-1] if person != "N/A" else person
            print(f"   • {person_id}: {name}")

        # Clean up
        await db.delete_repository("multi-format-example")
        print("\n🧹 Repository cleaned up")


async def example_upload_with_base_uri():
    """Example 4: Upload a file with relative URIs using a base URI."""
    print("\n" + "=" * 60)
    print("Example 4: Uploading with Base URI for Relative URIs")
    print("=" * 60)

    # Create a file with relative URIs
    relative_uris_content = """
<alice> <http://xmlns.com/foaf/0.1/name> "Alice" .
<bob> <http://xmlns.com/foaf/0.1/name> "Bob" .
<alice> <http://xmlns.com/foaf/0.1/knows> <bob> .
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write(relative_uris_content)
        relative_file = f.name

    try:
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            config = RepositoryConfig(
                repo_id="base-uri-example",
                title="Base URI Example",
                impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
            )
            repo = await db.create_repository(config=config)

            # Upload with a base URI to resolve relative URIs
            base_uri = "http://example.com/people/"
            print(f"📤 Uploading file with base URI: {base_uri}")
            await repo.upload_file(relative_file, base_uri=base_uri)
            print("✅ File uploaded with resolved URIs!")

            # Query the data
            print("\n📊 Querying resolved URIs:")
            result = await repo.query("""
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                SELECT ?person ?name WHERE {
                    ?person foaf:name ?name .
                }
                ORDER BY ?name
            """)

            for row in result:
                person = row["person"].value if row["person"] else "N/A"
                name = row["name"].value if row["name"] else "N/A"
                print(f"   • {person} - {name}")

            # Clean up
            await db.delete_repository("base-uri-example")
            print("\n🧹 Repository cleaned up")

    finally:
        Path(relative_file).unlink()


async def example_upload_with_explicit_format():
    """Example 5: Upload a file with explicitly specified format."""
    print("\n" + "=" * 60)
    print("Example 5: Uploading with Explicit Format")
    print("=" * 60)

    # Create a file without a standard RDF extension
    nt_content = (
        '<http://example.com/doc1> <http://purl.org/dc/terms/title> "My Document" .\n'
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write(nt_content)
        txt_file = f.name

    try:
        async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
            config = RepositoryConfig(
                repo_id="explicit-format-example",
                title="Explicit Format Example",
                impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False)),
            )
            repo = await db.create_repository(config=config)

            # Upload with explicit format specification
            print("📤 Uploading .txt file as N-Triples format")
            await repo.upload_file(txt_file, rdf_format=og.RdfFormat.N_TRIPLES)
            print("✅ File uploaded with explicit format!")

            # Query the data
            print("\n📊 Querying uploaded data:")
            result = await repo.query("""
                PREFIX dc: <http://purl.org/dc/terms/>
                SELECT ?doc ?title WHERE {
                    ?doc dc:title ?title .
                }
            """)

            for row in result:
                doc = row["doc"].value if row["doc"] else "N/A"
                title = row["title"].value if row["title"] else "N/A"
                print(f"   • {doc}: {title}")

            # Clean up
            await db.delete_repository("explicit-format-example")
            print("\n🧹 Repository cleaned up")

    finally:
        Path(txt_file).unlink()


async def main():
    """Run all examples."""
    print("\n🚀 RDF File Upload Examples")
    print("=" * 60)
    print("Demonstrating various ways to upload RDF files to repositories")
    print("=" * 60)

    try:
        await example_upload_turtle_file()
        await example_upload_to_named_graph()
        await example_upload_multiple_formats()
        await example_upload_with_base_uri()
        await example_upload_with_explicit_format()

        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
