#!/usr/bin/env python3
"""
Demo script for RDF4J CLI showing table formatting capabilities.

This script demonstrates the CLI functionality without requiring a live RDF4J server.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from pyoxigraph import QuerySolutions, QueryTriples

from rdf4j_python.cli import RDF4JCLI


def create_sample_select_results():
    """Create sample SELECT query results."""
    solutions = []
    
    # Sample data: People with names and ages
    people_data = [
        ("http://example.org/person/alice", "Alice", "30"),
        ("http://example.org/person/bob", "Bob", "25"),
        ("http://example.org/person/charlie", "Charlie", "35"),
        ("http://example.org/person/diana", "Diana", "28"),
    ]
    
    for person_uri, name, age in people_data:
        solution = MagicMock()
        solution.keys.return_value = ['person', 'name', 'age']
        solution.__getitem__.side_effect = lambda key, p=person_uri, n=name, a=age: {
            'person': MagicMock(value=p),
            'name': MagicMock(value=n),
            'age': MagicMock(value=a)
        }[key]
        solutions.append(solution)
    
    mock_results = MagicMock(spec=QuerySolutions)
    mock_results.__iter__.return_value = iter(solutions)
    return mock_results


def create_sample_construct_results():
    """Create sample CONSTRUCT query results."""
    triples = []
    
    # Sample triples
    triple_data = [
        ("http://example.org/person/alice", "http://example.org/hasName", "Alice"),
        ("http://example.org/person/alice", "http://example.org/hasAge", "30"),
        ("http://example.org/person/bob", "http://example.org/hasName", "Bob"),
        ("http://example.org/person/bob", "http://example.org/hasAge", "25"),
    ]
    
    for subject, predicate, obj in triple_data:
        triple = MagicMock()
        triple.subject = subject
        triple.predicate = predicate
        triple.object = obj
        triples.append(triple)
    
    mock_results = MagicMock(spec=QueryTriples)
    mock_results.__iter__.return_value = iter(triples)
    return mock_results


async def main():
    """Demonstrate CLI table formatting with sample data."""
    print("🎬 RDF4J CLI - Table Formatting Demo")
    print("=" * 50)
    
    cli = RDF4JCLI()
    
    # Demo 1: SELECT Query Results
    print("\n1️⃣  SELECT Query Results:")
    print("Query: SELECT ?person ?name ?age WHERE { ?person ex:name ?name ; ex:age ?age }")
    
    select_results = create_sample_select_results()
    cli.print_select_results(select_results, "People Data")
    
    # Demo 2: CONSTRUCT Query Results
    print("\n2️⃣  CONSTRUCT Query Results:")
    print("Query: CONSTRUCT { ?person ex:hasName ?name ; ex:hasAge ?age } WHERE { ... }")
    
    construct_results = create_sample_construct_results()
    cli.print_construct_results(construct_results, "Person Triples")
    
    # Demo 3: ASK Query Result
    print("\n3️⃣  ASK Query Result:")
    print("Query: ASK { ?person ex:name 'Alice' }")
    print("\n❓ ASK Query Result: ✅ True")
    
    # Demo 4: Empty Results
    print("\n4️⃣  Empty Results:")
    print("Query: SELECT * WHERE { ?s ex:nonexistent ?o }")
    
    empty_results = MagicMock(spec=QuerySolutions)
    empty_results.__iter__.return_value = iter([])
    cli.print_select_results(empty_results, "Empty Query")
    
    print("\n" + "=" * 50)
    print("🎉 Demo completed!")
    print("\n📋 To try the interactive CLI:")
    print("   python rdf4j_python/cli.py")
    print("\n🔧 Setup required for real usage:")
    print("   1. Start RDF4J server: docker run -p 19780:8080 eclipse/rdf4j:latest")
    print("   2. Create a repository via RDF4J Workbench")
    print("   3. Connect: \\c http://localhost:19780/rdf4j-server <repo-name>")
    print("   4. Run SPARQL queries and see formatted results!")


if __name__ == "__main__":
    asyncio.run(main())