# rdf4j-python

**A modern Python client for the Eclipse RDF4J framework, enabling seamless RDF data management and SPARQL operations from Python applications.**

rdf4j-python bridges the gap between Python and the robust [Eclipse RDF4J](https://rdf4j.org/) ecosystem, providing a clean, async-first API for managing RDF repositories, executing SPARQL queries, and handling semantic data with ease.

> ⚠️ **Note:** This project is currently under active development and considered **experimental**. Interfaces may change. Use with caution in production environments—and feel free to help shape its future!

## Features

- **🚀 Async-First Design**: Native support for async/await with synchronous fallback
- **🔄 Repository Management**: Create, access, and manage RDF4J repositories programmatically
- **⚡ SPARQL Support**: Execute SELECT, ASK, CONSTRUCT, and UPDATE queries effortlessly
- **📊 Flexible Data Handling**: Add, retrieve, and manipulate RDF triples and quads
- **🎯 Multiple Formats**: Support for various RDF serialization formats (Turtle, N-Triples, JSON-LD, etc.)
- **🛠️ Repository Types**: Memory stores, native stores, HTTP repositories, and more
- **🔗 Named Graph Support**: Work with multiple graphs within repositories
- **⚙️ Inferencing**: Built-in support for RDFS and custom inferencing rules

## Installation

### Prerequisites

- Python 3.10 or higher
- RDF4J Server (for remote repositories) or embedded usage

### Install from PyPI

```bash
pip install rdf4j-python
```

### Install with Optional Dependencies

```bash
# Include SPARQLWrapper integration
pip install rdf4j-python[sparqlwrapper]
```

### Development Installation

```bash
git clone https://github.com/odysa/rdf4j-python.git
cd rdf4j-python
uv sync --group dev
```

## Usage

### Quick Start

```python
import asyncio
from rdf4j_python import AsyncRdf4j
from rdf4j_python.model.repository_config import RepositoryConfig, MemoryStoreConfig, SailRepositoryConfig
from rdf4j_python.model.term import IRI, Literal

async def main():
    # Connect to RDF4J server
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Create an in-memory repository
        config = RepositoryConfig(
            repo_id="my-repo",
            title="My Repository",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=False))
        )
        repo = await db.create_repository(config=config)
        
        # Add some data
        await repo.add_statement(
            IRI("http://example.com/person/alice"),
            IRI("http://xmlns.com/foaf/0.1/name"),
            Literal("Alice")
        )
        
        # Query the data
        results = await repo.query("SELECT * WHERE { ?s ?p ?o }")
        for result in results:
            print(f"Subject: {result['s']}, Predicate: {result['p']}, Object: {result['o']}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Working with Multiple Graphs

```python
from rdf4j_python.model.term import Quad

async def multi_graph_example():
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repo = await db.get_repository("my-repo")
        
        # Add data to specific graphs
        statements = [
            Quad(
                IRI("http://example.com/person/bob"),
                IRI("http://xmlns.com/foaf/0.1/name"),
                Literal("Bob"),
                IRI("http://example.com/graph/people")
            ),
            Quad(
                IRI("http://example.com/person/bob"),
                IRI("http://xmlns.com/foaf/0.1/age"),
                Literal("30", datatype=IRI("http://www.w3.org/2001/XMLSchema#integer")),
                IRI("http://example.com/graph/demographics")
            )
        ]
        await repo.add_statements(statements)
        
        # Query specific graph
        graph_query = """
        SELECT * WHERE {
            GRAPH <http://example.com/graph/people> {
                ?person ?property ?value
            }
        }
        """
        results = await repo.query(graph_query)
```

### Advanced Repository Configuration

Here's a more comprehensive example showing repository creation with different configurations:

```python
async def advanced_example():
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        # Memory store with persistence
        persistent_config = RepositoryConfig(
            repo_id="persistent-repo",
            title="Persistent Memory Store",
            impl=SailRepositoryConfig(sail_impl=MemoryStoreConfig(persist=True))
        )
        
        # Create and populate repository
        repo = await db.create_repository(config=persistent_config)
        
        # Bulk data operations
        data = [
            (IRI("http://example.com/alice"), IRI("http://xmlns.com/foaf/0.1/name"), Literal("Alice")),
            (IRI("http://example.com/alice"), IRI("http://xmlns.com/foaf/0.1/email"), Literal("alice@example.com")),
            (IRI("http://example.com/bob"), IRI("http://xmlns.com/foaf/0.1/name"), Literal("Bob")),
        ]
        
        statements = [
            Quad(subj, pred, obj, IRI("http://example.com/default"))
            for subj, pred, obj in data
        ]
        await repo.add_statements(statements)
        
        # Complex SPARQL query
        query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?name ?email WHERE {
            ?person foaf:name ?name .
            OPTIONAL { ?person foaf:email ?email }
        }
        ORDER BY ?name
        """
        results = await repo.query(query)
```

For more detailed examples, see the [examples](examples/) directory.

## Development

### Setting up Development Environment

1. **Clone the repository**:
   ```bash
   git clone https://github.com/odysa/rdf4j-python.git
   cd rdf4j-python
   ```

2. **Install development dependencies**:
   ```bash
   uv sync --group dev
   ```

3. **Start RDF4J Server** (for integration tests):
   ```bash
   # Using Docker
   docker run -p 19780:8080 eclipse/rdf4j:latest
   ```

4. **Run tests**:
   ```bash
   pytest tests/
   ```

5. **Run linting**:
   ```bash
   ruff check .
   ruff format .
   ```

### Project Structure

```
rdf4j_python/
├── _driver/          # Core async driver implementation
├── model/            # Data models and configurations
├── exception/        # Custom exceptions
└── utils/           # Utility functions

examples/            # Usage examples
tests/              # Test suite
docs/               # Documentation
```

### Contributing

We welcome contributions! Here's how to get involved:

1. **Fork** the repository on GitHub
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Make** your changes and add tests
4. **Run** the test suite to ensure everything works
5. **Commit** your changes (`git commit -m 'Add amazing feature'`)
6. **Push** to your branch (`git push origin feature/amazing-feature`)
7. **Open** a Pull Request

### Running Examples

```bash
# Make sure RDF4J server is running on localhost:19780
python examples/complete_workflow.py
python examples/query.py
```

## License

This project is licensed under the **BSD 3-Clause License**. See the [LICENSE](LICENSE) file for the full license text.

```
Copyright (c) 2025, Chengxu Bian
All rights reserved.
```

---

**Questions or Issues?** Please feel free to [open an issue](https://github.com/odysa/rdf4j-python/issues) on GitHub.

**⭐ Star this repo** if you find it useful!
