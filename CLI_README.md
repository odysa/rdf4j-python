# RDF4J Python CLI

Interactive command-line interface for connecting to RDF4J repositories and executing SPARQL queries with table-formatted results.

## Features

- **Interactive Console**: Command-line interface with prompt
- **Repository Connection**: Connect to RDF4J servers using `\c` command
- **SPARQL Queries**: Execute SELECT, CONSTRUCT, and ASK queries
- **Table Display**: Results displayed in formatted tables
- **Query Types Support**:
  - SELECT queries → Tabular results
  - CONSTRUCT queries → Triple listings
  - ASK queries → Boolean results

## Installation

The CLI is included with the rdf4j-python package:

```bash
pip install rdf4j-python
```

## Usage

### Starting the CLI

```bash
# Run directly
python -m rdf4j_python.cli

# Or if installed with console script
rdf4j-cli
```

### Available Commands

- `\c <server_url> <repo_id>` - Connect to a repository
- `\h` - Show help
- `\q` - Quit the CLI
- `<SPARQL_query>` - Execute a SPARQL query (after connecting)

### Example Session

```
Welcome to RDF4J Python CLI
Type '\h' for help, '\q' to quit
Use '\c <server_url> <repo_id>' to connect to a repository

rdf4j> \c http://localhost:19780/rdf4j-server my-repo
✅ Connected to repository 'my-repo' at http://localhost:19780/rdf4j-server

rdf4j:my-repo> SELECT ?person ?name WHERE { ?person foaf:name ?name } LIMIT 5
Executing query: SELECT ?person ?name WHERE { ?person foaf:name ?nam...

📊 Query Results
============================================================
person                    | name    
---------------------------------
http://example.org/per... | Alice   
http://example.org/per... | Bob     
http://example.org/per... | Charlie 

Total results: 3

rdf4j:my-repo> ASK { ?person foaf:name "Alice" }
Executing query: ASK { ?person foaf:name "Alice" }

❓ ASK Query Result: ✅ True

rdf4j:my-repo> \q
Goodbye!
```

## Query Types

### SELECT Queries
Display results in tabular format with column headers:

```sparql
SELECT ?subject ?predicate ?object WHERE {
  ?subject ?predicate ?object
} LIMIT 10
```

### CONSTRUCT Queries
Display constructed triples:

```sparql
CONSTRUCT {
  ?person ex:hasName ?name
} WHERE {
  ?person foaf:name ?name
}
```

### ASK Queries
Display boolean results:

```sparql
ASK {
  ?person foaf:name "Alice"
}
```

## Prerequisites

- Python 3.10 or higher
- RDF4J Server running (for connections)
- Repository created in RDF4J Server

### Starting RDF4J Server

Using Docker:
```bash
docker run -p 19780:8080 eclipse/rdf4j:latest
```

Access RDF4J Workbench at: `http://localhost:19780/rdf4j-workbench`

## Error Handling

The CLI provides clear error messages for:
- Connection failures
- Invalid queries
- Repository not found
- Server unavailable

Example error handling:
```
rdf4j> SELECT invalid syntax
❌ Query execution failed: Invalid SPARQL syntax

rdf4j> \c http://invalid:9999/rdf4j-server test
❌ Failed to connect to repository: Connection refused
```

## Development

Run tests:
```bash
pytest tests/test_cli.py -v
```

Lint code:
```bash
ruff check rdf4j_python/cli.py
```