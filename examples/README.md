# RDF4J Python Examples

This directory contains comprehensive examples demonstrating how to use the `rdf4j-python` library to interact with RDF4J repositories. These examples cover the full lifecycle of repository management, data manipulation, and querying.

## Prerequisites

1. **RDF4J Server**: You need a running RDF4J server. The examples assume it's running on `http://localhost:19780/rdf4j-server`.

   To start an RDF4J server using Docker:
   ```bash
   docker run -p 19780:8080 eclipse/rdf4j-workbench:latest
   ```

2. **Python Dependencies**: Install the required dependencies:
   ```bash
   pip install rdf4j-python
   ```

## Examples Overview

### 📁 Repository Management Examples

#### `create_repository.py`
Demonstrates how to create different types of RDF4J repositories:
- In-memory repositories (non-persistent)
- Persistent memory repositories 
- Custom configuration options

**Key Features:**
- Multiple repository configurations
- Error handling
- Clear output formatting

**Usage:**
```bash
python examples/create_repository.py
```

#### `list_repositories.py`
Shows how to list and analyze repositories on an RDF4J server:
- List all repositories with metadata
- Filter repositories by access permissions
- Search repositories by name patterns
- Display repository statistics

**Key Features:**
- Repository metadata display
- Filtering and searching capabilities
- Summary statistics

**Usage:**
```bash
python examples/list_repositories.py
```

#### `delete_repository.py`
Demonstrates safe repository deletion:
- Delete repositories by ID
- Delete repositories matching patterns
- Safe deletion with confirmation
- Cleanup of test repositories

**Key Features:**
- Safety checks before deletion
- Pattern-based deletion
- Detailed repository information before deletion
- Automatic test repository cleanup

**Usage:**
```bash
python examples/delete_repository.py
```

### 🔍 Data Querying Examples

#### `query_and_print.py`
Comprehensive example of SPARQL querying and result formatting:
- SELECT queries with various clauses (FILTER, OPTIONAL, JOIN)
- CONSTRUCT queries for data transformation
- ASK queries for boolean checks
- Aggregate queries (COUNT, AVG, MIN, MAX)
- Multiple result formatting options

**Key Features:**
- Automatic test data setup
- Formatted table output
- JSON-like result formatting
- Different query types demonstration

**Usage:**
```bash
python examples/query_and_print.py
```

#### `query.py` (Original)
Simple query example showing basic SPARQL execution.

**Usage:**
```bash
python examples/query.py
```

### 🔄 Complete Workflow Example

#### `complete_workflow.py`
End-to-end example demonstrating the full repository lifecycle:
1. **Repository Creation**: Create multiple repositories for different purposes
2. **Data Addition**: Add structured data using quads with named graphs
3. **Data Querying**: Execute various SPARQL queries
4. **Repository Listing**: Analyze repository metadata
5. **Cleanup**: Safe deletion of created repositories

**Key Features:**
- Multi-repository workflow
- Real-world data scenarios (customers, products, analytics)
- Named graph usage
- Comprehensive error handling and cleanup

**Usage:**
```bash
python examples/complete_workflow.py
```

#### `repo.py` (Original)
Basic repository creation and data insertion example.

**Usage:**
```bash
python examples/repo.py
```

## Running the Examples

### Option 1: Run Individual Examples
```bash
# Start with repository creation
python examples/create_repository.py

# List the created repositories
python examples/list_repositories.py

# Query data (creates its own test repository)
python examples/query_and_print.py

# Clean up repositories
python examples/delete_repository.py
```

### Option 2: Run the Complete Workflow
```bash
# This runs the entire lifecycle in one go
python examples/complete_workflow.py
```

### Option 3: Start Docker and Run Examples
```bash
# Start RDF4J server
docker run -d -p 19780:8080 --name rdf4j eclipse/rdf4j-workbench:latest

# Wait a moment for the server to start, then run examples
python examples/complete_workflow.py

# Stop and remove the container when done
docker stop rdf4j && docker rm rdf4j
```

## Example Output

### Repository Creation
```
🚀 RDF4J Repository Creation Examples
==================================================
Creating in-memory repository...
✅ Created repository: memory-example
   Title: In-Memory Example Repository
   Type: In-memory (non-persistent)

Creating persistent memory repository...
✅ Created repository: persistent-memory-example
   Title: Persistent Memory Example Repository
   Type: Memory with persistence
```

### Repository Listing
```
📋 Listing all repositories...
Found 2 repository(ies):
================================================================================
1. Repository Details:
   ID: memory-example
   Title: In-Memory Example Repository
   URI: http://localhost:19780/rdf4j-server/repositories/memory-example
   Readable: ✅
   Writable: ✅
```

### Query Results
```
📊 All People and Their Names
============================================================
person          | name           
----------------------------------------
http://example.org/person/alice | Alice          
http://example.org/person/bob   | Bob            
http://example.org/person/charlie | Charlie        

Total results: 3
```

## Error Handling

All examples include comprehensive error handling:
- Connection errors to RDF4J server
- Repository creation/deletion failures
- Query execution errors
- Automatic cleanup on errors

## Tips for Usage

1. **Start Simple**: Begin with `create_repository.py` and `list_repositories.py`
2. **Test Queries**: Use `query_and_print.py` to understand SPARQL querying
3. **Complete Example**: Run `complete_workflow.py` to see everything together
4. **Cleanup**: Always clean up test repositories using `delete_repository.py`

## Customization

These examples can be easily customized:
- **Server URL**: Change the RDF4J server URL in each example
- **Repository Configuration**: Modify repository settings in creation examples
- **Data Model**: Adapt the data structures in query examples
- **Query Patterns**: Extend the SPARQL queries for your use case

## Support

For more information about the RDF4J Python library:
- [GitHub Repository](https://github.com/odysa/rdf4j-python)
- [RDF4J Documentation](https://rdf4j.org/documentation/)
- [SPARQL Specification](https://www.w3.org/TR/sparql11-query/)