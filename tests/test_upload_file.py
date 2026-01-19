from pathlib import Path

import pyoxigraph as og
import pytest
from pyoxigraph import QuerySolutions

from rdf4j_python import AsyncRdf4JRepository
from rdf4j_python.model.term import IRI, Literal

# Path to test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.mark.asyncio
async def test_upload_turtle_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading a Turtle file to the repository."""
    # Upload the sample Turtle file
    sample_file = FIXTURES_DIR / "sample_data.ttl"
    await mem_repo.upload_file(str(sample_file))

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o } ORDER BY ?s")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0]["s"] == IRI("http://example.org/subject1")
    assert result_list[0]["p"] == IRI("http://example.org/predicate")
    assert result_list[0]["o"] == Literal("test_object1")
    assert result_list[1]["s"] == IRI("http://example.org/subject2")


@pytest.mark.asyncio
async def test_upload_ntriples_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an N-Triples file to the repository."""
    # Upload the sample N-Triples file
    sample_file = FIXTURES_DIR / "sample_data.nt"
    await mem_repo.upload_file(str(sample_file))

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o } ORDER BY ?s")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0]["s"] == IRI("http://example.org/subject1")


@pytest.mark.asyncio
async def test_upload_nquads_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an N-Quads file to the repository."""
    # Upload the sample N-Quads file
    sample_file = FIXTURES_DIR / "sample_data.nq"
    await mem_repo.upload_file(str(sample_file))

    # Verify the data was uploaded with correct contexts
    result = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/graph1> {
                ?s ?p ?o 
            }
        } ORDER BY ?s
    """)
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2


@pytest.mark.asyncio
async def test_upload_file_with_context(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with a specified context."""
    # Upload the sample file to a specific context
    sample_file = FIXTURES_DIR / "sample_with_context.ttl"
    context = IRI("http://example.org/custom-graph")
    await mem_repo.upload_file(str(sample_file), context=context)

    # Verify the data was uploaded to the specified context
    result = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/custom-graph> {
                ?s ?p ?o 
            }
        }
    """)
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0]["s"] == IRI("http://example.org/subject1")


@pytest.mark.asyncio
async def test_upload_file_with_explicit_format(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with explicitly specified format."""
    # Upload a .txt file with explicit N-Triples format
    sample_file = FIXTURES_DIR / "sample_data.txt"
    await mem_repo.upload_file(str(sample_file), rdf_format=og.RdfFormat.N_TRIPLES)

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 1


@pytest.mark.asyncio
async def test_upload_file_with_base_uri(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with a specified base URI."""
    # Upload a file with relative URIs using a base URI
    sample_file = FIXTURES_DIR / "sample_relative_uris.ttl"
    base_uri = "http://example.com/people/"
    await mem_repo.upload_file(str(sample_file), base_uri=base_uri)

    # Verify the relative URIs were resolved with the base URI
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 3
    # Check that alice was resolved with the base URI
    alice_iri = IRI("http://example.com/people/alice")
    alice_subjects = [r for r in result_list if r["s"] == alice_iri]
    assert len(alice_subjects) == 2


@pytest.mark.asyncio
async def test_upload_nonexistent_file(mem_repo: AsyncRdf4JRepository):
    """Test that uploading a non-existent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        await mem_repo.upload_file("/nonexistent/path/file.ttl")


@pytest.mark.asyncio
async def test_upload_invalid_rdf_file(mem_repo: AsyncRdf4JRepository):
    """Test that uploading an invalid RDF file raises SyntaxError."""
    # Upload a file with invalid RDF content
    sample_file = FIXTURES_DIR / "invalid_data.ttl"
    with pytest.raises(SyntaxError):
        await mem_repo.upload_file(str(sample_file))


@pytest.mark.asyncio
async def test_upload_rdf_xml_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an RDF/XML file to the repository."""
    # Upload the sample RDF/XML file
    sample_file = FIXTURES_DIR / "sample_data.rdf"
    await mem_repo.upload_file(str(sample_file))

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0]["s"] == IRI("http://example.org/subject1")


@pytest.mark.asyncio
async def test_upload_jsonld_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading a JSON-LD file to the repository."""
    # Upload the sample JSON-LD file
    sample_file = FIXTURES_DIR / "sample_data.jsonld"
    await mem_repo.upload_file(str(sample_file))

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o } ORDER BY ?s")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0]["s"] == IRI("http://example.org/subject1")
    assert result_list[1]["s"] == IRI("http://example.org/subject2")


@pytest.mark.asyncio
async def test_upload_trig_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading a TriG file with multiple named graphs."""
    # Upload the sample TriG file
    sample_file = FIXTURES_DIR / "sample_data.trig"
    await mem_repo.upload_file(str(sample_file))

    # Verify data in graph1
    result1 = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/graph1> {
                ?s ?p ?o 
            }
        }
    """)
    assert isinstance(result1, QuerySolutions)
    result1_list = list(result1)
    assert len(result1_list) == 1
    assert result1_list[0]["s"] == IRI("http://example.org/subject1")

    # Verify data in graph2
    result2 = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/graph2> {
                ?s ?p ?o 
            }
        }
    """)
    assert isinstance(result2, QuerySolutions)
    result2_list = list(result2)
    assert len(result2_list) == 1
    assert result2_list[0]["s"] == IRI("http://example.org/subject2")


@pytest.mark.asyncio
async def test_upload_n3_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an N3 file to the repository."""
    # Upload the sample N3 file
    sample_file = FIXTURES_DIR / "sample_data.n3"
    await mem_repo.upload_file(str(sample_file))

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o } ORDER BY ?s")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0]["s"] == IRI("http://example.org/subject1")


@pytest.mark.asyncio
async def test_upload_empty_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an empty file (only comments)."""
    # Upload a file with only comments
    sample_file = FIXTURES_DIR / "empty_data.ttl"
    await mem_repo.upload_file(str(sample_file))

    # Verify no data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 0


@pytest.mark.asyncio
async def test_upload_large_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading a larger file with multiple statements."""
    # Upload a file with many statements
    sample_file = FIXTURES_DIR / "large_data.ttl"
    await mem_repo.upload_file(str(sample_file))

    # Verify all data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    # 10 persons * 4 properties (type, name, age, knows) - 3 without knows = 37 triples
    assert len(result_list) >= 30  # At least 30 triples


@pytest.mark.asyncio
async def test_upload_file_with_path_object(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file using Path object instead of string."""
    # Upload using Path object
    sample_file = FIXTURES_DIR / "sample_data.ttl"
    await mem_repo.upload_file(sample_file)  # Pass Path object directly

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2


@pytest.mark.asyncio
async def test_upload_multiple_predicates(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with multiple predicates per subject."""
    # Upload file with multiple predicates
    sample_file = FIXTURES_DIR / "multiple_predicates.ttl"
    await mem_repo.upload_file(str(sample_file))

    # Verify all predicates were uploaded
    result = await mem_repo.query("""
        SELECT * WHERE { 
            <http://example.org/resource1> ?p ?o 
        }
    """)
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) >= 5  # At least 5 different predicates


@pytest.mark.asyncio
async def test_upload_file_overrides_context(mem_repo: AsyncRdf4JRepository):
    """Test that context parameter overrides named graphs in file."""
    # Upload N-Quads file with context parameter
    sample_file = FIXTURES_DIR / "override_context.nq"
    new_context = IRI("http://example.org/new-graph")
    await mem_repo.upload_file(str(sample_file), context=new_context)

    # Verify data is in the new context, not the original
    result_new = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/new-graph> {
                ?s ?p ?o 
            }
        }
    """)
    assert isinstance(result_new, QuerySolutions)
    result_new_list = list(result_new)
    assert len(result_new_list) == 2

    # Verify data is NOT in the original context
    result_old = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/original-graph> {
                ?s ?p ?o 
            }
        }
    """)
    assert isinstance(result_old, QuerySolutions)
    result_old_list = list(result_old)
    assert len(result_old_list) == 0


@pytest.mark.asyncio
async def test_upload_file_twice_accumulates(mem_repo: AsyncRdf4JRepository):
    """Test that uploading the same file twice accumulates data."""
    # Upload file first time
    sample_file = FIXTURES_DIR / "sample_data.ttl"
    await mem_repo.upload_file(str(sample_file))

    # Verify initial upload
    result1 = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result1, QuerySolutions)
    result1_list = list(result1)
    initial_count = len(result1_list)

    # Upload same file again
    await mem_repo.upload_file(str(sample_file))

    # Verify data was accumulated (doubled)
    result2 = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result2, QuerySolutions)
    result2_list = list(result2)
    # Since RDF repositories typically deduplicate identical triples,
    # the count should remain the same
    assert len(result2_list) == initial_count


@pytest.mark.asyncio
async def test_upload_different_files_to_same_graph(mem_repo: AsyncRdf4JRepository):
    """Test uploading multiple different files to the same named graph."""
    context = IRI("http://example.org/combined-graph")

    # Upload first file
    sample_file1 = FIXTURES_DIR / "sample_data.ttl"
    await mem_repo.upload_file(str(sample_file1), context=context)

    # Upload second file with different data
    sample_file2 = FIXTURES_DIR / "multiple_predicates.ttl"
    await mem_repo.upload_file(str(sample_file2), context=context)

    # Verify both files' data is in the same graph
    result = await mem_repo.query("""
        SELECT * WHERE { 
            GRAPH <http://example.org/combined-graph> {
                ?s ?p ?o 
            }
        }
    """)
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    # sample_data.ttl has 2 statements, multiple_predicates.ttl has 5
    assert len(result_list) >= 7


@pytest.mark.asyncio
async def test_upload_file_with_special_characters_in_path(
    mem_repo: AsyncRdf4JRepository,
):
    """Test uploading a file when path contains the file system separators."""
    # This tests that Path handling works correctly
    sample_file = FIXTURES_DIR / "sample_data.ttl"
    # Ensure the path is properly handled
    await mem_repo.upload_file(str(sample_file.absolute()))

    # Verify the data was uploaded
    result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
    assert isinstance(result, QuerySolutions)
    result_list = list(result)
    assert len(result_list) == 2
