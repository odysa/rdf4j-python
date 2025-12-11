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
