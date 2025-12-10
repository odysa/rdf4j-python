import tempfile
from pathlib import Path

import pyoxigraph as og
import pytest
from pyoxigraph import QuerySolutions

from rdf4j_python import AsyncRdf4JRepository
from rdf4j_python.model.term import IRI, Literal


@pytest.mark.asyncio
async def test_upload_turtle_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading a Turtle file to the repository."""
    # Create a temporary Turtle file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write("""
@prefix ex: <http://example.org/> .

ex:subject1 ex:predicate "test_object1" .
ex:subject2 ex:predicate "test_object2" .
""")
        temp_file = f.name

    try:
        # Upload the file
        await mem_repo.upload_file(temp_file)

        # Verify the data was uploaded
        result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o } ORDER BY ?s")
        assert isinstance(result, QuerySolutions)
        result_list = list(result)
        assert len(result_list) == 2
        assert result_list[0]["s"] == IRI("http://example.org/subject1")
        assert result_list[0]["p"] == IRI("http://example.org/predicate")
        assert result_list[0]["o"] == Literal("test_object1")
        assert result_list[1]["s"] == IRI("http://example.org/subject2")
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_ntriples_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an N-Triples file to the repository."""
    # Create a temporary N-Triples file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".nt", delete=False) as f:
        f.write("""<http://example.org/subject1> <http://example.org/predicate> "test_object1" .
<http://example.org/subject2> <http://example.org/predicate> "test_object2" .
""")
        temp_file = f.name

    try:
        # Upload the file
        await mem_repo.upload_file(temp_file)

        # Verify the data was uploaded
        result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o } ORDER BY ?s")
        assert isinstance(result, QuerySolutions)
        result_list = list(result)
        assert len(result_list) == 2
        assert result_list[0]["s"] == IRI("http://example.org/subject1")
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_nquads_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an N-Quads file to the repository."""
    # Create a temporary N-Quads file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".nq", delete=False) as f:
        f.write("""<http://example.org/subject1> <http://example.org/predicate> "test_object1" <http://example.org/graph1> .
<http://example.org/subject2> <http://example.org/predicate> "test_object2" <http://example.org/graph1> .
""")
        temp_file = f.name

    try:
        # Upload the file
        await mem_repo.upload_file(temp_file)

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
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_file_with_context(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with a specified context."""
    # Create a temporary Turtle file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write("""
@prefix ex: <http://example.org/> .

ex:subject1 ex:predicate "test_object1" .
""")
        temp_file = f.name

    try:
        # Upload the file to a specific context
        context = IRI("http://example.org/custom-graph")
        await mem_repo.upload_file(temp_file, context=context)

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
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_file_with_explicit_format(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with explicitly specified format."""
    # Create a temporary file without standard extension
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("""<http://example.org/subject1> <http://example.org/predicate> "test_object1" .
""")
        temp_file = f.name

    try:
        # Upload the file with explicit format
        await mem_repo.upload_file(temp_file, rdf_format=og.RdfFormat.N_TRIPLES)

        # Verify the data was uploaded
        result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
        assert isinstance(result, QuerySolutions)
        result_list = list(result)
        assert len(result_list) == 1
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_file_with_base_uri(mem_repo: AsyncRdf4JRepository):
    """Test uploading a file with a specified base URI."""
    # Create a temporary Turtle file with relative URIs
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write("""
<subject1> <predicate> "test_object1" .
""")
        temp_file = f.name

    try:
        # Upload the file with a base URI
        base_uri = "http://example.org/"
        await mem_repo.upload_file(temp_file, base_uri=base_uri)

        # Verify the relative URI was resolved with the base URI
        result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
        assert isinstance(result, QuerySolutions)
        result_list = list(result)
        assert len(result_list) == 1
        assert result_list[0]["s"] == IRI("http://example.org/subject1")
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_nonexistent_file(mem_repo: AsyncRdf4JRepository):
    """Test that uploading a non-existent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        await mem_repo.upload_file("/nonexistent/path/file.ttl")


@pytest.mark.asyncio
async def test_upload_invalid_rdf_file(mem_repo: AsyncRdf4JRepository):
    """Test that uploading an invalid RDF file raises SyntaxError."""
    # Create a temporary file with invalid RDF content
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        f.write("This is not valid RDF content!")
        temp_file = f.name

    try:
        with pytest.raises(SyntaxError):
            await mem_repo.upload_file(temp_file)
    finally:
        # Clean up
        Path(temp_file).unlink()


@pytest.mark.asyncio
async def test_upload_rdf_xml_file(mem_repo: AsyncRdf4JRepository):
    """Test uploading an RDF/XML file to the repository."""
    # Create a temporary RDF/XML file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".rdf", delete=False) as f:
        f.write("""<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/subject1">
    <ex:predicate>test_object1</ex:predicate>
  </rdf:Description>
</rdf:RDF>
""")
        temp_file = f.name

    try:
        # Upload the file
        await mem_repo.upload_file(temp_file)

        # Verify the data was uploaded
        result = await mem_repo.query("SELECT * WHERE { ?s ?p ?o }")
        assert isinstance(result, QuerySolutions)
        result_list = list(result)
        assert len(result_list) == 1
        assert result_list[0]["s"] == IRI("http://example.org/subject1")
    finally:
        # Clean up
        Path(temp_file).unlink()
