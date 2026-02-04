"""Tests for fine-grained SPARQL query interfaces."""
import pytest
import pytest_asyncio
from pyoxigraph import QuerySolutions, QueryTriples

from rdf4j_python import AsyncRdf4JRepository, QueryTypeMismatchError
from rdf4j_python.model.term import IRI, Literal, Quad
from rdf4j_python.model.vocabulary import EXAMPLE as ex
from rdf4j_python.model.vocabulary import RDF, XSD


@pytest_asyncio.fixture
async def sample_repo(mem_repo: AsyncRdf4JRepository):
    """Repository with sample data for testing."""
    data = [
        Quad(ex["alice"], RDF.type, ex["Person"], ex["graph"]),
        Quad(ex["alice"], ex["name"], Literal("Alice"), ex["graph"]),
        Quad(ex["alice"], ex["age"], Literal("30", datatype=XSD.integer), ex["graph"]),
        Quad(ex["bob"], RDF.type, ex["Person"], ex["graph"]),
        Quad(ex["bob"], ex["name"], Literal("Bob"), ex["graph"]),
        Quad(ex["bob"], ex["age"], Literal("25", datatype=XSD.integer), ex["graph"]),
    ]
    await mem_repo.add_statements(data)
    return mem_repo


class TestSelectMethod:
    """Tests for the select() method."""

    @pytest.mark.asyncio
    async def test_select_basic(self, sample_repo):
        """Test basic SELECT query."""
        result = await sample_repo.select(
            "SELECT ?name WHERE { ?s <http://example.org/name> ?name }"
        )
        assert isinstance(result, QuerySolutions)
        names = {solution["name"].value for solution in result}
        assert names == {"Alice", "Bob"}

    @pytest.mark.asyncio
    async def test_select_with_bindings(self, sample_repo):
        """Test SELECT with variable bindings."""
        result = await sample_repo.select(
            "SELECT ?name WHERE { ?person <http://example.org/name> ?name }",
            bindings={"person": IRI("http://example.org/alice")},
        )
        assert isinstance(result, QuerySolutions)
        results_list = list(result)
        assert len(results_list) == 1
        assert results_list[0]["name"].value == "Alice"

    @pytest.mark.asyncio
    async def test_select_strict_mode_valid(self, sample_repo):
        """Test SELECT with strict=True and valid query."""
        result = await sample_repo.select("SELECT * WHERE { ?s ?p ?o }", strict=True)
        assert isinstance(result, QuerySolutions)

    @pytest.mark.asyncio
    async def test_select_strict_mode_invalid(self, sample_repo):
        """Test SELECT with strict=True and ASK query raises error."""
        with pytest.raises(QueryTypeMismatchError) as exc_info:
            await sample_repo.select("ASK { ?s ?p ?o }", strict=True)
        assert exc_info.value.expected == "SELECT"
        assert exc_info.value.actual == "ASK"

    @pytest.mark.asyncio
    async def test_select_with_prefix(self, sample_repo):
        """Test SELECT with PREFIX declarations."""
        result = await sample_repo.select(
            """
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?s ex:name ?name }
            """
        )
        assert isinstance(result, QuerySolutions)
        names = {solution["name"].value for solution in result}
        assert names == {"Alice", "Bob"}


class TestAskMethod:
    """Tests for the ask() method."""

    @pytest.mark.asyncio
    async def test_ask_true(self, sample_repo):
        """Test ASK query returning True."""
        result = await sample_repo.ask("ASK { ?s <http://example.org/name> 'Alice' }")
        assert result is True

    @pytest.mark.asyncio
    async def test_ask_false(self, sample_repo):
        """Test ASK query returning False."""
        result = await sample_repo.ask(
            "ASK { ?s <http://example.org/name> 'NonExistent' }"
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_ask_with_bindings(self, sample_repo):
        """Test ASK with variable bindings."""
        result = await sample_repo.ask(
            "ASK { ?person <http://example.org/name> ?name }",
            bindings={"person": IRI("http://example.org/alice")},
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_ask_strict_mode_valid(self, sample_repo):
        """Test ASK with strict=True and valid query."""
        result = await sample_repo.ask("ASK { ?s ?p ?o }", strict=True)
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_ask_strict_mode_invalid(self, sample_repo):
        """Test ASK with strict=True and SELECT query raises error."""
        with pytest.raises(QueryTypeMismatchError) as exc_info:
            await sample_repo.ask("SELECT * WHERE { ?s ?p ?o }", strict=True)
        assert exc_info.value.expected == "ASK"
        assert exc_info.value.actual == "SELECT"


class TestConstructMethod:
    """Tests for the construct() method."""

    @pytest.mark.asyncio
    async def test_construct_basic(self, sample_repo):
        """Test basic CONSTRUCT query."""
        result = await sample_repo.construct(
            """
            CONSTRUCT { ?s <http://example.org/hasName> ?name }
            WHERE { ?s <http://example.org/name> ?name }
        """
        )
        assert isinstance(result, QueryTriples)
        triples_list = list(result)
        assert len(triples_list) == 2

    @pytest.mark.asyncio
    async def test_construct_with_bindings(self, sample_repo):
        """Test CONSTRUCT with variable bindings."""
        result = await sample_repo.construct(
            """
            CONSTRUCT { ?person <http://example.org/displayName> ?name }
            WHERE { ?person <http://example.org/name> ?name }
            """,
            bindings={"person": IRI("http://example.org/alice")},
        )
        triples_list = list(result)
        assert len(triples_list) == 1
        assert str(triples_list[0].subject) == "<http://example.org/alice>"

    @pytest.mark.asyncio
    async def test_construct_strict_mode_valid(self, sample_repo):
        """Test CONSTRUCT with strict=True and valid query."""
        result = await sample_repo.construct(
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", strict=True
        )
        assert isinstance(result, QueryTriples)

    @pytest.mark.asyncio
    async def test_construct_strict_mode_invalid(self, sample_repo):
        """Test CONSTRUCT with strict=True and SELECT query raises error."""
        with pytest.raises(QueryTypeMismatchError) as exc_info:
            await sample_repo.construct("SELECT * WHERE { ?s ?p ?o }", strict=True)
        assert exc_info.value.expected == "CONSTRUCT"
        assert exc_info.value.actual == "SELECT"


class TestDescribeMethod:
    """Tests for the describe() method."""

    @pytest.mark.asyncio
    async def test_describe_resource(self, sample_repo):
        """Test DESCRIBE for a specific resource."""
        result = await sample_repo.describe("DESCRIBE <http://example.org/alice>")
        assert isinstance(result, QueryTriples)
        triples_list = list(result)
        assert len(triples_list) > 0

    @pytest.mark.asyncio
    async def test_describe_with_where(self, sample_repo):
        """Test DESCRIBE with WHERE clause."""
        result = await sample_repo.describe(
            """
            DESCRIBE ?person WHERE {
                ?person <http://example.org/name> "Alice"
            }
        """
        )
        assert isinstance(result, QueryTriples)

    @pytest.mark.asyncio
    async def test_describe_strict_mode_valid(self, sample_repo):
        """Test DESCRIBE with strict=True and valid query."""
        result = await sample_repo.describe(
            "DESCRIBE <http://example.org/alice>", strict=True
        )
        assert isinstance(result, QueryTriples)

    @pytest.mark.asyncio
    async def test_describe_strict_mode_invalid(self, sample_repo):
        """Test DESCRIBE with strict=True and SELECT query raises error."""
        with pytest.raises(QueryTypeMismatchError) as exc_info:
            await sample_repo.describe("SELECT * WHERE { ?s ?p ?o }", strict=True)
        assert exc_info.value.expected == "DESCRIBE"
        assert exc_info.value.actual == "SELECT"


class TestQueryMethodWithBindings:
    """Tests for the generic query() method with bindings support."""

    @pytest.mark.asyncio
    async def test_query_select_with_bindings(self, sample_repo):
        """Test generic query() with SELECT and bindings."""
        result = await sample_repo.query(
            "SELECT ?name WHERE { ?person <http://example.org/name> ?name }",
            bindings={"person": IRI("http://example.org/alice")},
        )
        assert isinstance(result, QuerySolutions)
        results_list = list(result)
        assert len(results_list) == 1
        assert results_list[0]["name"].value == "Alice"

    @pytest.mark.asyncio
    async def test_query_ask_with_bindings(self, sample_repo):
        """Test generic query() with ASK and bindings."""
        from pyoxigraph import QueryBoolean

        result = await sample_repo.query(
            "ASK { ?person <http://example.org/name> ?name }",
            bindings={"person": IRI("http://example.org/alice")},
        )
        assert isinstance(result, QueryBoolean)
        assert bool(result) is True


class TestBindingsSerialization:
    """Tests for binding value serialization."""

    @pytest.mark.asyncio
    async def test_binding_with_iri(self, sample_repo):
        """Test binding with IRI value."""
        result = await sample_repo.select(
            "SELECT ?name WHERE { ?person <http://example.org/name> ?name }",
            bindings={"person": IRI("http://example.org/alice")},
        )
        results_list = list(result)
        assert len(results_list) == 1
        assert results_list[0]["name"].value == "Alice"

    @pytest.mark.asyncio
    async def test_binding_with_literal(self, sample_repo):
        """Test binding with literal value."""
        result = await sample_repo.select(
            "SELECT ?s WHERE { ?s <http://example.org/name> ?name }",
            bindings={"name": Literal("Alice")},
        )
        results_list = list(result)
        assert len(results_list) == 1
        assert str(results_list[0]["s"]) == "<http://example.org/alice>"

    @pytest.mark.asyncio
    async def test_binding_with_typed_literal(self, sample_repo):
        """Test binding with typed literal."""
        result = await sample_repo.select(
            "SELECT ?s WHERE { ?s <http://example.org/age> ?age }",
            bindings={"age": Literal("30", datatype=XSD.integer)},
        )
        results_list = list(result)
        assert len(results_list) == 1
        assert str(results_list[0]["s"]) == "<http://example.org/alice>"

    @pytest.mark.asyncio
    async def test_binding_variable_with_question_mark(self, sample_repo):
        """Test that variable names with ? prefix are handled correctly."""
        result = await sample_repo.select(
            "SELECT ?name WHERE { ?person <http://example.org/name> ?name }",
            bindings={"?person": IRI("http://example.org/alice")},  # with ? prefix
        )
        results_list = list(result)
        assert len(results_list) == 1
        assert results_list[0]["name"].value == "Alice"


class TestQueryTypeMismatchError:
    """Tests for QueryTypeMismatchError exception."""

    def test_error_attributes(self):
        """Test that error has expected attributes."""
        error = QueryTypeMismatchError("SELECT", "ASK", "ASK { ?s ?p ?o }")
        assert error.expected == "SELECT"
        assert error.actual == "ASK"
        assert error.query == "ASK { ?s ?p ?o }"

    def test_error_message(self):
        """Test error message format."""
        error = QueryTypeMismatchError("SELECT", "ASK", "ASK { ?s ?p ?o }")
        assert "Expected SELECT query but detected ASK" in str(error)

    def test_error_truncates_long_query(self):
        """Test that long queries are truncated in error message."""
        long_query = "SELECT * WHERE { " + "?s ?p ?o . " * 50 + "}"
        error = QueryTypeMismatchError("ASK", "SELECT", long_query)
        assert "..." in str(error)
