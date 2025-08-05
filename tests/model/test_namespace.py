import pyoxigraph as og
import pytest

from rdf4j_python.model import Namespace
from rdf4j_python.model.term import IRI
from tests.conftest import SparqlJsonResultBuilder


def test_namespace():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.term("foo") == IRI("http://example.org/foo")


def test_namespace_contains():
    namespace = Namespace("ex", "http://example.org/")

    assert "foo" in namespace.term("foo").value
    assert "bar" not in namespace


def test_namespace_term():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.term("foo") == IRI("http://example.org/foo")


def test_namespace_term_iri():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.term("foo") == IRI("http://example.org/foo")


def test_namespace_prefix():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.prefix == "ex"


def test_namespace_namespace():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.namespace == IRI("http://example.org/")


def test_namespace_getitem():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace["foo"] == IRI("http://example.org/foo")


def test_namespace_getattr():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.foo == IRI("http://example.org/foo")


def test_namespace_getattr_error():
    namespace = Namespace("ex", "http://example.org/")
    with pytest.raises(AttributeError):
        namespace.__foo


def test_namespace_from_sparql_query_solution():
    query_solution = og.parse_query_results(
        SparqlJsonResultBuilder()
        .add_variables(["prefix", "namespace"])
        .add_bindings(
            [
                {
                    "prefix": {"type": "literal", "value": "ex"},
                    "namespace": {"type": "literal", "value": "http://example.org/"},
                }
            ]
        )
        .to_json(),
        format=og.QueryResultsFormat.JSON,
    )
    assert isinstance(query_solution, og.QuerySolutions)
    namespace = Namespace.from_sparql_query_solution(next(query_solution))
    assert namespace.prefix == "ex"
    assert namespace.namespace.value == "http://example.org/"
