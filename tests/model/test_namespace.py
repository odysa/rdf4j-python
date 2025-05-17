import pytest
from rdflib import Literal, Variable

from rdf4j_python.model import Namespace
from rdf4j_python.model.term import IRI


def test_namespace():
    namespace = Namespace("ex", "http://example.org/")
    assert namespace.term("foo") == IRI("http://example.org/foo")


def test_namespace_contains():
    namespace = Namespace("ex", "http://example.org/")

    assert "foo" in namespace.term("foo")
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


def test_namespace_from_rdflib_binding():
    binding = {
        Variable("prefix"): Literal("ex"),
        Variable("namespace"): Literal("http://example.org/"),
    }
    namespace = Namespace.from_rdflib_binding(binding)
    assert namespace.prefix == "ex"
    assert namespace.namespace == IRI("http://example.org/")
