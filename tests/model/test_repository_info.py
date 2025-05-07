import pytest
import rdflib
from rdflib.term import Literal, URIRef, Variable

from rdf4j_python.model.repository import RepositoryInfo


@pytest.fixture
def valid_result():
    result = rdflib.query.Result("SELECT")
    result._bindings = [
        {
            Variable("id"): Literal("example-repo" + str(i)),
            Variable("uri"): URIRef(
                "http://localhost:19780/rdf4j-server/repositories/example-repo" + str(i)
            ),
            Variable("readable"): Literal(
                "true", datatype=URIRef("http://www.w3.org/2001/XMLSchema#boolean")
            ),
            Variable("writable"): Literal(
                "false", datatype=URIRef("http://www.w3.org/2001/XMLSchema#boolean")
            ),
        }
        for i in range(10)
    ]
    return result


@pytest.fixture
def partial_result():
    result = rdflib.query.Result("SELECT")
    result._bindings = [
        {
            Variable("id"): Literal("partial-repo"),
        }
    ]
    return result


def test_from_rdflib_valid(valid_result: rdflib.query.Result):
    for binding in valid_result.bindings:
        repo = RepositoryInfo.from_rdflib_binding(binding)
        assert repo.id == "example-repo" + str(binding[Variable("id")].value[-1])
        assert repo.title == "example-repo" + str(binding[Variable("id")].value[-1])
        assert (
            repo.uri
            == "http://localhost:19780/rdf4j-server/repositories/example-repo"
            + str(binding[Variable("id")].value[-1])
        )
        assert repo.readable is True
        assert repo.writable is False


def test_from_rdflib_partial(partial_result: rdflib.query.Result):
    repo = RepositoryInfo.from_rdflib_binding(partial_result.bindings[0])
    assert repo.id == "partial-repo"
    assert repo.title == "partial-repo"
    assert repo.uri == ""
    assert repo.readable is False
    assert repo.writable is False


def test_str_representation(valid_result: rdflib.query.Result):
    for binding in valid_result.bindings:
        repo = RepositoryInfo.from_rdflib_binding(binding)
        assert (
            str(repo) == f"Repository(id={repo.id}, title={repo.title}, uri={repo.uri})"
        )
