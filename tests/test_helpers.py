from rdf4j_python.model.term import IRI, Triple
from rdf4j_python.utils.helpers import serialize_statements


def test_serialize_statements():
    statements = [
        Triple(
            IRI("http://example.com/s"),
            IRI("http://example.com/p"),
            IRI("http://example.com/o"),
        ),
        Triple(
            IRI("http://example.com/s2"),
            IRI("http://example.com/p2"),
            IRI("http://example.com/o2"),
        ),
    ]
    assert (
        serialize_statements(statements)
        == b"<http://example.com/s> <http://example.com/p> <http://example.com/o> .\n<http://example.com/s2> <http://example.com/p2> <http://example.com/o2> .\n"
    )
