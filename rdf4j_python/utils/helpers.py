from io import BytesIO
from typing import Iterable

import pyoxigraph as og

from rdf4j_python.model.term import Quad, Triple


def serialize_statements(statements: Iterable[Quad] | Iterable[Triple]) -> bytes:
    """Serializes statements to RDF data.

    Args:
        statements (Iterable[Quad] | Iterable[Triple]): RDF statements.

    Returns:
        bytes: Serialized RDF data.
    """
    io = BytesIO()
    og.serialize(statements, output=io, format=og.RdfFormat.N_QUADS)
    return io.getvalue()
