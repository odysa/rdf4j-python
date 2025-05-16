from typing import Iterable

from rdf4j_python.model._term import RDFStatement


def serialize_statements(statements: Iterable[RDFStatement]):
    """Serializes statements to RDF data.

    Args:
        statements (Iterable[RDFStatement]): RDF statements.
    """
    lines = []
    for subj, pred, obj, ctx in statements:
        parts = [subj.n3(), pred.n3(), obj.n3()]
        if ctx:
            parts.append(ctx.n3())
        parts.append(".")
        lines.append(" ".join(parts))
    return "\n".join(lines) + "\n"
