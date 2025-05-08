from abc import ABC
from typing import Mapping, Optional

from rdflib.term import Identifier, Literal, URIRef, Variable


class _BaseModel(ABC):
    @staticmethod
    def get_literal(
        result: Mapping[Variable, Identifier],
        var_name: str,
        default: Optional[str] = None,
    ):
        """Extract and convert an RDFLib Literal to a Python value."""
        val = result.get(Variable(var_name))
        return val.toPython() if isinstance(val, Literal) else default

    @staticmethod
    def get_uri(
        result: Mapping[Variable, Identifier],
        var_name: str,
        default: Optional[str] = None,
    ):
        """Extract and convert an RDFLib URIRef to a string."""
        val = result.get(Variable(var_name))
        return str(val) if isinstance(val, URIRef) else default
