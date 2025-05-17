from abc import ABC
from typing import Mapping, Optional

from rdflib.term import Identifier, Literal, URIRef, Variable


class _BaseModel(ABC):
    """Abstract base class providing utility methods for parsing RDF query results."""

    @staticmethod
    def get_literal(
        result: Mapping[Variable, Identifier],
        var_name: str,
        default: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extracts a literal value from a SPARQL query result.

        Args:
            result (Mapping[Variable, Identifier]): A mapping of variable bindings from a query result.
            var_name (str): The variable name to extract.
            default (Optional[str], optional): The value to return if the variable is not found or is not a Literal. Defaults to None.

        Returns:
            Optional[str]: The Python representation of the literal, or the default value.
        """
        val = result.get(Variable(var_name))
        return val.toPython() if isinstance(val, Literal) else default

    @staticmethod
    def get_uri(
        result: Mapping[Variable, Identifier],
        var_name: str,
        default: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extracts a URI value from a SPARQL query result.

        Args:
            result (Mapping[Variable, Identifier]): A mapping of variable bindings from a query result.
            var_name (str): The variable name to extract.
            default (Optional[str], optional): The value to return if the variable is not found or is not a URIRef. Defaults to None.

        Returns:
            Optional[str]: The URI string, or the default value.
        """
        val = result.get(Variable(var_name))
        return str(val) if isinstance(val, URIRef) else default
