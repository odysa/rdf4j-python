from dataclasses import dataclass
from typing import Mapping

from rdflib.term import Identifier, Variable

from ._base_model import _BaseModel


@dataclass
class Repository(_BaseModel):
    """
    Represents a repository in RDF4J.
    """

    id: str  # The repository identifier
    uri: str  # The full URI to the repository
    title: str  # A human-readable title (currently reusing id)
    readable: bool  # Whether the repository is readable
    writable: bool  # Whether the repository is writable

    def __str__(self):
        # Custom string representation for easy printing
        return f"Repository(id={self.id}, title={self.title}, uri={self.uri})"

    @classmethod
    def from_rdflib(cls, result: Mapping[Variable, Identifier]) -> "Repository":
        """
        Create a Repository instance from a SPARQL query result
        represented as a Mapping from rdflib Variables to Identifiers.
        """

        # Construct and return the Repository object
        return cls(
            id=super().get_literal(result, "id", ""),
            uri=super().get_uri(result, "uri", ""),
            title=super().get_literal(result, "id", ""),
            readable=super().get_literal(result, "readable", False),
            writable=super().get_literal(result, "writable", False),
        )
