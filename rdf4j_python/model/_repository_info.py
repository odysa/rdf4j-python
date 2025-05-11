from dataclasses import dataclass
from typing import Mapping

from rdflib.term import Identifier, Variable

from ._base_model import _BaseModel


@dataclass
class RepositoryMetadata(_BaseModel):
    """
    Represents a repository metadata RDF4J.
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
    def from_rdflib_binding(
        cls, result: Mapping[Variable, Identifier]
    ) -> "RepositoryMetadata":
        """
        Create a Repository instance from a SPARQL query result
        represented as a Mapping from rdflib Variables to Identifiers.
        """

        # Construct and return the Repository object
        return cls(
            id=_BaseModel.get_literal(result, "id", ""),
            uri=_BaseModel.get_uri(result, "uri", ""),
            title=_BaseModel.get_literal(result, "title", ""),
            readable=_BaseModel.get_literal(result, "readable", False),
            writable=_BaseModel.get_literal(result, "writable", False),
        )
