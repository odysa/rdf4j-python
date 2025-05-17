from typing import Mapping

from rdflib.namespace import Namespace as RdflibNamespace
from rdflib.term import Identifier, Variable

from rdf4j_python.model._term import IRI

from ._base_model import _BaseModel


class Namespace:
    """
    Represents an RDF namespace with a prefix, wrapping RDFLib's Namespace.

    Provides utility methods for accessing terms as IRIs and working with SPARQL bindings.
    """

    _prefix: str
    _namespace: RdflibNamespace

    def __init__(self, prefix: str, namespace: str):
        """
        Initializes a Namespace instance.

        Args:
            prefix (str): The namespace prefix (e.g., "ex").
            namespace (str): The full namespace URI (e.g., "http://example.org/").
        """
        self._prefix = prefix
        self._namespace = RdflibNamespace(namespace)

    @classmethod
    def from_rdflib_binding(cls, binding: Mapping[Variable, Identifier]) -> "Namespace":
        """
        Constructs a Namespace instance from a SPARQL result binding.

        Args:
            binding (Mapping[Variable, Identifier]): A mapping of variables from a SPARQL query result.

        Returns:
            Namespace: The resulting Namespace instance.
        """
        prefix = _BaseModel.get_literal(binding, "prefix", "")
        namespace = _BaseModel.get_literal(binding, "namespace", "")
        return cls(prefix=prefix, namespace=namespace)

    def __str__(self) -> str:
        """
        Returns a human-readable string representation.

        Returns:
            str: The string representation (e.g., "ex: http://example.org/").
        """
        return f"{self._prefix}: {self._namespace}"

    def __repr__(self) -> str:
        """
        Returns a detailed string representation for debugging.

        Returns:
            str: The formal representation of the object.
        """
        return f"Namespace(prefix={self._prefix}, namespace={self._namespace})"

    def __contains__(self, item: str) -> bool:
        """
        Checks whether a term exists in the namespace.

        Args:
            item (str): The term to check.

        Returns:
            bool: True if the term exists, False otherwise.
        """
        return item in self._namespace

    def term(self, name: str) -> IRI:
        """
        Returns the IRI corresponding to a given term name.

        Args:
            name (str): The local name of the term.

        Returns:
            IRI: The full IRI for the term.
        """
        return IRI(self._namespace.term(name))

    def __getitem__(self, item: str) -> IRI:
        """
        Enables dictionary-style access to terms.

        Args:
            item (str): The term name.

        Returns:
            IRI: The IRI for the given term.
        """
        return self.term(item)

    def __getattr__(self, item: str) -> IRI:
        """
        Enables dot-access to terms (e.g., ns.person).

        Args:
            item (str): The term name.

        Returns:
            IRI: The IRI for the given term.

        Raises:
            AttributeError: If the attribute is special (starts with "__").
        """
        if item.startswith("__"):
            raise AttributeError
        return self.term(item)

    @property
    def namespace(self) -> IRI:
        """
        Returns the full namespace as an IRI.

        Returns:
            IRI: The namespace URI.
        """
        return IRI(self._namespace)

    @property
    def prefix(self) -> str:
        """
        Returns the prefix of the namespace.

        Returns:
            str: The namespace prefix.
        """
        return self._prefix
