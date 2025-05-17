from typing import Mapping

from rdflib.namespace import Namespace as RdflibNamespace
from rdflib.term import Identifier, Variable

from rdf4j_python.model.term import IRI

from ._base_model import _BaseModel


class Namespace:
    """
    Represents a namespace in RDF4J.
    """

    _prefix: str
    _namespace: RdflibNamespace

    def __init__(self, prefix: str, namespace: str):
        """
        Initializes a new Namespace.

        Args:
            prefix (str): The prefix of the namespace.
            namespace (str): The namespace URI.
        """
        self._prefix = prefix
        self._namespace = RdflibNamespace(namespace)

    @classmethod
    def from_rdflib_binding(cls, binding: Mapping[Variable, Identifier]) -> "Namespace":
        """
        Creates a Namespace from a RDFlib binding.

        Args:
            binding (Mapping[Variable, Identifier]): The RDFlib binding.

        Returns:
            Namespace: The created Namespace.
        """
        prefix = _BaseModel.get_literal(binding, "prefix", "")
        namespace = _BaseModel.get_literal(binding, "namespace", "")
        return cls(
            prefix=prefix,
            namespace=namespace,
        )

    def __str__(self):
        """
        Returns a string representation of the Namespace.

        Returns:
            str: A string representation of the Namespace.
        """
        return f"{self._prefix}: {self._namespace}"

    def __repr__(self):
        """
        Returns a string representation of the Namespace.

        Returns:
            str: A string representation of the Namespace.
        """
        return f"Namespace(prefix={self._prefix}, namespace={self._namespace})"

    def __contains__(self, item: str) -> bool:
        """
        Checks if the Namespace contains a given item.

        Args:
            item (str): The item to check.

        Returns:
            bool: True if the Namespace contains the item, False otherwise.
        """
        return item in self._namespace

    def term(self, name: str) -> IRI:
        """
        Returns the IRI for a given term.

        Args:
            name (str): The term name.

        Returns:
            IRI: The IRI for the term.
        """
        return IRI(self._namespace.term(name))

    def __getitem__(self, item: str) -> IRI:
        """
        Returns the IRI for a given term.

        Args:
            item (str): The term name.

        Returns:
            IRI: The IRI for the term.
        """
        return self.term(item)

    def __getattr__(self, item: str) -> IRI:
        """
        Returns the IRI for a given term.

        Args:
            item (str): The term name.

        Returns:
            IRI: The IRI for the term.
        """
        if item.startswith("__"):
            raise AttributeError
        return self.term(item)

    @property
    def namespace(self) -> IRI:
        """
        Returns the namespace URI.

        Returns:
            IRI: The namespace URI.
        """
        return IRI(self._namespace)

    @property
    def prefix(self) -> str:
        """
        Returns the prefix of the namespace.

        Returns:
            str: The prefix of the namespace.
        """
        return self._prefix
