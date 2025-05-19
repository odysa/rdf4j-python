import pyoxigraph as og

from rdf4j_python.model.term import IRI


class _Namespace(str):
    def __new__(cls, value: str | bytes) -> "Namespace":
        try:
            rt = str.__new__(cls, value)
        except UnicodeDecodeError:
            rt = str.__new__(cls, value, "utf-8")
        return rt

    def term(self, name: str) -> IRI:
        return IRI(self + (name if isinstance(name, str) else ""))

    def __getitem__(self, key: str) -> IRI:
        return self.term(key)

    def __getattr__(self, name: str) -> IRI:
        if name.startswith("__"):
            raise AttributeError
        return self.term(name)

    def __repr__(self) -> str:
        return f"Namespace({super().__repr__()})"

    def __contains__(self, ref: str) -> bool:
        return ref.startswith(self)


class Namespace:
    """
    Represents a namespace in RDF4J.
    """

    _prefix: str
    _namespace: _Namespace

    def __init__(self, prefix: str, namespace: str):
        """
        Initializes a new Namespace.

        Args:
            prefix (str): The prefix of the namespace.
            namespace (str): The namespace URI.
        """
        self._prefix = prefix
        self._namespace = _Namespace(namespace)

    @classmethod
    def from_sparql_query_solution(
        cls, query_solution: og.QuerySolution
    ) -> "Namespace":
        """
        Creates a Namespace from a  binding.

        Args:
            binding (Mapping[Variable, Identifier]): The  binding.

        Returns:
            Namespace: The created Namespace.
        """
        prefix: og.Literal = query_solution[og.Variable("prefix")]
        namespace: og.NamedNode = query_solution[og.Variable("namespace")]
        return cls(
            prefix=prefix.value,
            namespace=namespace.value,
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
        return self._namespace.term(name)

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
