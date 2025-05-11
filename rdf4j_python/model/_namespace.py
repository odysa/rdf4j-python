from typing import Mapping

from rdflib import URIRef
from rdflib.namespace import Namespace as RdflibNamespace
from rdflib.term import Identifier, Variable

from ._base_model import _BaseModel


class IRI(URIRef): ...


class Namespace:
    _prefix: str
    _namespace: RdflibNamespace

    def __init__(self, prefix: str, namespace: str):
        self._prefix = prefix
        self._namespace = RdflibNamespace(namespace)

    @classmethod
    def from_rdflib_binding(cls, binding: Mapping[Variable, Identifier]) -> "Namespace":
        prefix = _BaseModel.get_literal(binding, "prefix", "")
        namespace = _BaseModel.get_literal(binding, "namespace", "")
        return cls(
            prefix=prefix,
            namespace=namespace,
        )

    def __str__(self):
        return f"{self._prefix}: {self._namespace}"

    def __repr__(self):
        return f"Namespace(prefix={self._prefix}, namespace={self._namespace})"

    def __contains__(self, item: str) -> bool:
        return item in self._namespace

    def term(self, name: str) -> IRI:
        return IRI(self._namespace.term(name))

    def __getitem__(self, item: str) -> IRI:
        return self.term(item)

    def __getattr__(self, item: str) -> IRI:
        if item.startswith("__"):
            raise AttributeError
        return self.term(item)

    @property
    def namespace(self) -> IRI:
        return IRI(self._namespace)

    @property
    def prefix(self) -> str:
        return self._prefix
