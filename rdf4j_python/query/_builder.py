"""SPARQL query builder classes."""

from __future__ import annotations

import copy
from typing import Any

from rdf4j_python.model._namespace import Namespace

from ._pattern import GraphPattern
from ._term import Term, serialize_term


class _QueryBase:
    """Shared state and behaviour for all query builders.

    Provides prefix management, WHERE-clause delegation, ``copy()``, and
    ``__str__()`` so concrete builders only need to implement ``build()``.
    """

    def __init__(self) -> None:
        self._pattern = GraphPattern()
        self._prefixes: dict[str, str] = {}

    # ── prefix handling ──────────────────────────────────────────────

    def prefix(self, name_or_ns: str | Namespace, uri: str | None = None) -> Any:
        """Register a prefix.

        - ``prefix("ex", "http://example.org/")`` — string pair
        - ``prefix(ns)`` — extract from a ``Namespace`` object
        """
        if isinstance(name_or_ns, Namespace):
            self._prefixes[name_or_ns.prefix] = name_or_ns.namespace.value
        else:
            if uri is None:
                raise ValueError("uri is required when name_or_ns is a string")
            self._prefixes[name_or_ns] = uri
        return self

    def _render_prefixes(self) -> str:
        return "\n".join(f"PREFIX {k}: <{v}>" for k, v in self._prefixes.items())

    # ── WHERE-clause delegation ──────────────────────────────────────

    def where(self, s: Term, p: Term, o: Term) -> Any:
        self._pattern.where(s, p, o)
        return self

    def filter(self, expr: str) -> Any:
        self._pattern.filter(expr)
        return self

    def optional(
        self,
        s_or_pattern: Term | GraphPattern,
        p: Term | None = None,
        o: Term | None = None,
    ) -> Any:
        self._pattern.optional(s_or_pattern, p, o)
        return self

    def union(self, *patterns: GraphPattern) -> Any:
        self._pattern.union(*patterns)
        return self

    def bind(self, expr: str, var: str) -> Any:
        self._pattern.bind(expr, var)
        return self

    def values(self, var: str, vals: list[Any]) -> Any:
        self._pattern.values(var, vals)
        return self

    def sub_query(self, builder: SelectQuery) -> Any:
        self._pattern.sub_query(builder)
        return self

    # ── common helpers ───────────────────────────────────────────────

    def _render_where(self, parts: list[str]) -> None:
        """Append ``WHERE { … }`` to *parts* if patterns exist."""
        if len(self._pattern) > 0:
            parts.append("WHERE {")
            parts.append(self._pattern.to_sparql())
            parts.append("}")

    def _build_parts(self) -> list[str]:
        """Return the prefix lines (if any) as a starting list."""
        parts: list[str] = []
        if self._prefixes:
            parts.append(self._render_prefixes())
        return parts

    def copy(self):
        return copy.deepcopy(self)

    def build(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.build()


# ── concrete builders ────────────────────────────────────────────────


class SelectQuery(_QueryBase):
    """Builder for ``SELECT`` queries."""

    def __init__(self, *variables: str) -> None:
        super().__init__()
        self._variables = list(variables)
        self._distinct = False
        self._order_by: list[str] = []
        self._group_by: list[str] = []
        self._having: str | None = None
        self._limit: int | None = None
        self._offset: int | None = None

    def distinct(self) -> SelectQuery:
        self._distinct = True
        return self

    def order_by(self, *exprs: str) -> SelectQuery:
        self._order_by.extend(exprs)
        return self

    def group_by(self, *exprs: str) -> SelectQuery:
        self._group_by.extend(exprs)
        return self

    def having(self, expr: str) -> SelectQuery:
        self._having = expr
        return self

    def limit(self, n: int) -> SelectQuery:
        self._limit = n
        return self

    def offset(self, n: int) -> SelectQuery:
        self._offset = n
        return self

    def build(self) -> str:
        if not self._variables:
            raise ValueError("SELECT query requires at least one variable")
        if len(self._pattern) == 0:
            raise ValueError("SELECT query requires at least one WHERE pattern")

        parts = self._build_parts()

        keyword = "SELECT DISTINCT" if self._distinct else "SELECT"
        parts.append(f"{keyword} {' '.join(self._variables)}")

        self._render_where(parts)

        if self._group_by:
            parts.append(f"GROUP BY {' '.join(self._group_by)}")
        if self._having:
            parts.append(f"HAVING ({self._having})")
        if self._order_by:
            parts.append(f"ORDER BY {' '.join(self._order_by)}")
        if self._limit is not None:
            parts.append(f"LIMIT {self._limit}")
        if self._offset is not None:
            parts.append(f"OFFSET {self._offset}")

        return "\n".join(parts)


class AskQuery(_QueryBase):
    """Builder for ``ASK`` queries."""

    def build(self) -> str:
        if len(self._pattern) == 0:
            raise ValueError("ASK query requires at least one WHERE pattern")

        parts = self._build_parts()
        parts.append("ASK {")
        parts.append(self._pattern.to_sparql())
        parts.append("}")
        return "\n".join(parts)


class ConstructQuery(_QueryBase):
    """Builder for ``CONSTRUCT`` queries."""

    def __init__(self, *templates: tuple[Term, Term, Term]) -> None:
        super().__init__()
        self._templates = list(templates)

    def build(self) -> str:
        if not self._templates:
            raise ValueError("CONSTRUCT query requires at least one template triple")

        parts = self._build_parts()

        parts.append("CONSTRUCT {")
        for s, p, o in self._templates:
            parts.append(
                f"  {serialize_term(s)} {serialize_term(p)} {serialize_term(o)} ."
            )
        parts.append("}")

        self._render_where(parts)
        return "\n".join(parts)


class DescribeQuery(_QueryBase):
    """Builder for ``DESCRIBE`` queries."""

    def __init__(self, *resources: Term) -> None:
        super().__init__()
        self._resources = list(resources)

    def build(self) -> str:
        if not self._resources:
            raise ValueError("DESCRIBE query requires at least one resource")

        parts = self._build_parts()
        parts.append(f"DESCRIBE {' '.join(serialize_term(r) for r in self._resources)}")
        self._render_where(parts)
        return "\n".join(parts)
