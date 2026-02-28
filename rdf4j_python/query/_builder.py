"""SPARQL query builder classes."""

from __future__ import annotations

import copy
from typing import Any

from rdf4j_python.model._namespace import Namespace

from ._pattern import GraphPattern
from ._term import Term, serialize_term


# ── mixin for WHERE-clause delegation ────────────────────────────────


class _WhereClauseMixin:
    """Methods that delegate to the internal ``_pattern: GraphPattern``."""

    _pattern: GraphPattern

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


# ── prefix handling mixin ────────────────────────────────────────────


class _PrefixMixin:
    """Shared prefix management."""

    _prefixes: dict[str, str]

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
        lines = [f"PREFIX {k}: <{v}>" for k, v in self._prefixes.items()]
        return "\n".join(lines)


# ── SelectQuery ──────────────────────────────────────────────────────


class SelectQuery(_WhereClauseMixin, _PrefixMixin):
    """Builder for ``SELECT`` queries."""

    def __init__(self, *variables: str) -> None:
        self._variables = list(variables)
        self._distinct = False
        self._pattern = GraphPattern()
        self._prefixes: dict[str, str] = {}
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

    def copy(self) -> SelectQuery:
        return copy.deepcopy(self)

    def build(self) -> str:
        if not self._variables:
            raise ValueError("SELECT query requires at least one variable")
        if len(self._pattern) == 0:
            raise ValueError("SELECT query requires at least one WHERE pattern")

        parts: list[str] = []

        # prefixes
        if self._prefixes:
            parts.append(self._render_prefixes())

        # SELECT line
        keyword = "SELECT DISTINCT" if self._distinct else "SELECT"
        parts.append(f"{keyword} {' '.join(self._variables)}")

        # WHERE
        parts.append("WHERE {")
        parts.append(self._pattern.to_sparql())
        parts.append("}")

        # modifiers
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

    def __str__(self) -> str:
        return self.build()


# ── AskQuery ─────────────────────────────────────────────────────────


class AskQuery(_WhereClauseMixin, _PrefixMixin):
    """Builder for ``ASK`` queries."""

    def __init__(self) -> None:
        self._pattern = GraphPattern()
        self._prefixes: dict[str, str] = {}

    def copy(self) -> AskQuery:
        return copy.deepcopy(self)

    def build(self) -> str:
        if len(self._pattern) == 0:
            raise ValueError("ASK query requires at least one WHERE pattern")

        parts: list[str] = []
        if self._prefixes:
            parts.append(self._render_prefixes())
        parts.append("ASK {")
        parts.append(self._pattern.to_sparql())
        parts.append("}")
        return "\n".join(parts)

    def __str__(self) -> str:
        return self.build()


# ── ConstructQuery ───────────────────────────────────────────────────


class ConstructQuery(_WhereClauseMixin, _PrefixMixin):
    """Builder for ``CONSTRUCT`` queries."""

    def __init__(self, *templates: tuple[Term, Term, Term]) -> None:
        self._templates = list(templates)
        self._pattern = GraphPattern()
        self._prefixes: dict[str, str] = {}

    def copy(self) -> ConstructQuery:
        return copy.deepcopy(self)

    def build(self) -> str:
        if not self._templates:
            raise ValueError("CONSTRUCT query requires at least one template triple")

        parts: list[str] = []
        if self._prefixes:
            parts.append(self._render_prefixes())

        # CONSTRUCT template
        template_lines = []
        for s, p, o in self._templates:
            template_lines.append(
                f"  {serialize_term(s)} {serialize_term(p)} {serialize_term(o)} ."
            )
        parts.append("CONSTRUCT {")
        parts.extend(template_lines)
        parts.append("}")

        # WHERE (optional for CONSTRUCT but we always emit it if patterns exist)
        if len(self._pattern) > 0:
            parts.append("WHERE {")
            parts.append(self._pattern.to_sparql())
            parts.append("}")

        return "\n".join(parts)

    def __str__(self) -> str:
        return self.build()


# ── DescribeQuery ────────────────────────────────────────────────────


class DescribeQuery(_WhereClauseMixin, _PrefixMixin):
    """Builder for ``DESCRIBE`` queries."""

    def __init__(self, *resources: Term) -> None:
        self._resources = list(resources)
        self._pattern = GraphPattern()
        self._prefixes: dict[str, str] = {}

    def copy(self) -> DescribeQuery:
        return copy.deepcopy(self)

    def build(self) -> str:
        if not self._resources:
            raise ValueError("DESCRIBE query requires at least one resource")

        parts: list[str] = []
        if self._prefixes:
            parts.append(self._render_prefixes())

        resources_str = " ".join(serialize_term(r) for r in self._resources)
        parts.append(f"DESCRIBE {resources_str}")

        if len(self._pattern) > 0:
            parts.append("WHERE {")
            parts.append(self._pattern.to_sparql())
            parts.append("}")

        return "\n".join(parts)

    def __str__(self) -> str:
        return self.build()
