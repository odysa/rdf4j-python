"""GraphPattern — composable SPARQL WHERE block."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from ._term import Term, serialize_term

if TYPE_CHECKING:
    from ._builder import SelectQuery


class GraphPattern:
    """A composable block of SPARQL graph patterns (triples, filters, etc.).

    Used as the body of a WHERE clause and inside OPTIONAL / UNION blocks.
    Every mutating method returns ``self`` for fluent chaining.
    """

    def __init__(self) -> None:
        self._elements: list[str] = []

    # ── triple patterns ──────────────────────────────────────────────

    def where(self, s: Term, p: Term, o: Term) -> GraphPattern:
        """Add a triple pattern."""
        self._elements.append(
            f"{serialize_term(s)} {serialize_term(p)} {serialize_term(o)} ."
        )
        return self

    # ── FILTER ───────────────────────────────────────────────────────

    def filter(self, expr: str) -> GraphPattern:
        """Add a ``FILTER(expr)`` clause."""
        self._elements.append(f"FILTER({expr})")
        return self

    # ── OPTIONAL ─────────────────────────────────────────────────────

    def optional(self, s_or_pattern: Term | GraphPattern, p: Term | None = None, o: Term | None = None) -> GraphPattern:
        """Add an ``OPTIONAL { … }`` block.

        Two calling conventions:
        - ``optional(s, p, o)`` — single triple shorthand
        - ``optional(GraphPattern())`` — complex pattern block
        """
        if isinstance(s_or_pattern, GraphPattern):
            body = s_or_pattern.to_sparql(indent=4)
            self._elements.append(f"OPTIONAL {{\n{body}\n  }}")
        else:
            if p is None or o is None:
                raise ValueError("optional() requires either a GraphPattern or three term arguments (s, p, o)")
            triple = f"{serialize_term(s_or_pattern)} {serialize_term(p)} {serialize_term(o)} ."
            self._elements.append(f"OPTIONAL {{ {triple} }}")
        return self

    # ── UNION ────────────────────────────────────────────────────────

    def union(self, *patterns: GraphPattern) -> GraphPattern:
        """Add ``{ … } UNION { … }`` blocks."""
        if len(patterns) < 2:
            raise ValueError("union() requires at least two GraphPattern arguments")
        parts = []
        for pat in patterns:
            body = pat.to_sparql(indent=4)
            parts.append(f"{{\n{body}\n  }}")
        self._elements.append(" UNION ".join(parts))
        return self

    # ── BIND ─────────────────────────────────────────────────────────

    def bind(self, expr: str, var: str) -> GraphPattern:
        """Add a ``BIND(expr AS ?var)`` clause."""
        v = var if var.startswith("?") else f"?{var}"
        self._elements.append(f"BIND({expr} AS {v})")
        return self

    # ── VALUES ───────────────────────────────────────────────────────

    def values(self, var: str, vals: list[Any]) -> GraphPattern:
        """Add a ``VALUES ?var { … }`` clause."""
        v = var if var.startswith("?") else f"?{var}"
        serialized = " ".join(serialize_term(val) for val in vals)
        self._elements.append(f"VALUES {v} {{ {serialized} }}")
        return self

    # ── sub-query ────────────────────────────────────────────────────

    def sub_query(self, builder: SelectQuery) -> GraphPattern:
        """Embed a sub-SELECT inside this pattern."""
        inner = builder.build()
        indented = "\n".join(f"    {line}" for line in inner.splitlines())
        self._elements.append(f"{{\n{indented}\n  }}")
        return self

    # ── rendering ────────────────────────────────────────────────────

    def to_sparql(self, indent: int = 2) -> str:
        """Render the pattern body (without the outer braces)."""
        prefix = " " * indent
        return "\n".join(f"{prefix}{el}" for el in self._elements)

    def copy(self) -> GraphPattern:
        """Return a deep copy of this pattern."""
        return copy.deepcopy(self)

    def __len__(self) -> int:
        return len(self._elements)
