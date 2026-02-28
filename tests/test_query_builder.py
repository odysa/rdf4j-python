"""Tests for the SPARQL query builder."""

import pytest
import pyoxigraph as og

from rdf4j_python.model._namespace import Namespace
from rdf4j_python.model.vocabulary import EXAMPLE as ex
from rdf4j_python.model.vocabulary import RDF
from rdf4j_python.query import (
    GraphPattern,
    ask,
    construct,
    describe,
    select,
)


# ── serialize_term ───────────────────────────────────────────────────


class TestSerializeTerm:
    def test_iri(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term(og.NamedNode("http://example.org/Person")) == "<http://example.org/Person>"

    def test_variable(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term(og.Variable("name")) == "?name"

    def test_literal_plain(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term(og.Literal("hello")) == '"hello"'

    def test_literal_with_language(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term(og.Literal("hello", language="en")) == '"hello"@en'

    def test_literal_with_datatype(self):
        from rdf4j_python.query._term import serialize_term

        dt = og.NamedNode("http://www.w3.org/2001/XMLSchema#integer")
        assert serialize_term(og.Literal("42", datatype=dt)) == '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'

    def test_literal_string_datatype_omitted(self):
        from rdf4j_python.query._term import serialize_term

        dt = og.NamedNode("http://www.w3.org/2001/XMLSchema#string")
        assert serialize_term(og.Literal("hello", datatype=dt)) == '"hello"'

    def test_blank_node(self):
        from rdf4j_python.query._term import serialize_term

        bn = og.BlankNode("b0")
        assert serialize_term(bn) == "_:b0"

    def test_string_variable(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term("?x") == "?x"

    def test_string_a(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term("a") == "a"

    def test_string_prefixed(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term("foaf:name") == "foaf:name"

    def test_string_full_iri(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term("<http://example.org/x>") == "<http://example.org/x>"

    def test_string_literal(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term('"hello"') == '"hello"'

    def test_unsupported_type(self):
        from rdf4j_python.query._term import serialize_term

        with pytest.raises(TypeError):
            serialize_term(42)

    def test_literal_with_quotes(self):
        from rdf4j_python.query._term import serialize_term

        assert serialize_term(og.Literal('say "hi"')) == '"say \\"hi\\""'

    def test_namespace_produced_iri(self):
        from rdf4j_python.query._term import serialize_term

        person = ex.Person
        assert serialize_term(person) == "<http://example.org/Person>"


# ── SelectQuery ──────────────────────────────────────────────────────


class TestSelectQuery:
    def test_basic_select(self):
        q = select("?s", "?p").where("?s", "a", "?p").build()
        assert "SELECT ?s ?p" in q
        assert "?s a ?p ." in q

    def test_select_with_typed_terms(self):
        q = (
            select("?person", "?name")
            .where("?person", RDF.type, ex.Person)
            .where("?person", ex.name, "?name")
            .build()
        )
        assert "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in q
        assert "<http://example.org/Person>" in q
        assert "<http://example.org/name>" in q

    def test_select_with_limit(self):
        q = select("?s").where("?s", "a", "?o").limit(10).build()
        assert "LIMIT 10" in q

    def test_select_with_offset(self):
        q = select("?s").where("?s", "a", "?o").offset(5).build()
        assert "OFFSET 5" in q

    def test_select_with_order_by(self):
        q = select("?name").where("?s", "a", "?name").order_by("?name").build()
        assert "ORDER BY ?name" in q

    def test_select_distinct(self):
        q = select("?name").distinct().where("?s", "a", "?name").build()
        assert "SELECT DISTINCT ?name" in q

    def test_select_with_filter(self):
        q = (
            select("?name")
            .where("?person", "a", "ex:Person")
            .where("?person", "ex:name", "?name")
            .filter("?name != 'Bob'")
            .build()
        )
        assert "FILTER(?name != 'Bob')" in q

    def test_select_with_optional_triple(self):
        q = (
            select("?name", "?email")
            .where("?person", "a", "ex:Person")
            .optional("?person", "ex:email", "?email")
            .build()
        )
        assert "OPTIONAL { ?person ex:email ?email . }" in q

    def test_select_with_optional_pattern(self):
        pattern = GraphPattern().where("?person", ex.email, "?email").filter("bound(?email)")
        q = (
            select("?name", "?email")
            .where("?person", "a", "ex:Person")
            .optional(pattern)
            .build()
        )
        assert "OPTIONAL {" in q
        assert "FILTER(bound(?email))" in q

    def test_select_with_group_by_having(self):
        q = (
            select("?city", "(COUNT(?person) AS ?count)")
            .where("?person", RDF.type, ex.Person)
            .where("?person", ex.city, "?city")
            .group_by("?city")
            .having("COUNT(?person) > 1")
            .order_by("DESC(?count)")
            .build()
        )
        assert "GROUP BY ?city" in q
        assert "HAVING (COUNT(?person) > 1)" in q
        assert "ORDER BY DESC(?count)" in q

    def test_select_with_union(self):
        q = (
            select("?label")
            .where("?s", RDF.type, ex.Person)
            .union(
                GraphPattern().where("?s", ex.name, "?label"),
                GraphPattern().where("?s", ex.nickname, "?label"),
            )
            .build()
        )
        assert "UNION" in q

    def test_select_with_bind(self):
        q = (
            select("?fullName")
            .where("?s", ex.firstName, "?fname")
            .where("?s", ex.lastName, "?lname")
            .bind("CONCAT(?fname, ' ', ?lname)", "?fullName")
            .build()
        )
        assert "BIND(CONCAT(?fname, ' ', ?lname) AS ?fullName)" in q

    def test_select_with_values(self):
        q = (
            select("?person", "?name")
            .where("?person", ex.name, "?name")
            .values("?person", [ex.alice, ex.bob])
            .build()
        )
        assert "VALUES ?person { <http://example.org/alice> <http://example.org/bob> }" in q

    def test_select_with_sub_query(self):
        sub = (
            select("?person", "(MAX(?score) AS ?maxScore)")
            .where("?person", ex.score, "?score")
            .group_by("?person")
        )
        q = (
            select("?person", "?maxScore", "?name")
            .where("?person", ex.name, "?name")
            .sub_query(sub)
            .build()
        )
        assert "SELECT ?person (MAX(?score) AS ?maxScore)" in q
        assert "GROUP BY ?person" in q

    def test_select_with_string_prefix(self):
        q = (
            select("?name")
            .prefix("ex", "http://example.org/")
            .prefix("foaf", "http://xmlns.com/foaf/0.1/")
            .where("?person", "a", "ex:Person")
            .where("?person", "foaf:name", "?name")
            .build()
        )
        assert "PREFIX ex: <http://example.org/>" in q
        assert "PREFIX foaf: <http://xmlns.com/foaf/0.1/>" in q

    def test_select_with_namespace_prefix(self):
        ns = Namespace("schema", "http://schema.org/")
        q = (
            select("?name")
            .prefix(ns)
            .where("?person", "schema:name", "?name")
            .build()
        )
        assert "PREFIX schema: <http://schema.org/>" in q

    def test_str_equals_build(self):
        builder = select("?s").where("?s", "a", "?o")
        assert str(builder) == builder.build()

    def test_copy_independence(self):
        original = select("?s").where("?s", "a", "?o")
        cloned = original.copy()
        cloned.limit(10)
        assert "LIMIT" not in original.build()
        assert "LIMIT 10" in cloned.build()

    def test_validation_no_variables(self):
        with pytest.raises(ValueError, match="at least one variable"):
            select().where("?s", "a", "?o").build()

    def test_validation_no_where(self):
        with pytest.raises(ValueError, match="at least one WHERE pattern"):
            select("?s").build()

    def test_modifier_order(self):
        """GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET appear in correct order."""
        q = (
            select("?x", "(COUNT(?y) AS ?c)")
            .where("?x", "a", "?y")
            .group_by("?x")
            .having("COUNT(?y) > 1")
            .order_by("?x")
            .limit(10)
            .offset(5)
            .build()
        )
        lines = q.splitlines()
        idx = {kw: i for i, line in enumerate(lines) for kw in ("GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET") if line.startswith(kw)}
        assert idx["GROUP BY"] < idx["HAVING"] < idx["ORDER BY"] < idx["LIMIT"] < idx["OFFSET"]

    def test_multiple_order_by(self):
        q = select("?a", "?b").where("?a", "a", "?b").order_by("?a", "DESC(?b)").build()
        assert "ORDER BY ?a DESC(?b)" in q

    def test_chaining_returns_self(self):
        builder = select("?s")
        assert builder.where("?s", "a", "?o") is builder
        assert builder.filter("true") is builder
        assert builder.optional("?s", "?p", "?o") is builder
        assert builder.bind("1", "?x") is builder
        assert builder.distinct() is builder
        assert builder.order_by("?s") is builder
        assert builder.group_by("?s") is builder
        assert builder.having("true") is builder
        assert builder.limit(1) is builder
        assert builder.offset(0) is builder


# ── AskQuery ─────────────────────────────────────────────────────────


class TestAskQuery:
    def test_basic_ask(self):
        q = ask().where("?s", RDF.type, ex.Person).build()
        assert q.startswith("ASK {")
        assert "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in q

    def test_ask_with_filter(self):
        q = ask().where("?s", "a", "?o").filter("?o = <http://example.org/X>").build()
        assert "FILTER(?o = <http://example.org/X>)" in q

    def test_ask_validation(self):
        with pytest.raises(ValueError, match="at least one WHERE pattern"):
            ask().build()

    def test_ask_str_equals_build(self):
        builder = ask().where("?s", "a", "?o")
        assert str(builder) == builder.build()

    def test_ask_with_prefix(self):
        q = (
            ask()
            .prefix("ex", "http://example.org/")
            .where("?s", "a", "ex:Person")
            .build()
        )
        assert "PREFIX ex: <http://example.org/>" in q


# ── ConstructQuery ───────────────────────────────────────────────────


class TestConstructQuery:
    def test_basic_construct(self):
        q = (
            construct(("?s", ex.fullName, "?name"))
            .where("?s", ex.firstName, "?fname")
            .bind("CONCAT(?fname, ' ', ?lname)", "?name")
            .build()
        )
        assert "CONSTRUCT {" in q
        assert "<http://example.org/fullName>" in q
        assert "WHERE {" in q
        assert "BIND(" in q

    def test_construct_validation(self):
        with pytest.raises(ValueError, match="at least one template triple"):
            construct().build()

    def test_construct_str_equals_build(self):
        builder = construct(("?s", "?p", "?o")).where("?s", "?p", "?o")
        assert str(builder) == builder.build()

    def test_construct_without_where(self):
        """CONSTRUCT without WHERE patterns should not emit WHERE block."""
        q = construct(("?s", ex.label, '"test"')).build()
        assert "CONSTRUCT {" in q
        assert "WHERE" not in q


# ── DescribeQuery ────────────────────────────────────────────────────


class TestDescribeQuery:
    def test_describe_resource(self):
        q = describe(ex.alice).build()
        assert q == "DESCRIBE <http://example.org/alice>"

    def test_describe_with_where(self):
        q = (
            describe("?person")
            .where("?person", RDF.type, ex.Person)
            .filter("?person = <http://example.org/alice>")
            .build()
        )
        assert "DESCRIBE ?person" in q
        assert "WHERE {" in q
        assert "FILTER(?person = <http://example.org/alice>)" in q

    def test_describe_multiple_resources(self):
        q = describe(ex.alice, ex.bob).build()
        assert "<http://example.org/alice>" in q
        assert "<http://example.org/bob>" in q

    def test_describe_validation(self):
        with pytest.raises(ValueError, match="at least one resource"):
            describe().build()

    def test_describe_str_equals_build(self):
        builder = describe(ex.alice)
        assert str(builder) == builder.build()


# ── GraphPattern ─────────────────────────────────────────────────────


class TestGraphPattern:
    def test_copy_independence(self):
        p1 = GraphPattern().where("?s", "a", "?o")
        p2 = p1.copy()
        p2.filter("true")
        assert "FILTER" not in p1.to_sparql()
        assert "FILTER" in p2.to_sparql()

    def test_union_requires_two_patterns(self):
        p = GraphPattern()
        with pytest.raises(ValueError, match="at least two"):
            p.union(GraphPattern().where("?s", "a", "?o"))

    def test_optional_requires_three_args_or_pattern(self):
        p = GraphPattern()
        with pytest.raises(ValueError):
            p.optional("?s", "?p")

    def test_nested_optional(self):
        inner = GraphPattern().where("?s", ex.email, "?email").filter("bound(?email)")
        outer = GraphPattern().where("?s", "a", "ex:Person").optional(inner)
        sparql = outer.to_sparql()
        assert "OPTIONAL {" in sparql
        assert "FILTER(bound(?email))" in sparql

    def test_values_with_string_var(self):
        p = GraphPattern().values("person", [ex.alice])
        assert "VALUES ?person" in p.to_sparql()

    def test_bind_with_string_var_no_question_mark(self):
        p = GraphPattern().bind("1 + 1", "result")
        assert "BIND(1 + 1 AS ?result)" in p.to_sparql()


# ── Integration / complex compositions ───────────────────────────────


class TestComplexCompositions:
    def test_full_query_from_plan_example(self):
        """Test the main SELECT example from the plan."""
        q = (
            select("?person", "?name")
            .where("?person", RDF.type, ex.Person)
            .where("?person", ex.name, "?name")
            .order_by("?name")
            .limit(10)
            .build()
        )
        assert "SELECT ?person ?name" in q
        assert "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in q
        assert "<http://example.org/Person>" in q
        assert "ORDER BY ?name" in q
        assert "LIMIT 10" in q

    def test_optional_filter_combo(self):
        q = (
            select("?name", "?email")
            .where("?person", RDF.type, ex.Person)
            .where("?person", ex.name, "?name")
            .optional("?person", ex.email, "?email")
            .filter("?name != 'Bob'")
            .build()
        )
        assert "OPTIONAL" in q
        assert "FILTER" in q

    def test_variable_objects(self):
        """Test using pyoxigraph Variable objects directly."""
        person = og.Variable("person")
        name = og.Variable("name")
        q = (
            select("?person", "?name")
            .where(person, RDF.type, ex.Person)
            .where(person, ex.name, name)
            .build()
        )
        assert "?person" in q
        assert "?name" in q

    def test_literal_in_where(self):
        lit = og.Literal("Alice", language="en")
        q = (
            select("?s")
            .where("?s", ex.name, lit)
            .build()
        )
        assert '"Alice"@en' in q
