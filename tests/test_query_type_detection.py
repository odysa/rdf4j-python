"""Tests for SPARQL query type detection."""

from rdf4j_python._driver._async_repository import _detect_query_type


class TestQueryTypeDetection:
    """Tests for the _detect_query_type helper function."""

    def test_simple_select(self):
        query = "SELECT * WHERE { ?s ?p ?o }"
        assert _detect_query_type(query) == "SELECT"

    def test_simple_ask(self):
        query = "ASK { ?s ?p ?o }"
        assert _detect_query_type(query) == "ASK"

    def test_simple_construct(self):
        query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        assert _detect_query_type(query) == "CONSTRUCT"

    def test_simple_describe(self):
        query = "DESCRIBE <http://example.org/resource>"
        assert _detect_query_type(query) == "DESCRIBE"

    def test_select_with_prefix(self):
        query = """
        PREFIX ex: <http://example.org/>
        SELECT * WHERE { ?s ex:name ?o }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_select_with_multiple_prefixes(self):
        query = """
        PREFIX ex: <http://example.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?s ?label WHERE {
            ?s rdf:type ex:Person .
            ?s rdfs:label ?label .
        }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_construct_with_prefix(self):
        query = """
        PREFIX ex: <http://example.org/>
        CONSTRUCT { ?s ex:newProp ?o }
        WHERE { ?s ex:oldProp ?o }
        """
        assert _detect_query_type(query) == "CONSTRUCT"

    def test_ask_with_prefix(self):
        query = """
        PREFIX ex: <http://example.org/>
        ASK { ex:subject ex:predicate ex:object }
        """
        assert _detect_query_type(query) == "ASK"

    def test_describe_with_prefix(self):
        query = """
        PREFIX ex: <http://example.org/>
        DESCRIBE ex:resource
        """
        assert _detect_query_type(query) == "DESCRIBE"

    def test_select_with_base(self):
        query = """
        BASE <http://example.org/>
        SELECT * WHERE { ?s ?p ?o }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_select_with_base_and_prefix(self):
        query = """
        BASE <http://example.org/>
        PREFIX ex: <http://example.org/ns#>
        SELECT * WHERE { ?s ex:name ?o }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_select_with_comment(self):
        query = """
        # This is a comment
        SELECT * WHERE { ?s ?p ?o }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_select_with_comment_and_prefix(self):
        query = """
        # Query to find all names
        PREFIX ex: <http://example.org/>
        # Another comment
        SELECT ?name WHERE { ?s ex:name ?name }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_lowercase_keywords(self):
        query = """
        prefix ex: <http://example.org/>
        select * where { ?s ?p ?o }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_mixed_case_keywords(self):
        query = """
        PrEfIx ex: <http://example.org/>
        SeLeCt * WHERE { ?s ?p ?o }
        """
        assert _detect_query_type(query) == "SELECT"

    def test_empty_query(self):
        query = ""
        assert _detect_query_type(query) == ""

    def test_whitespace_only_query(self):
        query = "   \n\t  "
        assert _detect_query_type(query) == ""

    def test_comment_only_query(self):
        query = "# Just a comment"
        assert _detect_query_type(query) == ""

    def test_insert_query(self):
        query = """
        PREFIX ex: <http://example.org/>
        INSERT DATA { ex:s ex:p ex:o }
        """
        assert _detect_query_type(query) == "INSERT"

    def test_delete_query(self):
        query = """
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ?s ex:obsolete ?o }
        """
        assert _detect_query_type(query) == "DELETE"
