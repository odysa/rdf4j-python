import pytest
import pytest_asyncio
from pyoxigraph import QuerySolutions, QueryTriples, QueryBoolean

from rdf4j_python import AsyncRdf4JRepository
from rdf4j_python.model.term import IRI, Literal, Quad
from rdf4j_python.model.vocabulary import EXAMPLE as ex
from rdf4j_python.model.vocabulary import RDF, XSD


# Sample RDF data for testing
@pytest_asyncio.fixture
async def sample_data_repo(mem_repo: AsyncRdf4JRepository):
    """Repository with comprehensive sample data for SPARQL testing."""

    # Person data
    people_data = [
        # Alice - person with complete information
        Quad(ex["alice"], RDF.type, ex["Person"], ex["people_graph"]),
        Quad(ex["alice"], ex["name"], Literal("Alice"), ex["people_graph"]),
        Quad(
            ex["alice"],
            ex["age"],
            Literal("30", datatype=XSD.integer),
            ex["people_graph"],
        ),
        Quad(
            ex["alice"], ex["email"], Literal("alice@example.org"), ex["people_graph"]
        ),
        Quad(ex["alice"], ex["city"], Literal("New York"), ex["people_graph"]),
        # Bob - person with partial information
        Quad(ex["bob"], RDF.type, ex["Person"], ex["people_graph"]),
        Quad(ex["bob"], ex["name"], Literal("Bob"), ex["people_graph"]),
        Quad(
            ex["bob"],
            ex["age"],
            Literal("25", datatype=XSD.integer),
            ex["people_graph"],
        ),
        Quad(ex["bob"], ex["city"], Literal("Boston"), ex["people_graph"]),
        # Charlie - older person
        Quad(ex["charlie"], RDF.type, ex["Person"], ex["people_graph"]),
        Quad(ex["charlie"], ex["name"], Literal("Charlie"), ex["people_graph"]),
        Quad(
            ex["charlie"],
            ex["age"],
            Literal("45", datatype=XSD.integer),
            ex["people_graph"],
        ),
        Quad(
            ex["charlie"],
            ex["email"],
            Literal("charlie@example.org"),
            ex["people_graph"],
        ),
        Quad(ex["charlie"], ex["city"], Literal("Chicago"), ex["people_graph"]),
    ]

    # Company data
    company_data = [
        Quad(ex["company_a"], RDF.type, ex["Company"], ex["company_graph"]),
        Quad(ex["company_a"], ex["name"], Literal("TechCorp"), ex["company_graph"]),
        Quad(ex["company_a"], ex["location"], Literal("New York"), ex["company_graph"]),
        Quad(ex["company_b"], RDF.type, ex["Company"], ex["company_graph"]),
        Quad(ex["company_b"], ex["name"], Literal("DataInc"), ex["company_graph"]),
        Quad(ex["company_b"], ex["location"], Literal("Boston"), ex["company_graph"]),
    ]

    # Employment relationships
    employment_data = [
        Quad(ex["alice"], ex["worksFor"], ex["company_a"], ex["employment_graph"]),
        Quad(ex["bob"], ex["worksFor"], ex["company_b"], ex["employment_graph"]),
        Quad(ex["charlie"], ex["worksFor"], ex["company_a"], ex["employment_graph"]),
    ]

    # Friend relationships
    friendship_data = [
        Quad(ex["alice"], ex["knows"], ex["bob"], ex["social_graph"]),
        Quad(ex["bob"], ex["knows"], ex["alice"], ex["social_graph"]),
        Quad(ex["alice"], ex["knows"], ex["charlie"], ex["social_graph"]),
    ]

    all_data = people_data + company_data + employment_data + friendship_data
    await mem_repo.add_statements(all_data)

    return mem_repo


class TestSelectQueries:
    """Test cases for SPARQL SELECT queries."""

    @pytest.mark.asyncio
    async def test_select_all_people(self, sample_data_repo):
        """Test basic SELECT query to get all people."""
        query = """
        SELECT ?person ?name WHERE {
            ?person a <http://example.org/Person> .
            ?person <http://example.org/name> ?name .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 3

        names = {solution["name"].value for solution in results_list}
        assert names == {"Alice", "Bob", "Charlie"}

    @pytest.mark.asyncio
    async def test_select_with_filter(self, sample_data_repo):
        """Test SELECT query with FILTER clause."""
        query = """
        SELECT ?person ?name ?age WHERE {
            ?person <http://example.org/name> ?name .
            ?person <http://example.org/age> ?age .
            FILTER(?age > 30)
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 1
        assert results_list[0]["name"].value == "Charlie"
        assert int(results_list[0]["age"].value) == 45

    @pytest.mark.asyncio
    async def test_select_with_optional(self, sample_data_repo):
        """Test SELECT query with OPTIONAL clause."""
        query = """
        SELECT ?person ?name ?email WHERE {
            ?person a <http://example.org/Person> .
            ?person <http://example.org/name> ?name .
            OPTIONAL { ?person <http://example.org/email> ?email }
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 3

        # Check that Alice and Charlie have email, Bob doesn't
        results_by_name = {
            solution["name"].value: solution for solution in results_list
        }

        assert results_by_name["Alice"]["email"] is not None
        assert results_by_name["Alice"]["email"].value == "alice@example.org"

        assert results_by_name["Charlie"]["email"] is not None
        assert results_by_name["Charlie"]["email"].value == "charlie@example.org"

        # Bob should either not have an email key or have None email value
        try:
            bob_email = results_by_name["Bob"]["email"]
            assert bob_email is None
        except KeyError:
            pass  # Missing key is also acceptable

    @pytest.mark.asyncio
    async def test_select_with_join(self, sample_data_repo):
        """Test SELECT query with JOIN across graphs."""
        query = """
        SELECT ?person ?name ?company WHERE {
            ?person <http://example.org/name> ?name .
            ?person <http://example.org/worksFor> ?org .
            ?org <http://example.org/name> ?company .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 3

        # Check specific employment relationships
        results_by_person = {
            solution["name"].value: solution for solution in results_list
        }

        assert results_by_person["Alice"]["company"].value == "TechCorp"
        assert results_by_person["Bob"]["company"].value == "DataInc"
        assert results_by_person["Charlie"]["company"].value == "TechCorp"

    @pytest.mark.asyncio
    async def test_select_with_aggregation(self, sample_data_repo):
        """Test SELECT query with aggregation functions."""
        query = """
        SELECT (COUNT(?person) AS ?totalPeople) (AVG(?age) AS ?avgAge) WHERE {
            ?person <http://example.org/age> ?age .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 1

        total_people = int(results_list[0]["totalPeople"].value)
        avg_age = float(results_list[0]["avgAge"].value)

        assert total_people == 3
        assert avg_age == (30 + 25 + 45) / 3

    @pytest.mark.asyncio
    async def test_select_with_order_by(self, sample_data_repo):
        """Test SELECT query with ORDER BY clause."""
        query = """
        SELECT ?person ?name ?age WHERE {
            ?person <http://example.org/name> ?name .
            ?person <http://example.org/age> ?age .
        }
        ORDER BY ?age
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 3

        # Should be ordered by age: Bob (25), Alice (30), Charlie (45)
        ages = [int(solution["age"].value) for solution in results_list]
        assert ages == [25, 30, 45]

        names = [solution["name"].value for solution in results_list]
        assert names == ["Bob", "Alice", "Charlie"]

    @pytest.mark.asyncio
    async def test_select_with_limit(self, sample_data_repo):
        """Test SELECT query with LIMIT clause."""
        query = """
        SELECT ?person ?name WHERE {
            ?person <http://example.org/name> ?name .
        }
        LIMIT 2
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) == 2


class TestAskQueries:
    """Test cases for SPARQL ASK queries."""

    @pytest.mark.asyncio
    async def test_ask_person_exists(self, sample_data_repo):
        """Test ASK query to check if specific person exists."""
        query = """
        ASK {
            ?person <http://example.org/name> "Alice" .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is True

    @pytest.mark.asyncio
    async def test_ask_person_not_exists(self, sample_data_repo):
        """Test ASK query for non-existent person."""
        query = """
        ASK {
            ?person <http://example.org/name> "David" .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is False

    @pytest.mark.asyncio
    async def test_ask_with_filter(self, sample_data_repo):
        """Test ASK query with FILTER clause."""
        query = """
        ASK {
            ?person <http://example.org/age> ?age .
            FILTER(?age > 40)
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is True  # Charlie is 45

    @pytest.mark.asyncio
    async def test_ask_with_filter_false(self, sample_data_repo):
        """Test ASK query with FILTER that should return false."""
        query = """
        ASK {
            ?person <http://example.org/age> ?age .
            FILTER(?age > 50)
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is False

    @pytest.mark.asyncio
    async def test_ask_relationship_exists(self, sample_data_repo):
        """Test ASK query to check if relationships exist."""
        query = """
        ASK {
            ?person1 <http://example.org/knows> ?person2 .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is True

    @pytest.mark.asyncio
    async def test_ask_specific_relationship(self, sample_data_repo):
        """Test ASK query for specific relationship."""
        query = """
        ASK {
            <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is True

    @pytest.mark.asyncio
    async def test_ask_email_exists(self, sample_data_repo):
        """Test ASK query to check if any email addresses exist."""
        query = """
        ASK {
            ?person <http://example.org/email> ?email .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryBoolean)
        assert bool(result) is True


class TestConstructQueries:
    """Test cases for SPARQL CONSTRUCT queries."""

    @pytest.mark.asyncio
    async def test_construct_simplified_person_data(self, sample_data_repo):
        """Test CONSTRUCT query to create simplified person data."""
        query = """
        CONSTRUCT {
            ?person <http://example.org/hasName> ?name ;
                    <http://example.org/hasAge> ?age .
        }
        WHERE {
            ?person <http://example.org/name> ?name .
            ?person <http://example.org/age> ?age .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) == 6  # 3 people * 2 properties each

        # Check that all expected triples are present
        subjects = {triple.subject for triple in triples_list}
        assert len(subjects) == 3  # Alice, Bob, Charlie

        predicates = {str(triple.predicate) for triple in triples_list}
        expected_predicates = {
            "<http://example.org/hasName>",
            "<http://example.org/hasAge>",
        }
        assert predicates == expected_predicates

    @pytest.mark.asyncio
    async def test_construct_employment_relationships(self, sample_data_repo):
        """Test CONSTRUCT query to create employment relationships."""
        query = """
        CONSTRUCT {
            ?person <http://example.org/employedBy> ?companyName .
        }
        WHERE {
            ?person <http://example.org/worksFor> ?company .
            ?company <http://example.org/name> ?companyName .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) == 3  # 3 employment relationships

        # Check that all triples have the correct predicate
        for triple in triples_list:
            assert str(triple.predicate) == "<http://example.org/employedBy>"

        # Check specific relationships
        company_by_person = {}
        for triple in triples_list:
            person_uri = str(triple.subject)
            company_name = triple.object.value
            company_by_person[person_uri] = company_name

        assert company_by_person["<http://example.org/alice>"] == "TechCorp"
        assert company_by_person["<http://example.org/bob>"] == "DataInc"
        assert company_by_person["<http://example.org/charlie>"] == "TechCorp"

    @pytest.mark.asyncio
    async def test_construct_with_filter(self, sample_data_repo):
        """Test CONSTRUCT query with FILTER clause."""
        query = """
        CONSTRUCT {
            ?person <http://example.org/isSenior> true .
        }
        WHERE {
            ?person <http://example.org/name> ?name .
            ?person <http://example.org/age> ?age .
            FILTER(?age > 30)
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) == 1  # Only Charlie is > 30

        triple = triples_list[0]
        assert str(triple.subject) == "<http://example.org/charlie>"
        assert str(triple.predicate) == "<http://example.org/isSenior>"
        assert triple.object.value == "true"

    @pytest.mark.asyncio
    async def test_construct_social_network(self, sample_data_repo):
        """Test CONSTRUCT query to create social network data."""
        query = """
        CONSTRUCT {
            ?person1 <http://example.org/connected> ?person2 .
            ?person2 <http://example.org/connected> ?person1 .
        }
        WHERE {
            ?person1 <http://example.org/knows> ?person2 .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert (
            len(triples_list) == 4
        )  # 4 unique bidirectional connections (with duplicates removed)

        # Check that all triples use the connected predicate
        for triple in triples_list:
            assert str(triple.predicate) == "<http://example.org/connected>"


class TestDescribeQueries:
    """Test cases for SPARQL DESCRIBE queries."""

    @pytest.mark.asyncio
    async def test_describe_specific_person(self, sample_data_repo):
        """Test DESCRIBE query for a specific person."""
        query = """
        DESCRIBE <http://example.org/alice>
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) > 0

        # All triples should have Alice as subject or object
        alice_uri = IRI("http://example.org/alice")
        for triple in triples_list:
            assert triple.subject == alice_uri or triple.object == alice_uri

    @pytest.mark.asyncio
    async def test_describe_with_where(self, sample_data_repo):
        """Test DESCRIBE query with WHERE clause."""
        query = """
        DESCRIBE ?person WHERE {
            ?person <http://example.org/age> ?age .
            FILTER(?age > 30)
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) > 0

        # Should describe Charlie (only person > 30)
        charlie_uri = IRI("http://example.org/charlie")
        charlie_triples = [t for t in triples_list if t.subject == charlie_uri]
        assert len(charlie_triples) > 0

    @pytest.mark.asyncio
    async def test_describe_multiple_resources(self, sample_data_repo):
        """Test DESCRIBE query for multiple resources."""
        query = """
        DESCRIBE ?person WHERE {
            ?person a <http://example.org/Person> .
            ?person <http://example.org/email> ?email .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) > 0

        # Should describe Alice and Charlie (people with email)
        alice_uri = IRI("http://example.org/alice")
        charlie_uri = IRI("http://example.org/charlie")

        alice_triples = [t for t in triples_list if t.subject == alice_uri]
        charlie_triples = [t for t in triples_list if t.subject == charlie_uri]

        assert len(alice_triples) > 0
        assert len(charlie_triples) > 0

    @pytest.mark.asyncio
    async def test_describe_companies(self, sample_data_repo):
        """Test DESCRIBE query for companies."""
        query = """
        DESCRIBE ?company WHERE {
            ?company a <http://example.org/Company> .
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) > 0

        # Should describe both companies
        company_a_uri = IRI("http://example.org/company_a")
        company_b_uri = IRI("http://example.org/company_b")

        company_a_triples = [t for t in triples_list if t.subject == company_a_uri]
        company_b_triples = [t for t in triples_list if t.subject == company_b_uri]

        assert len(company_a_triples) > 0
        assert len(company_b_triples) > 0


class TestComplexQueries:
    """Test cases for complex SPARQL queries combining multiple patterns."""

    @pytest.mark.asyncio
    async def test_complex_select_with_multiple_joins(self, sample_data_repo):
        """Test complex SELECT with multiple joins and filters."""
        query = """
        SELECT ?personName ?companyName ?friendName WHERE {
            ?person <http://example.org/name> ?personName .
            ?person <http://example.org/worksFor> ?company .
            ?company <http://example.org/name> ?companyName .
            ?person <http://example.org/knows> ?friend .
            ?friend <http://example.org/name> ?friendName .
            ?person <http://example.org/age> ?age .
            FILTER(?age < 35)
        }
        ORDER BY ?personName
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QuerySolutions)

        results_list = list(result)
        assert len(results_list) > 0

        # Should include Alice and Bob (both under 35 and have friends)
        person_names = {solution["personName"].value for solution in results_list}
        expected_names = {
            "Alice",
            "Bob",
        }  # Alice knows Bob/Charlie and is under 35, Bob knows Alice and is under 35
        assert person_names == expected_names

    @pytest.mark.asyncio
    async def test_complex_construct_with_calculations(self, sample_data_repo):
        """Test CONSTRUCT query creating derived data."""
        query = """
        CONSTRUCT {
            ?person <http://example.org/ageGroup> ?ageGroup .
            ?person <http://example.org/hasContact> true .
        }
        WHERE {
            ?person <http://example.org/name> ?name .
            ?person <http://example.org/age> ?age .
            OPTIONAL { ?person <http://example.org/email> ?email }
            BIND(
                IF(?age < 30, "young",
                    IF(?age < 40, "middle", "senior")
                ) AS ?ageGroup
            )
            FILTER(BOUND(?email))
        }
        """
        result = await sample_data_repo.query(query)
        assert isinstance(result, QueryTriples)

        triples_list = list(result)
        assert len(triples_list) > 0

        # Should create data for Alice and Charlie (people with email)
        subjects = {str(triple.subject) for triple in triples_list}
        expected_subjects = {
            "<http://example.org/alice>",
            "<http://example.org/charlie>",
        }
        assert subjects == expected_subjects
