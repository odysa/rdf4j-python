import asyncio

from pyoxigraph import QuerySolutions

from rdf4j_python import AsyncRdf4j, select
from rdf4j_python.model.term import IRI, Literal, Quad


async def main():
    async with AsyncRdf4j("http://localhost:19780/rdf4j-server") as db:
        repo = await db.get_repository("example-repo-2")
        await repo.add_statements(
            [
                Quad(
                    IRI("http://example.org/subject1"),
                    IRI("http://example.org/predicate"),
                    Literal("test_object1"),
                    IRI("http://example.org/graph1"),
                ),
                Quad(
                    IRI("http://example.org/subject2"),
                    IRI("http://example.org/predicate"),
                    Literal("test_object2"),
                    IRI("http://example.org/graph1"),
                ),
            ]
        )

        # Build the query using the fluent query builder
        query = select("?s", "?p", "?o").where("?s", "?p", "?o").build()

        result = await repo.query(query)
        assert isinstance(result, QuerySolutions)
        for solution in result:
            print(solution)


if __name__ == "__main__":
    asyncio.run(main())
