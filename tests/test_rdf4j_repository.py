import pytest

from rdf4j_python import AsyncRdf4JRepository
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryNotFoundException,
)
from rdf4j_python.model.term import Literal, Quad, QuadResultSet, Triple
from rdf4j_python.model.vocabulary import EXAMPLE as ex
from rdf4j_python.model.vocabulary import RDF, RDFS

ex_ns = ex.namespace
rdf_ns = RDF.namespace
rdfs_ns = RDFS.namespace


@pytest.mark.asyncio
async def test_repo_size(mem_repo: AsyncRdf4JRepository):
    size = await mem_repo.size()
    assert size == 0


@pytest.mark.asyncio
async def test_repo_size_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.size()


@pytest.mark.asyncio
async def test_repo_set_namespace(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", ex_ns)


@pytest.mark.asyncio
async def test_repo_set_namespace_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(NamespaceException):
            await repo.set_namespace("ex", ex_ns)


@pytest.mark.asyncio
async def test_repo_get_namespaces(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", ex_ns)
    await mem_repo.set_namespace("rdf", rdf_ns)
    namespaces = await mem_repo.get_namespaces()
    assert len(namespaces) == 2
    assert namespaces[0].prefix == "ex"
    assert namespaces[0].namespace == ex_ns
    assert namespaces[1].prefix == "rdf"
    assert namespaces[1].namespace == rdf_ns


@pytest.mark.asyncio
async def test_repo_get_namespace_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.get_namespace("ex")


@pytest.mark.asyncio
async def test_repo_get_namespace(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", ex_ns)
    namespace = await mem_repo.get_namespace("ex")
    assert namespace.prefix == "ex"
    assert namespace.namespace == ex_ns


@pytest.mark.asyncio
async def test_repo_delete_namespace_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.delete_namespace("ex")


@pytest.mark.asyncio
async def test_repo_delete_namespace(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("rdf", rdf_ns)
    await mem_repo.set_namespace("ex", ex_ns)
    assert len(await mem_repo.get_namespaces()) == 2
    await mem_repo.delete_namespace("ex")
    namespaces = await mem_repo.get_namespaces()
    assert len(namespaces) == 1
    assert namespaces[0].prefix == "rdf"
    assert namespaces[0].namespace == rdf_ns


@pytest.mark.asyncio
async def test_repo_clear_all_namespaces(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", ex_ns)
    await mem_repo.set_namespace("rdf", rdf_ns)
    await mem_repo.set_namespace("rdfs", rdfs_ns)
    assert len(await mem_repo.get_namespaces()) == 3
    await mem_repo.clear_all_namespaces()
    assert len(await mem_repo.get_namespaces()) == 0


@pytest.mark.asyncio
async def test_repo_add_statement(mem_repo: AsyncRdf4JRepository):
    await mem_repo.add_statement(
        ex["subject"],
        ex["predicate"],
        Literal("test_object"),
    )
    await mem_repo.add_statement(
        ex["subject2"],
        ex["predicate2"],
        Literal("test_object2"),
        ex["context"],
    )


@pytest.mark.asyncio
async def test_repo_add_statements(mem_repo: AsyncRdf4JRepository):
    statements = [
        Triple(ex["subject1"], ex["predicate"], Literal("test_object")),
        Triple(ex["subject2"], ex["predicate"], Literal("test_object2")),
        Quad(ex["subject3"], ex["predicate"], Literal("test_object3"), ex["context"]),
        Quad(ex["subject4"], ex["predicate"], Literal("test_object4"), ex["context"]),
    ]
    await mem_repo.add_statements(statements)


@pytest.mark.asyncio
async def test_repo_get_statements(mem_repo: AsyncRdf4JRepository):
    statement_1 = Quad(
        ex["subject1"],
        ex["predicate"],
        Literal("test_object"),
        ex["context1"],
    )
    statement_2 = Quad(ex["subject1"], ex["predicate"], Literal("test_object2"), None)
    statement_3 = Quad(ex["subject2"], ex["predicate"], Literal("test_object3"), None)
    statement_4 = Quad(
        ex["subject3"],
        ex["predicate"],
        Literal("test_object4"),
        ex["context2"],
    )

    await mem_repo.add_statements([statement_1, statement_2, statement_3, statement_4])

    resultSet: QuadResultSet = list(
        await mem_repo.get_statements(subject=ex["subject1"])
    )
    assert len(resultSet) == 2
    assert statement_1 in resultSet
    assert statement_2 in resultSet

    context_resultSet: QuadResultSet = list(
        await mem_repo.get_statements(contexts=[ex["context1"], ex["context2"]])
    )
    assert len(context_resultSet) == 2
    assert statement_1 in context_resultSet
    assert statement_4 in context_resultSet


@pytest.mark.asyncio
async def test_repo_delete_statements(mem_repo: AsyncRdf4JRepository):
    statement_1 = Quad(ex["subject1"], ex["predicate"], Literal("test_object"), None)
    statement_2 = Quad(ex["subject2"], ex["predicate"], Literal("test_object2"), None)
    statement_3 = Quad(ex["subject3"], ex["predicate"], Literal("test_object3"), None)

    await mem_repo.add_statements([statement_1, statement_2, statement_3])
    resultSet: QuadResultSet = await mem_repo.get_statements()
    assert len(list(resultSet)) == 3

    await mem_repo.delete_statements(subject=ex["subject1"])

    resultSet: QuadResultSet = await mem_repo.get_statements()
    assert statement_1 not in list(resultSet)

    await mem_repo.delete_statements(subject=ex["subject2"])
    assert statement_2 not in list(resultSet)

    await mem_repo.delete_statements(subject=ex["subject3"])
    assert len(list(await mem_repo.get_statements())) == 0


@pytest.mark.asyncio
async def test_repo_replace_statements(mem_repo: AsyncRdf4JRepository):
    old_statement_1 = Quad(
        ex["subject1"], ex["predicate"], Literal("test_object"), None
    )
    old_statement_2 = Quad(
        ex["subject2"], ex["predicate"], Literal("test_object2"), None
    )
    new_statement_1 = Quad(
        ex["subject1"], ex["predicate"], Literal("test_object3"), None
    )
    new_statement_2 = Quad(
        ex["subject2"], ex["predicate"], Literal("test_object4"), None
    )

    await mem_repo.add_statements([old_statement_1, old_statement_2])
    await mem_repo.replace_statements([new_statement_1, new_statement_2])

    resultSet: QuadResultSet = list(await mem_repo.get_statements())
    assert len(resultSet) == 2
    assert new_statement_1 in resultSet
    assert new_statement_2 in resultSet
    assert old_statement_1 not in resultSet
    assert old_statement_2 not in resultSet


@pytest.mark.asyncio
async def test_repo_replace_statements_contexts(mem_repo: AsyncRdf4JRepository):
    old_statement_1 = Quad(
        ex["subject1"],
        ex["predicate"],
        Literal("test_object", language="en"),
        ex["context"],
    )
    old_statement_2 = Quad(
        ex["subject2"],
        ex["predicate"],
        Literal("test_object2", language="en"),
        ex["context"],
    )
    new_statement_1 = Quad(
        ex["subject1"],
        ex["predicate"],
        Literal("test_object3", language="en"),
        ex["context"],
    )
    new_statement_2 = Quad(
        ex["subject2"],
        ex["predicate"],
        Literal("test_object4", language="en"),
        ex["context"],
    )
    await mem_repo.add_statements([old_statement_1, old_statement_2])
    resultSet: QuadResultSet = list(await mem_repo.get_statements())
    assert len(resultSet) == 2
    assert old_statement_1 in resultSet
    assert old_statement_2 in resultSet

    await mem_repo.replace_statements(
        [new_statement_1, new_statement_2],
        contexts=[ex["context"]],
    )
    resultSet: QuadResultSet = list(await mem_repo.get_statements())
    assert len(resultSet) == 2
    assert new_statement_1 in resultSet
    assert new_statement_2 in resultSet
    assert old_statement_1 not in resultSet
    assert old_statement_2 not in resultSet
