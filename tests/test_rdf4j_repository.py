import pytest
from rdflib import Literal

from rdf4j_python import AsyncRdf4JRepository
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryNotFoundException,
)
from rdf4j_python.model.term import IRI
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
    assert namespaces[0].namespace == IRI(ex_ns)
    assert namespaces[1].prefix == "rdf"
    assert namespaces[1].namespace == IRI(rdf_ns)


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
    assert namespace.namespace == IRI(ex_ns)


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
    assert namespaces[0].namespace == IRI(rdf_ns)


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
    statement_1 = (
        ex["subject"],
        ex["predicate"],
        Literal("test_object"),
        ex["context"],
    )
    statement_2 = (ex["subject"], ex["predicate"], Literal("test_object2"), None)
    await mem_repo.add_statement(*statement_1)
    await mem_repo.add_statement(*statement_2)


@pytest.mark.asyncio
async def test_repo_add_statements(mem_repo: AsyncRdf4JRepository):
    statements = [
        (ex["subject1"], ex["predicate"], Literal("test_object"), None),
        (ex["subject2"], ex["predicate"], Literal("test_object2"), None),
        (ex["subject3"], ex["predicate"], Literal("test_object3"), None),
        (ex["subject4"], ex["predicate"], Literal("test_object4"), None),
    ]
    await mem_repo.add_statements(statements)


@pytest.mark.asyncio
async def test_repo_get_statements(mem_repo: AsyncRdf4JRepository):
    statement_1 = (
        ex["subject1"],
        ex["predicate"],
        Literal("test_object"),
        ex["context1"],
    )
    statement_2 = (ex["subject1"], ex["predicate"], Literal("test_object2"), None)
    statement_3 = (ex["subject2"], ex["predicate"], Literal("test_object3"), None)
    statement_4 = (
        ex["subject3"],
        ex["predicate"],
        Literal("test_object4"),
        ex["context2"],
    )

    await mem_repo.add_statements([statement_1, statement_2, statement_3, statement_4])

    statements = (await mem_repo.get_statements(subject=ex["subject1"])).as_list()
    assert len(statements) == 2
    assert statement_1 in statements
    assert statement_2 in statements

    context_statements = (
        await mem_repo.get_statements(contexts=[ex["context1"], ex["context2"]])
    ).as_list()
    assert len(context_statements) == 2
    assert statement_1 in context_statements
    assert statement_4 in context_statements


@pytest.mark.asyncio
async def test_repo_delete_statements(mem_repo: AsyncRdf4JRepository):
    statement_1 = (ex["subject1"], ex["predicate"], Literal("test_object"), None)
    statement_2 = (ex["subject2"], ex["predicate"], Literal("test_object2"), None)
    statement_3 = (ex["subject3"], ex["predicate"], Literal("test_object3"), None)

    await mem_repo.add_statements([statement_1, statement_2, statement_3])

    assert len(await mem_repo.get_statements()) == 3
    await mem_repo.delete_statements(subject=ex["subject1"])
    assert statement_1 not in await mem_repo.get_statements()
    await mem_repo.delete_statements(subject=ex["subject2"])
    assert statement_2 not in await mem_repo.get_statements()
    await mem_repo.delete_statements(subject=ex["subject3"])
    assert len(await mem_repo.get_statements()) == 0


@pytest.mark.asyncio
async def test_repo_replace_statements(mem_repo: AsyncRdf4JRepository):
    old_statement_1 = (ex["subject1"], ex["predicate"], Literal("test_object"), None)
    old_statement_2 = (ex["subject2"], ex["predicate"], Literal("test_object2"), None)
    new_statement_1 = (ex["subject1"], ex["predicate"], Literal("test_object3"), None)
    new_statement_2 = (ex["subject2"], ex["predicate"], Literal("test_object4"), None)

    await mem_repo.add_statements([old_statement_1, old_statement_2])
    await mem_repo.replace_statements([new_statement_1, new_statement_2])

    all_statements = await mem_repo.get_statements()
    assert len(all_statements) == 2
    assert new_statement_1 in all_statements
    assert new_statement_2 in all_statements
    assert old_statement_1 not in all_statements
    assert old_statement_2 not in all_statements


@pytest.mark.asyncio
async def test_repo_replace_statements_contexts(mem_repo: AsyncRdf4JRepository):
    old_statement_1 = (
        ex["subject1"],
        ex["predicate"],
        Literal("test_object"),
        ex["context1"],
    )
    old_statement_2 = (
        ex["subject2"],
        ex["predicate"],
        Literal("test_object2"),
        ex["context2"],
    )
    new_statement_1 = (
        ex["subject1"],
        ex["predicate"],
        Literal("test_object3"),
        ex["context1"],
    )
    new_statement_2 = (
        ex["subject2"],
        ex["predicate"],
        Literal("test_object4"),
        ex["context2"],
    )
    await mem_repo.add_statements([old_statement_1, old_statement_2])
    assert len(await mem_repo.get_statements()) == 2
    assert old_statement_1 in await mem_repo.get_statements()
    assert old_statement_2 in await mem_repo.get_statements()

    await mem_repo.replace_statements(
        [new_statement_1, new_statement_2],
        contexts=[ex["context1"], ex["context2"]],
    )
    assert len(await mem_repo.get_statements()) == 2
    assert new_statement_1 in await mem_repo.get_statements()
    assert new_statement_2 in await mem_repo.get_statements()
    assert old_statement_1 not in await mem_repo.get_statements()
    assert old_statement_2 not in await mem_repo.get_statements()
