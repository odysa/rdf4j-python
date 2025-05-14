import pytest
from rdflib import Literal

from rdf4j_python import AsyncRdf4JRepository
from rdf4j_python.exception.repo_exception import (
    NamespaceException,
    RepositoryNotFoundException,
)
from rdf4j_python.model import IRI, Namespace


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
    await mem_repo.set_namespace("ex", "http://example.org/")


@pytest.mark.asyncio
async def test_repo_set_namespace_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(NamespaceException):
            await repo.set_namespace("ex", "http://example.org/")


@pytest.mark.asyncio
async def test_repo_get_namespaces(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", "http://example.org/")
    await mem_repo.set_namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    namespaces = await mem_repo.get_namespaces()
    assert len(namespaces) == 2
    assert namespaces[0].prefix == "ex"
    assert namespaces[0].namespace == IRI("http://example.org/")
    assert namespaces[1].prefix == "rdf"
    assert namespaces[1].namespace == IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#")


@pytest.mark.asyncio
async def test_repo_get_namespace_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.get_namespace("ex")


@pytest.mark.asyncio
async def test_repo_get_namespace(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", "http://example.org/")
    namespace = await mem_repo.get_namespace("ex")
    assert namespace.prefix == "ex"
    assert namespace.namespace == IRI("http://example.org/")


@pytest.mark.asyncio
async def test_repo_delete_namespace_not_found(rdf4j_service: str):
    from rdf4j_python import AsyncRdf4j

    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.get_repository("not_found")
        with pytest.raises(RepositoryNotFoundException):
            await repo.delete_namespace("ex")


@pytest.mark.asyncio
async def test_repo_delete_namespace(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    await mem_repo.set_namespace("ex", "http://example.org/")
    assert len(await mem_repo.get_namespaces()) == 2
    await mem_repo.delete_namespace("ex")
    namespaces = await mem_repo.get_namespaces()
    assert len(namespaces) == 1
    assert namespaces[0].prefix == "rdf"
    assert namespaces[0].namespace == IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#")


@pytest.mark.asyncio
async def test_repo_clear_all_namespaces(mem_repo: AsyncRdf4JRepository):
    await mem_repo.set_namespace("ex", "http://example.org/")
    await mem_repo.set_namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    await mem_repo.set_namespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
    assert len(await mem_repo.get_namespaces()) == 3
    await mem_repo.clear_all_namespaces()
    assert len(await mem_repo.get_namespaces()) == 0


@pytest.mark.asyncio
async def test_repo_add_statement(mem_repo: AsyncRdf4JRepository):
    ns = Namespace("ex", "http://example.org/")
    await mem_repo.add_statement(
        ns["subject"], ns["predicate"], Literal("test_object"), ns["context"]
    )
    await mem_repo.add_statement(
        ns["subject"], ns["predicate"], Literal("test_object2"), None
    )


@pytest.mark.asyncio
async def test_repo_add_statements(mem_repo: AsyncRdf4JRepository):
    ns = Namespace("ex", "http://example.org/")
    await mem_repo.add_statements(
        [
            (ns["subject1"], ns["predicate"], Literal("test_object"), None),
            (ns["subject2"], ns["predicate"], Literal("test_object2"), None),
            (ns["subject3"], ns["predicate"], Literal("test_object3"), None),
            (ns["subject4"], ns["predicate"], Literal("test_object4"), None),
        ]
    )


@pytest.mark.asyncio
async def test_repo_get_statements(mem_repo: AsyncRdf4JRepository):
    ns = Namespace("ex", "http://example.org/")
    await mem_repo.add_statements(
        [
            (ns["subject1"], ns["predicate"], Literal("test_object"), ns["context1"]),
            (ns["subject1"], ns["predicate"], Literal("test_object2"), None),
            (ns["subject2"], ns["predicate"], Literal("test_object3"), None),
            (ns["subject3"], ns["predicate"], Literal("test_object4"), ns["context2"]),
        ]
    )
    statements = (await mem_repo.get_statements(subject=ns["subject1"])).as_list()
    assert len(statements) == 2
    assert (
        ns["subject1"],
        ns["predicate"],
        Literal("test_object"),
        ns["context1"],
    ) in statements
    assert (
        ns["subject1"],
        ns["predicate"],
        Literal("test_object2"),
        None,
    ) in statements

    context_statements = (
        await mem_repo.get_statements(contexts=[ns["context1"], ns["context2"]])
    ).as_list()
    assert len(context_statements) == 2
    assert (
        ns["subject1"],
        ns["predicate"],
        Literal("test_object"),
        ns["context1"],
    ) in context_statements
    assert (
        ns["subject3"],
        ns["predicate"],
        Literal("test_object4"),
        ns["context2"],
    ) in context_statements
