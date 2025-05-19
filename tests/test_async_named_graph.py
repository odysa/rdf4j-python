import pytest

from rdf4j_python._driver._async_repository import AsyncRdf4JRepository
from rdf4j_python.model.term import IRI, Literal, Quad, Triple
from rdf4j_python.model.vocabulary import EXAMPLE as ex


@pytest.mark.asyncio
async def test_async_named_graph_uri(
    rdf4j_service: str, mem_repo: AsyncRdf4JRepository
):
    graph = await mem_repo.get_named_graph("test")
    assert graph.iri == IRI(
        f"{rdf4j_service}/repositories/{mem_repo.repository_id}/rdf-graphs/test"
    )


@pytest.mark.asyncio
async def test_async_named_graph_add(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    await graph.add([Triple(ex["subject"], ex["predicate"], ex["object"])])
    assert len(list(await graph.get())) == 1


@pytest.mark.asyncio
async def test_async_named_graph_add_multiple(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    await graph.add(
        [
            Triple(ex["subject1"], ex["predicate"], Literal("test_object")),
            Triple(ex["subject2"], ex["predicate"], Literal("test_object2")),
        ]
    )
    assert len(list(await graph.get())) == 2


@pytest.mark.asyncio
async def test_async_named_graph_get(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    statement = Triple(ex["subject"], ex["predicate"], Literal("test_object"))
    await graph.add([statement])
    dataset = list(await graph.get())
    assert len(dataset) == 1
    assert (
        Quad(
            ex["subject"],
            ex["predicate"],
            Literal("test_object"),
            graph.iri,
        )
        in dataset
    )


@pytest.mark.asyncio
async def test_async_named_graph_get_multiple(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    statement_1 = Triple(ex["subject"], ex["predicate"], Literal("test_object"))
    statement_2 = Triple(ex["subject"], ex["predicate"], Literal("test_object2"))
    await graph.add([statement_1, statement_2])
    dataset = list(await graph.get())
    assert len(dataset) == 2
    assert (
        Quad(
            ex["subject"],
            ex["predicate"],
            Literal("test_object"),
            graph.iri,
        )
        in dataset
    )
    assert (
        Quad(
            ex["subject"],
            ex["predicate"],
            Literal("test_object2"),
            graph.iri,
        )
        in dataset
    )


@pytest.mark.asyncio
async def test_async_named_graph_clear(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    await graph.add([Triple(ex["subject"], ex["predicate"], ex["object"])])
    assert len(list(await graph.get())) == 1
    await graph.clear()
    assert len(list(await graph.get())) == 0
