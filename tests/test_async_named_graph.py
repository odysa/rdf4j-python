import pytest
from rdflib import Literal

from rdf4j_python._driver._async_repository import AsyncRdf4JRepository
from rdf4j_python.model.term import IRI
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
    await graph.add([(ex["subject"], ex["predicate"], ex["object"], ex["context"])])
    assert len(await graph.get()) == 1


@pytest.mark.asyncio
async def test_async_named_graph_add_multiple(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    await graph.add(
        [
            (ex["subject"], ex["predicate"], Literal("test_object"), None),
            (ex["subject"], ex["predicate"], Literal("test_object2"), None),
        ]
    )
    assert len(await graph.get()) == 2


@pytest.mark.asyncio
async def test_async_named_graph_get(
    rdf4j_service: str, mem_repo: AsyncRdf4JRepository
):
    graph = await mem_repo.get_named_graph("test")
    statement = (ex["subject"], ex["predicate"], Literal("test_object"), None)
    await graph.add([statement])
    dataset = await graph.get()
    assert len(dataset) == 1
    assert (
        ex["subject"],
        ex["predicate"],
        Literal("test_object"),
        graph.iri,
    ) in dataset.as_list()


@pytest.mark.asyncio
async def test_async_named_graph_get_multiple(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    statement_1 = (ex["subject"], ex["predicate"], Literal("test_object"), None)
    statement_2 = (ex["subject"], ex["predicate"], Literal("test_object2"), None)
    await graph.add([statement_1, statement_2])
    dataset = await graph.get()
    assert len(dataset) == 2
    assert (
        ex["subject"],
        ex["predicate"],
        Literal("test_object"),
        graph.iri,
    ) in dataset.as_list()
    assert (
        ex["subject"],
        ex["predicate"],
        Literal("test_object2"),
        graph.iri,
    ) in dataset.as_list()


@pytest.mark.asyncio
async def test_async_named_graph_clear(mem_repo: AsyncRdf4JRepository):
    graph = await mem_repo.get_named_graph("test")
    await graph.add([(ex["subject"], ex["predicate"], ex["object"], ex["context"])])
    assert len(await graph.get()) == 1
    await graph.clear()
    assert len(await graph.get()) == 0
