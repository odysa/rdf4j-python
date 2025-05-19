import pyoxigraph as og
import pytest

from rdf4j_python.model._repository_info import RepositoryMetadata


def test_from_sparql_query_solution_valid(valid_repo_list_result: og.QuerySolutions):
    for query_solution in valid_repo_list_result:
        repo = RepositoryMetadata.from_sparql_query_solution(query_solution)
        assert repo.id == query_solution["id"].value
        assert repo.title == query_solution["title"].value
        assert repo.uri == query_solution["uri"].value
        assert repo.readable is bool(query_solution["readable"].value)
        assert repo.writable is bool(query_solution["writable"].value)


def test_from_sparql_query_solution_partial(partial_result: og.QuerySolutions):
    with pytest.raises(ValueError):
        RepositoryMetadata.from_sparql_query_solution(next(partial_result))


def test_str_representation(valid_repo_list_result: og.QuerySolutions):
    for query_solution in valid_repo_list_result:
        repo = RepositoryMetadata.from_sparql_query_solution(query_solution)
        assert (
            str(repo) == f"Repository(id={repo.id}, title={repo.title}, uri={repo.uri})"
        )
