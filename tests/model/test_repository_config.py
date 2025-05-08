# Helper function to parse Turtle and compare graphs
from rdflib import Graph
from rdflib.compare import isomorphic

from rdf4j_python.model.repository_config import (
    ContextAwareRepositoryConfig,
    DatasetRepositoryConfig,
    ElasticsearchStoreConfig,
    HTTPRepositoryConfig,
    MemoryStoreConfig,
    NativeStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
    SPARQLRepositoryConfig,
)


def assert_isomorphic(turtle1: str, turtle2: str):
    """
    Parses two Turtle strings and asserts that the resulting RDF graphs are isomorphic.
    """
    graph1 = Graph()
    graph1.parse(data=turtle1, format="turtle")
    graph2 = Graph()
    graph2.parse(data=turtle2, format="turtle")
    if not isomorphic(graph1, graph2):
        print(graph1)
        print(graph2)
    assert isomorphic(graph1, graph2), (
        f"Graphs are not isomorphic:\n{turtle1}\n!=\n{turtle2}"
    )


class TestRepositoryConfig:
    def test_minimal_config(self):
        config = RepositoryConfig.Builder(rep_id="test_repo").build()
        expected_turtle = """
            @prefix config: <tag:rdf4j.org,2023:config/> .
            [] a config:Repository ;
                config:rep.id "test_repo" .
        """
        assert_isomorphic(config.to_turtle(), expected_turtle)

    def test_full_config(self):
        memory_store_config = (
            MemoryStoreConfig.Builder(persist=True)
            .sync_delay(1000)
            .iteration_cache_sync_threshold(5000)
            .default_query_evaluation_mode("STANDARD")
            .build()
        )
        sail_repo_config = SailRepositoryConfig.Builder(
            sail_impl=memory_store_config
        ).build()
        config = (
            RepositoryConfig.Builder(rep_id="full_test_repo")
            .label("Full Test Repository")
            .implementation(sail_repo_config)
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "full_test_repo" ;
                rdfs:label "Full Test Repository" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "openrdf:MemoryStore" ;
                        config:mem.persist "true"^^xsd:boolean ;
                        config:mem.syncDelay "1000"^^xsd:integer ;
                        config:sail.iterationCacheSyncThreshold "5000"^^xsd:integer ;
                        config:sail.defaultQueryEvaluationMode "STANDARD"
                    ]
                ] .
        """
        assert_isomorphic(config.to_turtle(), expected_turtle)

    def test_sparql_repo_config(self):
        sparql_config = (
            SPARQLRepositoryConfig.Builder(query_endpoint="http://example.com/sparql")
            .update_endpoint("http://example.com/sparql/update")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(rep_id="sparql_repo")
            .implementation(sparql_config)
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "sparql_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SPARQLRepository" ;
                    config:sparql.queryEndpoint "http://example.com/sparql" ;
                    config:sparql.updateEndpoint "http://example.com/sparql/update"
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle)

    def test_http_repo_config(self):
        http_config = (
            HTTPRepositoryConfig.Builder(url="http://example.com/rdf4j")
            .username("user1")
            .password("pass2")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(rep_id="http_repo")
            .implementation(http_config)
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "http_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:HTTPRepository" ;
                    config:http.url "http://example.com/rdf4j" ;
                    config:http.username "user1" ;
                    config:http.password "pass2"
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle)

    def test_dataset_repo_config(self):
        memory_store_config = MemoryStoreConfig.Builder(persist=False).build()
        sail_repo_config = SailRepositoryConfig.Builder(
            sail_impl=memory_store_config
        ).build()
        dataset_config = DatasetRepositoryConfig.Builder(
            delegate=sail_repo_config
        ).build()
        repo_config = (
            RepositoryConfig.Builder(rep_id="dataset_repo")
            .implementation(dataset_config)
            .build()
        )

        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "dataset_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:DatasetRepository" ;
                    config:delegate [
                        config:rep.type "openrdf:SailRepository" ;
                        config:sail.impl [
                            config:sail.type "openrdf:MemoryStore" ;
                            config:mem.persist "false"^^xsd:boolean
                        ]
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle)

    def test_context_aware_repo_config(self):
        http_repo_config = HTTPRepositoryConfig.Builder(
            url="http://example.com/rdf4j"
        ).build()
        context_aware_config = (
            ContextAwareRepositoryConfig.Builder(delegate=http_repo_config)
            .read_context("http://example.com/context1", "http://example.com/context2")
            .insert_context("http://example.com/insert_context")
            .remove_context("http://example.com/remove1", "http://example.com/remove2")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(rep_id="context_aware_repo")
            .implementation(context_aware_config)
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "context_aware_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:ContextAwareRepository" ;
                    config:delegate [
                        config:rep.type "openrdf:HTTPRepository" ;
                        config:http.url "http://example.com/rdf4j"
                    ] ;
                    config:ca.readContext <http://example.com/context1>, <http://example.com/context2> ;
                    config:ca.insertContext <http://example.com/insert_context> ;
                    config:ca.removeContext <http://example.com/remove1>, <http://example.com/remove2>
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle)

    def test_native_store_config(self):
        native_config = (
            NativeStoreConfig.Builder()
            .triple_indexes("spoc,posc")
            .force_sync(True)
            .value_cache_size(10000)
            .value_id_cache_size(5000)
            .namespace_cache_size(200)
            .namespace_id_cache_size(100)
            .iteration_cache_sync_threshold(20000)
            .default_query_evaluation_mode("STANDARD")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(rep_id="native_repo")
            .implementation(
                SailRepositoryConfig.Builder(sail_impl=native_config).build()
            )
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "native_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "openrdf:NativeStore" ;
                        config:native.tripleIndexes "spoc,posc" ;
                        config:native.forceSync "true"^^xsd:boolean ;
                        config:native.valueCacheSize "10000"^^xsd:integer ;
                        config:native.valueIDCacheSize "5000"^^xsd:integer ;
                        config:native.namespaceCacheSize "200"^^xsd:integer ;
                        config:native.namespaceIDCacheSize "100"^^xsd:integer;
                        config:sail.iterationCacheSyncThreshold "20000";
                        config:sail.defaultQueryEvaluationMode "STANDARD"
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle)

    def test_elasticsearch_store_config(self):
        es_config = (
            ElasticsearchStoreConfig.Builder(hostname="localhost")
            .port(9200)
            .cluster_name("mycluster")
            .index("myindex")
            .iteration_cache_sync_threshold(10000)
            .default_query_evaluation_mode("STANDARD")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(rep_id="es_repo")
            .implementation(SailRepositoryConfig.Builder(sail_impl=es_config).build())
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "es_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "rdf4j:ElasticsearchStore" ;
                        config:ess.hostname "localhost" ;
                        config:ess.port "9200"^^xsd:integer ;
                        config:ess.clusterName "mycluster" ;
                        config:ess.index "myindex";
                        config:sail.iterationCacheSyncThreshold "10000";
                        config:sail.defaultQueryEvaluationMode "STANDARD"
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle)
