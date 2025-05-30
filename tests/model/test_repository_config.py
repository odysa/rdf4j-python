# Helper function to parse Turtle and compare graphs
import pyoxigraph as og

from rdf4j_python.model.repository_config import (
    DatasetRepositoryConfig,
    ElasticsearchStoreConfig,
    HTTPRepositoryConfig,
    MemoryStoreConfig,
    NativeStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
    SPARQLRepositoryConfig,
)


def assert_isomorphic(turtle1: bytes | None, turtle2: bytes | None):
    """
    Parses two Turtle strings and asserts that the resulting RDF graphs are isomorphic.
    """
    parser_1 = og.parse(turtle1, format=og.RdfFormat.TURTLE)
    parser_2 = og.parse(turtle2, format=og.RdfFormat.TURTLE)
    graph1 = og.Dataset(parser_1)
    graph2 = og.Dataset(parser_2)
    graph1.canonicalize(og.CanonicalizationAlgorithm.UNSTABLE)
    graph2.canonicalize(og.CanonicalizationAlgorithm.UNSTABLE)

    assert graph1 == graph2


class TestRepositoryConfig:
    def test_minimal_config(self):
        config = RepositoryConfig.Builder(repo_id="test_repo").build()
        expected_turtle = """
            @prefix config: <tag:rdf4j.org,2023:config/> .
            [] a config:Repository ;
                config:rep.id "test_repo" .
        """
        assert_isomorphic(config.to_turtle(), expected_turtle.encode())

    def test_full_config(self):
        memory_store_config = (
            MemoryStoreConfig.Builder()
            .persist(True)
            .sync_delay(1000)
            .iteration_cache_sync_threshold(5000)
            .default_query_evaluation_mode("STANDARD")
            .build()
        )
        sail_repo_config = SailRepositoryConfig.Builder(
            sail_impl=memory_store_config
        ).build()
        config = (
            RepositoryConfig.Builder(repo_id="full_test_repo")
            .title("Full Test Repository")
            .repo_impl(sail_repo_config)
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
        assert_isomorphic(config.to_turtle(), expected_turtle.encode())

    def test_sparql_repo_config(self):
        sparql_config = (
            SPARQLRepositoryConfig.Builder(query_endpoint="http://example.com/sparql")
            .update_endpoint("http://example.com/sparql/update")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(repo_id="sparql_repo")
            .repo_impl(sparql_config)
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
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_http_repo_config(self):
        http_config = (
            HTTPRepositoryConfig.Builder(url="http://example.com/rdf4j")
            .username("user1")
            .password("pass2")
            .build()
        )
        repo_config = (
            RepositoryConfig.Builder(repo_id="http_repo").repo_impl(http_config).build()
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
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_dataset_repo_config(self):
        memory_store_config = MemoryStoreConfig.Builder().persist(False).build()
        sail_repo_config = SailRepositoryConfig.Builder(
            sail_impl=memory_store_config
        ).build()
        dataset_config = DatasetRepositoryConfig.Builder(
            delegate=sail_repo_config
        ).build()
        repo_config = (
            RepositoryConfig.Builder(repo_id="dataset_repo")
            .repo_impl(dataset_config)
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
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

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
            RepositoryConfig.Builder(repo_id="native_repo")
            .repo_impl(SailRepositoryConfig.Builder().sail_impl(native_config).build())
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
                        config:sail.iterationCacheSyncThreshold "20000"^^xsd:integer;
                        config:sail.defaultQueryEvaluationMode "STANDARD"
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

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
            RepositoryConfig.Builder(repo_id="es_repo")
            .repo_impl(SailRepositoryConfig.Builder(sail_impl=es_config).build())
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
                        config:sail.iterationCacheSyncThreshold "10000"^^xsd:integer;
                        config:sail.defaultQueryEvaluationMode "STANDARD"
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_memory_store_config_defaults(self):
        config = (
            RepositoryConfig.Builder(repo_id="memory_store_repo")
            .repo_impl(
                SailRepositoryConfig.Builder(
                    sail_impl=MemoryStoreConfig.Builder()
                    .persist(False)
                    .sync_delay(1000)
                    .iteration_cache_sync_threshold(5000)
                    .default_query_evaluation_mode("STANDARD")
                    .build()
                ).build()
            )
            .build()
        )
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "memory_store_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "openrdf:MemoryStore" ;
                        config:mem.persist "false"^^xsd:boolean ;
                        config:mem.syncDelay "1000"^^xsd:integer ;
                        config:sail.iterationCacheSyncThreshold "5000"^^xsd:integer ;
                        config:sail.defaultQueryEvaluationMode "STANDARD"
                    ]
                ] .
        """
        assert_isomorphic(config.to_turtle(), expected_turtle.encode())
