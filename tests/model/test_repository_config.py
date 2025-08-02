# Helper function to parse Turtle and compare graphs
import pyoxigraph as og

from rdf4j_python.model.repository_config import (
    DatasetRepositoryConfig,
    DirectTypeHierarchyInferencerConfig,
    ElasticsearchStoreConfig,
    HTTPRepositoryConfig,
    MemoryStoreConfig,
    NativeStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
    SchemaCachingRDFSInferencerConfig,
    SHACLSailConfig,
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
        config = RepositoryConfig(repo_id="test_repo")
        expected_turtle = """
            @prefix config: <tag:rdf4j.org,2023:config/> .
            [] a config:Repository ;
                config:rep.id "test_repo" .
        """
        assert_isomorphic(config.to_turtle(), expected_turtle.encode())

    def test_full_config(self):
        memory_store_config = MemoryStoreConfig(
            persist=True,
            sync_delay=1000,
            iteration_cache_sync_threshold=5000,
            default_query_evaluation_mode="STANDARD"
        )
        sail_repo_config = SailRepositoryConfig(sail_impl=memory_store_config)
        config = RepositoryConfig(
            repo_id="full_test_repo",
            title="Full Test Repository",
            impl=sail_repo_config
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
        sparql_config = SPARQLRepositoryConfig(
            query_endpoint="http://example.com/sparql",
            update_endpoint="http://example.com/sparql/update"
        )
        repo_config = RepositoryConfig(
            repo_id="sparql_repo",
            impl=sparql_config
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
        http_config = HTTPRepositoryConfig(
            url="http://example.com/rdf4j",
            username="user1",
            password="pass2"
        )
        repo_config = RepositoryConfig(repo_id="http_repo", impl=http_config)
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
        memory_store_config = MemoryStoreConfig(persist=False)
        sail_repo_config = SailRepositoryConfig(sail_impl=memory_store_config)
        dataset_config = DatasetRepositoryConfig(delegate=sail_repo_config)
        repo_config = RepositoryConfig(
            repo_id="dataset_repo",
            impl=dataset_config
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
        native_config = NativeStoreConfig(
            triple_indexes="spoc,posc",
            force_sync=True,
            value_cache_size=10000,
            value_id_cache_size=5000,
            namespace_cache_size=200,
            namespace_id_cache_size=100,
            iteration_cache_sync_threshold=20000,
            default_query_evaluation_mode="STANDARD"
        )
        repo_config = RepositoryConfig(
            repo_id="native_repo",
            impl=SailRepositoryConfig(sail_impl=native_config)
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
        es_config = ElasticsearchStoreConfig(
            hostname="localhost",
            port=9200,
            cluster_name="mycluster",
            index="myindex",
            iteration_cache_sync_threshold=10000,
            default_query_evaluation_mode="STANDARD"
        )
        repo_config = RepositoryConfig(
            repo_id="es_repo",
            impl=SailRepositoryConfig(sail_impl=es_config)
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
        config = RepositoryConfig(
            repo_id="memory_store_repo",
            impl=SailRepositoryConfig(
                sail_impl=MemoryStoreConfig(
                    persist=False,
                    sync_delay=1000,
                    iteration_cache_sync_threshold=5000,
                    default_query_evaluation_mode="STANDARD"
                )
            )
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

    def test_repository_config_with_sail_impl_convenience(self):
        """Test the convenience sail_impl parameter in RepositoryConfig constructor."""
        memory_config = MemoryStoreConfig(persist=True)
        repo_config = RepositoryConfig(
            repo_id="convenience_repo",
            title="Convenience Repository",
            sail_impl=memory_config
        )
        
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "convenience_repo" ;
                rdfs:label "Convenience Repository" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "openrdf:MemoryStore" ;
                        config:mem.persist "true"^^xsd:boolean
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_schema_caching_rdfs_inferencer_config(self):
        """Test SchemaCachingRDFSInferencerConfig configuration."""
        memory_config = MemoryStoreConfig(persist=False)
        rdfs_config = SchemaCachingRDFSInferencerConfig(
            delegate=memory_config,
            iteration_cache_sync_threshold=10000,
            default_query_evaluation_mode="STANDARD"
        )
        repo_config = RepositoryConfig(
            repo_id="rdfs_repo",
            impl=SailRepositoryConfig(sail_impl=rdfs_config)
        )
        
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "rdfs_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "rdf4j:SchemaCachingRDFSInferencer" ;
                        config:sail.iterationCacheSyncThreshold "10000"^^xsd:integer ;
                        config:sail.defaultQueryEvaluationMode "STANDARD" ;
                        config:delegate [
                            config:sail.type "openrdf:MemoryStore" ;
                            config:mem.persist "false"^^xsd:boolean
                        ]
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_direct_type_hierarchy_inferencer_config(self):
        """Test DirectTypeHierarchyInferencerConfig configuration."""
        native_config = NativeStoreConfig(triple_indexes="spoc")
        hierarchy_config = DirectTypeHierarchyInferencerConfig(
            delegate=native_config,
            iteration_cache_sync_threshold=5000,
            default_query_evaluation_mode="STRICT"
        )
        repo_config = RepositoryConfig(
            repo_id="hierarchy_repo",
            impl=SailRepositoryConfig(sail_impl=hierarchy_config)
        )
        
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "hierarchy_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "openrdf:DirectTypeHierarchyInferencer" ;
                        config:sail.iterationCacheSyncThreshold "5000"^^xsd:integer ;
                        config:sail.defaultQueryEvaluationMode "STRICT" ;
                        config:delegate [
                            config:sail.type "openrdf:NativeStore" ;
                            config:native.tripleIndexes "spoc"
                        ]
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_shacl_sail_config(self):
        """Test SHACLSailConfig configuration with various parameters."""
        memory_config = MemoryStoreConfig(persist=True)
        shacl_config = SHACLSailConfig(
            delegate=memory_config,
            parallel_validation=True,
            undefined_target_validates_all_subjects=False,
            log_validation_plans=True,
            log_validation_violations=False,
            ignore_no_shapes_loaded_exception=True,
            validation_enabled=True,
            cache_select_nodes=False,
            global_log_validation_execution=True,
            rdfs_sub_class_reasoning=False,
            performance_logging=True,
            serializable_validation=False,
            eclipse_rdf4j_shacl_extensions=True,
            dash_data_shapes=False,
            validation_results_limit_total=1000,
            validation_results_limit_per_constraint=100,
            iteration_cache_sync_threshold=15000,
            default_query_evaluation_mode="STANDARD"
        )
        repo_config = RepositoryConfig(
            repo_id="shacl_repo",
            impl=SailRepositoryConfig(sail_impl=shacl_config)
        )
        
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "shacl_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "rdf4j:ShaclSail" ;
                        config:sail.iterationCacheSyncThreshold "15000"^^xsd:integer ;
                        config:sail.defaultQueryEvaluationMode "STANDARD" ;
                        config:shacl.parallelValidation "true"^^xsd:boolean ;
                        config:shacl.undefinedTargetValidatesAllSubjects "false"^^xsd:boolean ;
                        config:shacl.logValidationPlans "true"^^xsd:boolean ;
                        config:shacl.logValidationViolations "false"^^xsd:boolean ;
                        config:shacl.ignoreNoShapesLoadedException "true"^^xsd:boolean ;
                        config:shacl.validationEnabled "true"^^xsd:boolean ;
                        config:shacl.cacheSelectNodes "false"^^xsd:boolean ;
                        config:shacl.globalLogValidationExecution "true"^^xsd:boolean ;
                        config:shacl.rdfsSubClassReasoning "false"^^xsd:boolean ;
                        config:shacl.performanceLogging "true"^^xsd:boolean ;
                        config:shacl.serializableValidation "false"^^xsd:boolean ;
                        config:shacl.eclipseRdf4jShaclExtensions "true"^^xsd:boolean ;
                        config:shacl.dashDataShapes "false"^^xsd:boolean ;
                        config:shacl.validationResultsLimitTotal "1000"^^xsd:integer ;
                        config:shacl.validationResultsLimitPerConstraint "100"^^xsd:integer ;
                        config:delegate [
                            config:sail.type "openrdf:MemoryStore" ;
                            config:mem.persist "true"^^xsd:boolean
                        ]
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_shacl_sail_config_minimal(self):
        """Test SHACLSailConfig with minimal configuration."""
        memory_config = MemoryStoreConfig(persist=False)
        shacl_config = SHACLSailConfig(delegate=memory_config)
        repo_config = RepositoryConfig(
            repo_id="shacl_minimal_repo",
            impl=SailRepositoryConfig(sail_impl=shacl_config)
        )
        
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "shacl_minimal_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "rdf4j:ShaclSail" ;
                        config:delegate [
                            config:sail.type "openrdf:MemoryStore" ;
                            config:mem.persist "false"^^xsd:boolean
                        ]
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())

    def test_nested_inferencer_config(self):
        """Test nested inferencer configurations."""
        # Create a chain: Memory -> RDFS Inferencer -> Direct Type Hierarchy Inferencer
        memory_config = MemoryStoreConfig(persist=True)
        rdfs_config = SchemaCachingRDFSInferencerConfig(
            delegate=memory_config,
            iteration_cache_sync_threshold=8000
        )
        hierarchy_config = DirectTypeHierarchyInferencerConfig(
            delegate=rdfs_config,
            iteration_cache_sync_threshold=12000
        )
        repo_config = RepositoryConfig(
            repo_id="nested_repo",
            impl=SailRepositoryConfig(sail_impl=hierarchy_config)
        )
        
        expected_turtle = """
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix config: <tag:rdf4j.org,2023:config/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            [] a config:Repository ;
                config:rep.id "nested_repo" ;
                config:rep.impl [
                    config:rep.type "openrdf:SailRepository" ;
                    config:sail.impl [
                        config:sail.type "openrdf:DirectTypeHierarchyInferencer" ;
                        config:sail.iterationCacheSyncThreshold "12000"^^xsd:integer ;
                        config:delegate [
                            config:sail.type "rdf4j:SchemaCachingRDFSInferencer" ;
                            config:sail.iterationCacheSyncThreshold "8000"^^xsd:integer ;
                            config:delegate [
                                config:sail.type "openrdf:MemoryStore" ;
                                config:mem.persist "true"^^xsd:boolean
                            ]
                        ]
                    ]
                ] .
        """
        assert_isomorphic(repo_config.to_turtle(), expected_turtle.encode())