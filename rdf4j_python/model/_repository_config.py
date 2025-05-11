from typing import Any, Dict, Optional

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD

# Define the RDF4J configuration namespace
CONFIG = Namespace("tag:rdf4j.org,2023:config/")


class RepositoryConfig:
    """
    Represents the configuration for an RDF4J Repository using RDFlib.
    """

    _repo_id: str
    _title: Optional[str] = None
    _impl: Optional["RepositoryImplConfig"] = None

    def __init__(
        self,
        repo_id: str,
        title: Optional[str] = None,
        impl: Optional["RepositoryImplConfig"] = None,
    ):
        self._repo_id = repo_id
        self._title = title
        self._impl = impl

    @property
    def repo_id(self) -> str:
        return self._repo_id

    @property
    def title(self) -> Optional[str]:
        return self._title

    def to_turtle(self) -> str:
        """
        Serializes the Repository configuration to Turtle syntax using RDFlib.
        """
        graph = Graph()
        graph.bind("rdfs", RDFS)
        graph.bind("config", CONFIG)
        graph.bind("xsd", XSD)
        repo_node = BNode()
        graph.add((repo_node, RDF["type"], CONFIG["Repository"]))

        graph.add((repo_node, CONFIG["rep.id"], Literal(self._repo_id)))

        if self._title:
            graph.add((repo_node, RDFS["label"], Literal(self._title)))

        if self._impl:
            impl_node = self._impl.add_to_graph(graph)
            graph.add((repo_node, CONFIG["rep.impl"], impl_node))

        return graph.serialize(format="turtle").encode("utf-8")

    @staticmethod
    def builder_with_sail_repository(
        sail_impl: "SailConfig",
    ) -> "RepositoryConfig.Builder":
        """
        Convenience method to create a RepositoryConfig with a SailRepositoryConfig.
        """

        repo_config = RepositoryConfig.Builder().repo_impl(
            SailRepositoryConfig.Builder().sail_impl(sail_impl).build()
        )

        return repo_config

    class Builder:
        """
        Builder class for creating RepositoryConfig instances.
        """

        def __init__(self, repo_id: Optional[str] = None):
            self._repo_id = repo_id
            self._title: Optional[str] = None
            self._impl: Optional["RepositoryImplConfig"] = None

        def repo_id(self, repo_id: str) -> "RepositoryConfig.Builder":
            """
            Sets the repository ID.
            """
            self._repo_id = repo_id
            return self

        def title(self, title: str) -> "RepositoryConfig.Builder":
            """
            Sets the human-readable title for the repository.
            """
            self._title = title
            return self

        def repo_impl(self, impl: "RepositoryImplConfig") -> "RepositoryConfig.Builder":
            """
            Sets the repository implementation configuration.
            """
            self._impl = impl
            return self

        def build(self) -> "RepositoryConfig":
            """
            Builds and returns the RepositoryConfig instance.
            """
            return RepositoryConfig(
                repo_id=self._repo_id, title=self._title, impl=self._impl
            )


class RepositoryImplConfig:
    """
    Base class for repository implementation configurations using RDFlib.
    """

    def __init__(self, rep_type: str):
        self.rep_type = rep_type
        self.config_params: Dict[str, Any] = {}

    def add_to_graph(self, graph: Graph) -> URIRef:
        """
        Adds the repository implementation configuration to the RDF graph.

        Returns:
            The RDF node representing this configuration.
        """
        sail_node = BNode()
        graph.add((sail_node, CONFIG["rep.type"], Literal(self.rep_type)))
        for key, value in self.config_params.items():
            if isinstance(value, str):
                graph.add((sail_node, CONFIG[key], Literal(value)))
            elif isinstance(value, int):
                graph.add(
                    (sail_node, CONFIG[key], Literal(value, datatype=XSD.integer))
                )
            elif isinstance(value, float):
                graph.add((sail_node, CONFIG[key], Literal(value, datatype=XSD.double)))
            elif isinstance(value, bool):
                graph.add(
                    (sail_node, CONFIG[key], Literal(value, datatype=XSD.boolean))
                )
            elif isinstance(value, list):
                for item in value:
                    graph.add((sail_node, CONFIG[key], URIRef(item)))  # Assuming IRIs
            elif isinstance(value, RepositoryImplConfig) or isinstance(
                value, SailConfig
            ):
                nested_node = value.add_to_graph(graph)
                graph.add((sail_node, CONFIG[key], nested_node))
            else:
                raise ValueError(f"Unsupported configuration value type: {type(value)}")
        return sail_node


class SPARQLRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for a SPARQLRepository using RDFlib.
    """

    TYPE = "openrdf:SPARQLRepository"

    def __init__(self, query_endpoint: str, update_endpoint: Optional[str] = None):
        super().__init__(rep_type=SPARQLRepositoryConfig.TYPE)
        self.config_params["sparql.queryEndpoint"] = query_endpoint
        if update_endpoint:
            self.config_params["sparql.updateEndpoint"] = update_endpoint

    class Builder:
        def __init__(self, query_endpoint: str):
            self._query_endpoint = query_endpoint
            self._update_endpoint: Optional[str] = None

        def update_endpoint(
            self, update_endpoint: str
        ) -> "SPARQLRepositoryConfig.Builder":
            self._update_endpoint = update_endpoint
            return self

        def build(self) -> "SPARQLRepositoryConfig":
            return SPARQLRepositoryConfig(
                query_endpoint=self._query_endpoint,
                update_endpoint=self._update_endpoint,
            )


class HTTPRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for an HTTPRepository using RDFlib.
    """

    TYPE = "openrdf:HTTPRepository"

    def __init__(
        self, url: str, username: Optional[str] = None, password: Optional[str] = None
    ):
        super().__init__(rep_type=HTTPRepositoryConfig.TYPE)
        self.config_params["http.url"] = url
        if username:
            self.config_params["http.username"] = username
        if password:
            self.config_params["http.password"] = password

    class Builder:
        def __init__(self, url: str):
            self._url = url
            self._username: Optional[str] = None
            self._password: Optional[str] = None

        def username(self, username: str) -> "HTTPRepositoryConfig.Builder":
            self._username = username
            return self

        def password(self, password: str) -> "HTTPRepositoryConfig.Builder":
            self._password = password
            return self

        def build(self) -> "HTTPRepositoryConfig":
            return HTTPRepositoryConfig(
                url=self._url, username=self._username, password=self._password
            )


class SailRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for a SailRepository using RDFlib.
    """

    TYPE = "openrdf:SailRepository"

    def __init__(self, sail_impl: "SailConfig"):
        super().__init__(rep_type=SailRepositoryConfig.TYPE)
        self.config_params["sail.impl"] = sail_impl

    def add_to_graph(self, graph: Graph) -> URIRef:
        """
        Adds the SailRepository configuration to the RDF graph.
        """
        return super().add_to_graph(graph)

    class Builder:
        def __init__(self, sail_impl: Optional["SailConfig"] = None):
            self._sail_impl = sail_impl

        def sail_impl(self, sail_impl: "SailConfig") -> "SailRepositoryConfig.Builder":
            self._sail_impl = sail_impl
            return self

        def build(self) -> "SailRepositoryConfig":
            return SailRepositoryConfig(sail_impl=self._sail_impl)


class DatasetRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for a DatasetRepository using RDFlib.
    """

    TYPE = "openrdf:DatasetRepository"

    def __init__(self, delegate: "RepositoryImplConfig"):
        super().__init__(rep_type=DatasetRepositoryConfig.TYPE)
        self.config_params["delegate"] = delegate

    def add_to_graph(self, graph: Graph) -> URIRef:
        """
        Adds the DatasetRepository configuration to the RDF Graph
        """
        repo_node = super().add_to_graph(graph)
        return repo_node

    class Builder:
        def __init__(self, delegate: "RepositoryImplConfig"):
            self._delegate = delegate

        def build(self) -> "DatasetRepositoryConfig":
            return DatasetRepositoryConfig(delegate=self._delegate)


class SailConfig:
    """
    Base class for SAIL configurations using RDFlib.
    """

    def __init__(
        self,
        sail_type: str,
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        self.sail_type = sail_type
        self.config_params: Dict[str, Any] = {}
        if iteration_cache_sync_threshold is not None:
            self.config_params["sail.iterationCacheSyncThreshold"] = (
                iteration_cache_sync_threshold
            )
        if default_query_evaluation_mode:
            self.config_params["sail.defaultQueryEvaluationMode"] = (
                default_query_evaluation_mode
            )

    def add_to_graph(self, graph: Graph) -> URIRef:
        """
        Adds the SAIL configuration to the RDF graph.

        Returns:
            The RDF node representing this configuration.
        """
        sail_node = BNode()
        graph.add((sail_node, CONFIG["sail.type"], Literal(self.sail_type)))
        for key, value in self.config_params.items():
            if isinstance(value, str):
                graph.add((sail_node, CONFIG[key], Literal(value)))
            elif isinstance(value, int):
                graph.add(
                    (sail_node, CONFIG[key], Literal(value, datatype=XSD.integer))
                )
            elif isinstance(value, float):
                graph.add((sail_node, CONFIG[key], Literal(value, datatype=XSD.double)))
            elif isinstance(value, bool):
                graph.add(
                    (sail_node, CONFIG[key], Literal(value, datatype=XSD.boolean))
                )
            elif isinstance(value, list):
                for item in value:
                    graph.add((sail_node, CONFIG[key], URIRef(item)))
            elif isinstance(value, SailConfig) or isinstance(
                value, RepositoryImplConfig
            ):
                nested_node = value.add_to_graph(graph)
                graph.add((sail_node, CONFIG[key], nested_node))
            else:
                raise ValueError(f"Unsupported configuration value type: {type(value)}")
        return sail_node


class MemoryStoreConfig(SailConfig):
    """
    Configuration for a MemoryStore using RDFlib.
    """

    TYPE = "openrdf:MemoryStore"

    def __init__(
        self,
        persist: Optional[bool] = None,
        sync_delay: Optional[int] = None,
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        super().__init__(
            sail_type=MemoryStoreConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        if persist is not None:
            self.config_params["mem.persist"] = persist
        if sync_delay is not None:
            self.config_params["mem.syncDelay"] = sync_delay

    class Builder:
        def __init__(self):
            self._persist: Optional[bool] = None
            self._sync_delay: Optional[int] = None
            self._iteration_cache_sync_threshold: Optional[int] = None
            self._default_query_evaluation_mode: Optional[str] = None

        def persist(self, persist: bool) -> "MemoryStoreConfig.Builder":
            self._persist = persist
            return self

        def sync_delay(self, sync_delay: int) -> "MemoryStoreConfig.Builder":
            self._sync_delay = sync_delay
            return self

        def iteration_cache_sync_threshold(
            self, threshold: int
        ) -> "MemoryStoreConfig.Builder":
            self._iteration_cache_sync_threshold = threshold
            return self

        def default_query_evaluation_mode(
            self, mode: str
        ) -> "MemoryStoreConfig.Builder":
            self._default_query_evaluation_mode = mode
            return self

        def build(self) -> "MemoryStoreConfig":
            return MemoryStoreConfig(
                persist=self._persist,
                sync_delay=self._sync_delay,
                iteration_cache_sync_threshold=self._iteration_cache_sync_threshold,
                default_query_evaluation_mode=self._default_query_evaluation_mode,
            )


class NativeStoreConfig(SailConfig):
    """
    Configuration for a NativeStore using RDFlib.
    """

    TYPE = "openrdf:NativeStore"

    def __init__(
        self,
        triple_indexes: Optional[str] = None,
        force_sync: Optional[bool] = None,
        value_cache_size: Optional[int] = None,
        value_id_cache_size: Optional[int] = None,
        namespace_cache_size: Optional[int] = None,
        namespace_id_cache_size: Optional[int] = None,
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        super().__init__(
            sail_type=NativeStoreConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        if triple_indexes:
            self.config_params["native.tripleIndexes"] = triple_indexes
        if force_sync:
            self.config_params["native.forceSync"] = force_sync
        if value_cache_size:
            self.config_params["native.valueCacheSize"] = value_cache_size
        if value_id_cache_size:
            self.config_params["native.valueIDCacheSize"] = value_id_cache_size
        if namespace_cache_size:
            self.config_params["native.namespaceCacheSize"] = namespace_cache_size
        if namespace_id_cache_size:
            self.config_params["native.namespaceIDCacheSize"] = namespace_id_cache_size

    class Builder:
        def __init__(self):
            self._triple_indexes: Optional[str] = None
            self._force_sync: Optional[bool] = None
            self._value_cache_size: Optional[int] = None
            self._value_id_cache_size: Optional[int] = None
            self._namespace_cache_size: Optional[int] = None
            self._namespace_id_cache_size: Optional[int] = None
            self._iteration_cache_sync_threshold: Optional[int] = None
            self._default_query_evaluation_mode: Optional[str] = None

        def triple_indexes(self, indexes: str) -> "NativeStoreConfig.Builder":
            self._triple_indexes = indexes
            return self

        def force_sync(self, sync: bool) -> "NativeStoreConfig.Builder":
            self._force_sync = sync
            return self

        def value_cache_size(self, size: int) -> "NativeStoreConfig.Builder":
            self._value_cache_size = size
            return self

        def value_id_cache_size(self, size: int) -> "NativeStoreConfig.Builder":
            self._value_id_cache_size = size
            return self

        def namespace_cache_size(self, size: int) -> "NativeStoreConfig.Builder":
            self._namespace_cache_size = size
            return self

        def namespace_id_cache_size(self, size: int) -> "NativeStoreConfig.Builder":
            self._namespace_id_cache_size = size
            return self

        def iteration_cache_sync_threshold(
            self, threshold: int
        ) -> "NativeStoreConfig.Builder":
            self._iteration_cache_sync_threshold = threshold
            return self

        def default_query_evaluation_mode(
            self, mode: str
        ) -> "NativeStoreConfig.Builder":
            self._default_query_evaluation_mode = mode
            return self

        def build(self) -> "NativeStoreConfig":
            return NativeStoreConfig(
                triple_indexes=self._triple_indexes,
                force_sync=self._force_sync,
                value_cache_size=self._value_cache_size,
                value_id_cache_size=self._value_id_cache_size,
                namespace_cache_size=self._namespace_cache_size,
                namespace_id_cache_size=self._namespace_id_cache_size,
                iteration_cache_sync_threshold=self._iteration_cache_sync_threshold,
                default_query_evaluation_mode=self._default_query_evaluation_mode,
            )


class ElasticsearchStoreConfig(SailConfig):
    """
    Configuration for an ElasticsearchStore using RDFlib.
    """

    TYPE = "rdf4j:ElasticsearchStore"

    def __init__(
        self,
        hostname: str,
        port: Optional[int] = None,
        cluster_name: Optional[str] = None,
        index: Optional[str] = None,
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        super().__init__(
            sail_type=ElasticsearchStoreConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        self.config_params["ess.hostname"] = hostname
        if port is not None:
            self.config_params["ess.port"] = port
        if cluster_name is not None:
            self.config_params["ess.clusterName"] = cluster_name
        if index is not None:
            self.config_params["ess.index"] = index

    class Builder:
        def __init__(self, hostname: str):
            self._hostname = hostname
            self._port: Optional[int] = None
            self._cluster_name: Optional[str] = None
            self._index: Optional[str] = None
            self._iteration_cache_sync_threshold: Optional[int] = None
            self._default_query_evaluation_mode: Optional[str] = None

        def port(self, port: int) -> "ElasticsearchStoreConfig.Builder":
            self._port = port
            return self

        def cluster_name(self, cluster_name: str) -> "ElasticsearchStoreConfig.Builder":
            self._cluster_name = cluster_name
            return self

        def index(self, index: str) -> "ElasticsearchStoreConfig.Builder":
            self._index = index
            return self

        def iteration_cache_sync_threshold(
            self, threshold: int
        ) -> "ElasticsearchStoreConfig.Builder":
            self._iteration_cache_sync_threshold = threshold
            return self

        def default_query_evaluation_mode(
            self, mode: str
        ) -> "ElasticsearchStoreConfig.Builder":
            self._default_query_evaluation_mode = mode
            return self

        def build(self) -> "ElasticsearchStoreConfig":
            return ElasticsearchStoreConfig(
                hostname=self._hostname,
                port=self._port,
                cluster_name=self._cluster_name,
                index=self._index,
                iteration_cache_sync_threshold=self._iteration_cache_sync_threshold,
                default_query_evaluation_mode=self._default_query_evaluation_mode,
            )


class SchemaCachingRDFSInferencerConfig(SailConfig):
    """
    Configuration for the RDF Schema inferencer using RDFlib.
    """

    TYPE = "rdf4j:SchemaCachingRDFSInferencer"

    def __init__(
        self,
        delegate: "SailConfig",
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        super().__init__(
            sail_type=SchemaCachingRDFSInferencerConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        self.config_params["delegate"] = delegate

    def add_to_graph(self, graph: Graph) -> URIRef:
        """
        Adds the SchemaCachingRDFSInferencer configuration to the RDF graph.
        """
        sail_node = super().add_to_graph(graph)
        delegate_node = self.config_params["delegate"].to_rdf(graph)
        graph.add((sail_node, CONFIG.delegate, delegate_node))
        return sail_node

    class Builder:
        def __init__(self, delegate: "SailConfig"):
            self._delegate = delegate
            self._iteration_cache_sync_threshold: Optional[int] = None
            self._default_query_evaluation_mode: Optional[str] = None

        def iteration_cache_sync_threshold(
            self, threshold: int
        ) -> "SchemaCachingRDFSInferencerConfig.Builder":
            self._iteration_cache_sync_threshold = threshold
            return self

        def default_query_evaluation_mode(
            self, mode: str
        ) -> "SchemaCachingRDFSInferencerConfig.Builder":
            self._default_query_evaluation_mode = mode
            return self

        def build(self) -> "SchemaCachingRDFSInferencerConfig":
            return SchemaCachingRDFSInferencerConfig(
                delegate=self._delegate,
                iteration_cache_sync_threshold=self._iteration_cache_sync_threshold,
                default_query_evaluation_mode=self._default_query_evaluation_mode,
            )


class DirectTypeHierarchyInferencerConfig(SailConfig):
    """
    Configuration for the Direct Type inferencer using RDFlib.
    """

    TYPE = "openrdf:DirectTypeHierarchyInferencer"

    def __init__(
        self,
        delegate: "SailConfig",
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        super().__init__(
            sail_type=DirectTypeHierarchyInferencerConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        self.config_params["delegate"] = delegate

    def add_to_graph(self, graph: Graph) -> URIRef:
        """
        Adds the DirectTypeHierarchyInferencerConfig to the graph
        """
        sail_node = super().add_to_graph(graph)
        delegate_node = self.config_params["delegate"].to_rdf(graph)
        graph.add((sail_node, CONFIG["delegate"], delegate_node))
        return sail_node

    class Builder:
        def __init__(self, delegate: "SailConfig"):
            self._delegate = delegate
            self._iteration_cache_sync_threshold: Optional[int] = None
            self._default_query_evaluation_mode: Optional[str] = None

        def iteration_cache_sync_threshold(
            self, threshold: int
        ) -> "DirectTypeHierarchyInferencerConfig.Builder":
            self._iteration_cache_sync_threshold = threshold
            return self

        def default_query_evaluation_mode(
            self, mode: str
        ) -> "DirectTypeHierarchyInferencerConfig.Builder":
            self._default_query_evaluation_mode = mode
            return self

        def build(self) -> "DirectTypeHierarchyInferencerConfig":
            return DirectTypeHierarchyInferencerConfig(
                delegate=self._delegate,
                iteration_cache_sync_threshold=self._iteration_cache_sync_threshold,
                default_query_evaluation_mode=self._default_query_evaluation_mode,
            )


class SHACLSailConfig(SailConfig):
    """
    Configuration for the SHACL Sail using RDFlib.
    """

    TYPE = "rdf4j:ShaclSail"

    def __init__(
        self,
        delegate: "SailConfig",
        parallel_validation: Optional[bool] = None,
        undefined_target_validates_all_subjects: Optional[bool] = None,
        log_validation_plans: Optional[bool] = None,
        log_validation_violations: Optional[bool] = None,
        ignore_no_shapes_loaded_exception: Optional[bool] = None,
        validation_enabled: Optional[bool] = None,
        cache_select_nodes: Optional[bool] = None,
        global_log_validation_execution: Optional[bool] = None,
        rdfs_sub_class_reasoning: Optional[bool] = None,
        performance_logging: Optional[bool] = None,
        serializable_validation: Optional[bool] = None,
        eclipse_rdf4j_shacl_extensions: Optional[bool] = None,
        dash_data_shapes: Optional[bool] = None,
        validation_results_limit_total: Optional[int] = None,
        validation_results_limit_per_constraint: Optional[int] = None,
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        super().__init__(
            sail_type=SHACLSailConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        self.config_params["delegate"] = delegate
        if parallel_validation is not None:
            self.config_params["shacl.parallelValidation"] = parallel_validation
        if undefined_target_validates_all_subjects is not None:
            self.config_params["shacl.undefinedTargetValidatesAllSubjects"] = (
                undefined_target_validates_all_subjects
            )
        if log_validation_plans is not None:
            self.config_params["shacl.logValidationPlans"] = log_validation_plans
        if log_validation_violations is not None:
            self.config_params["shacl.logValidationViolations"] = (
                log_validation_violations
            )
        if ignore_no_shapes_loaded_exception is not None:
            self.config_params["shacl.ignoreNoShapesLoadedException"] = (
                ignore_no_shapes_loaded_exception
            )
        if validation_enabled is not None:
            self.config_params["shacl.validationEnabled"] = validation_enabled
        if cache_select_nodes is not None:
            self.config_params["shacl.cacheSelectNodes"] = cache_select_nodes
        if global_log_validation_execution is not None:
            self.config_params["shacl.globalLogValidationExecution"] = (
                global_log_validation_execution
            )
        if rdfs_sub_class_reasoning is not None:
            self.config_params["shacl.rdfsSubClassReasoning"] = rdfs_sub_class_reasoning
        if performance_logging is not None:
            self.config_params["shacl.performanceLogging"] = performance_logging
        if serializable_validation is not None:
            self.config_params["shacl.serializableValidation"] = serializable_validation
        if eclipse_rdf4j_shacl_extensions is not None:
            self.config_params["shacl.eclipseRdf4jShaclExtensions"] = (
                eclipse_rdf4j_shacl_extensions
            )
        if dash_data_shapes is not None:
            self.config_params["shacl.dashDataShapes"] = dash_data_shapes
        if validation_results_limit_total is not None:
            self.config_params["shacl.validationResultsLimitTotal"] = (
                validation_results_limit_total
            )
        if validation_results_limit_per_constraint is not None:
            self.config_params["shacl.validationResultsLimitPerConstraint"] = (
                validation_results_limit_per_constraint
            )

    def add_to_graph(self, graph: Graph) -> URIRef:
        """Adds the SHACLSailConfig to the RDF graph."""
        sail_node = super().add_to_graph(graph)  # Get the basic node
        delegate_node = self.config_params["delegate"].to_rdf(graph)
        graph.add((sail_node, CONFIG.delegate, delegate_node))

        # Add SHACL-specific parameters
        for key, value in self.config_params.items():
            if key != "delegate":  # Delegate is already handled
                if isinstance(value, bool):
                    graph.add(
                        (sail_node, CONFIG[key], Literal(value, datatype=XSD.boolean))
                    )
                elif isinstance(value, int):
                    graph.add(
                        (sail_node, CONFIG[key], Literal(value, datatype=XSD.integer))
                    )
                else:
                    graph.add((sail_node, CONFIG[key], Literal(value)))
        return sail_node

    class Builder:
        def __init__(self, delegate: "SailConfig"):
            self._delegate = delegate
            self._parallel_validation: Optional[bool] = None
            self._undefined_target_validates_all_subjects: Optional[bool] = None
            self._log_validation_plans: Optional[bool] = None
            self._log_validation_violations: Optional[bool] = None
            self._ignore_no_shapes_loaded_exception: Optional[bool] = None
            self._validation_enabled: Optional[bool] = None
            self._cache_select_nodes: Optional[bool] = None
            self._global_log_validation_execution: Optional[bool] = None
            self._rdfs_sub_class_reasoning: Optional[bool] = None
            self._performance_logging: Optional[bool] = None
            self._serializable_validation: Optional[bool] = None
            self._eclipse_rdf4j_shacl_extensions: Optional[bool] = None
            self._dash_data_shapes: Optional[bool] = None
            self._validation_results_limit_total: Optional[int] = None
            self._validation_results_limit_per_constraint: Optional[int] = None
            self._iteration_cache_sync_threshold: Optional[int] = None
            self._default_query_evaluation_mode: Optional[str] = None

        def parallel_validation(self, value: bool) -> "SHACLSailConfig.Builder":
            self._parallel_validation = value
            return self

        def undefined_target_validates_all_subjects(
            self, value: bool
        ) -> "SHACLSailConfig.Builder":
            self._undefined_target_validates_all_subjects = value
            return self

        def log_validation_plans(self, value: bool) -> "SHACLSailConfig.Builder":
            self._log_validation_plans = value
            return self

        def log_validation_violations(self, value: bool) -> "SHACLSailConfig.Builder":
            self._log_validation_violations = value
            return self

        def ignore_no_shapes_loaded_exception(
            self, value: bool
        ) -> "SHACLSailConfig.Builder":
            self._ignore_no_shapes_loaded_exception = value
            return self

        def validation_enabled(self, value: bool) -> "SHACLSailConfig.Builder":
            self._validation_enabled = value
            return self

        def cache_select_nodes(self, value: bool) -> "SHACLSailConfig.Builder":
            self._cache_select_nodes = value
            return self

        def global_log_validation_execution(
            self, value: bool
        ) -> "SHACLSailConfig.Builder":
            self._global_log_validation_execution = value
            return self

        def rdfs_sub_class_reasoning(self, value: bool) -> "SHACLSailConfig.Builder":
            self._rdfs_sub_class_reasoning = value
            return self

        def performance_logging(self, value: bool) -> "SHACLSailConfig.Builder":
            self._performance_logging = value
            return self

        def serializable_validation(self, value: bool) -> "SHACLSailConfig.Builder":
            self._serializable_validation = value
            return self

        def eclipse_rdf4j_shacl_extensions(
            self, value: bool
        ) -> "SHACLSailConfig.Builder":
            self._eclipse_rdf4j_shacl_extensions = value
            return self

        def dash_data_shapes(self, value: bool) -> "SHACLSailConfig.Builder":
            self._dash_data_shapes = value
            return self

        def validation_results_limit_total(
            self, value: int
        ) -> "SHACLSailConfig.Builder":
            self._validation_results_limit_total = value
            return self

        def validation_results_limit_per_constraint(
            self, value: int
        ) -> "SHACLSailConfig.Builder":
            self._validation_results_limit_per_constraint = value
            return self

        def iteration_cache_sync_threshold(
            self, threshold: int
        ) -> "SHACLSailConfig.Builder":
            self._iteration_cache_sync_threshold = threshold
            return self

        def default_query_evaluation_mode(self, mode: str) -> "SHACLSailConfig.Builder":
            self._default_query_evaluation_mode = mode
            return self

        def build(self) -> "SHACLSailConfig":
            return SHACLSailConfig(
                delegate=self._delegate,
                parallel_validation=self._parallel_validation,
                undefined_target_validates_all_subjects=self._undefined_target_validates_all_subjects,
                log_validation_plans=self._log_validation_plans,
                log_validation_violations=self._log_validation_violations,
                ignore_no_shapes_loaded_exception=self._ignore_no_shapes_loaded_exception,
                validation_enabled=self._validation_enabled,
                cache_select_nodes=self._cache_select_nodes,
                global_log_validation_execution=self._global_log_validation_execution,
                rdfs_sub_class_reasoning=self._rdfs_sub_class_reasoning,
                performance_logging=self._performance_logging,
                serializable_validation=self._serializable_validation,
                eclipse_rdf4j_shacl_extensions=self._eclipse_rdf4j_shacl_extensions,
                dash_data_shapes=self._dash_data_shapes,
                validation_results_limit_total=self._validation_results_limit_total,
                validation_results_limit_per_constraint=self._validation_results_limit_per_constraint,
                iteration_cache_sync_threshold=self._iteration_cache_sync_threshold,
                default_query_evaluation_mode=self._default_query_evaluation_mode,
            )
