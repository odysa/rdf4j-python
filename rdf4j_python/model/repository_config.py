from typing import Any, Dict, Optional

from pyoxigraph import Dataset, Quad, RdfFormat, serialize

from rdf4j_python.model import Namespace
from rdf4j_python.model.term import IRI as URIRef
from rdf4j_python.model.term import BlankNode as BNode
from rdf4j_python.model.term import Literal
from rdf4j_python.model.vocabulary import RDF, RDFS, XSD

# Define the RDF4J configuration namespace
CONFIG = Namespace("config", "tag:rdf4j.org,2023:config/")


class RepositoryConfig:
    """
    Represents the configuration for an RDF4J Repository.
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
        """
        Initializes a new RepositoryConfig instance.

        Args:
            repo_id (str): The unique identifier for the repository.
            title (Optional[str], optional): A human-readable title for the repository. Defaults to None.
            impl (Optional[RepositoryImplConfig], optional): The implementation configuration for the repository. Defaults to None.
        """
        self._repo_id = repo_id
        self._title = title
        self._impl = impl

    @property
    def repo_id(self) -> str:
        """
        Returns the repository ID.

        Returns:
            str: The repository ID.
        """
        return self._repo_id

    @property
    def title(self) -> Optional[str]:
        """
        Returns the human-readable title for the repository.

        Returns:
            Optional[str]: The human-readable title for the repository, or None if not set.
        """
        return self._title

    def to_turtle(self) -> bytes | None:
        """
        Serializes the Repository configuration to Turtle syntax using pyoxigraph.

        Returns:
            bytes | None: A UTF-8 encoded Turtle string representing the RDF4J repository configuration.
                The serialization includes the repository ID, optional human-readable title,
                and nested repository implementation configuration if available.

        Raises:
            ValueError: If any of the configuration values are of unsupported types during serialization.
        """
        graph = Dataset()
        repo_node = BNode()
        graph.add(Quad(repo_node, RDF["type"], CONFIG["Repository"], None))

        graph.add(Quad(repo_node, CONFIG["rep.id"], Literal(self._repo_id), None))

        if self._title:
            graph.add(Quad(repo_node, RDFS["label"], Literal(self._title), None))

        if self._impl:
            impl_node = self._impl.add_to_graph(graph)
            graph.add(Quad(repo_node, CONFIG["rep.impl"], impl_node, None))

        return serialize(graph, format=RdfFormat.TURTLE)


class RepositoryImplConfig:
    """
    Base class for repository implementation configurations using RDF4J.
    """

    def __init__(self, rep_type: str):
        self.rep_type = rep_type
        self.config_params: Dict[str, Any] = {}

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the repository implementation configuration to the RDF graph.

        Returns:
            The RDF node representing this configuration.
        """
        sail_node = BNode()
        graph.add(Quad(sail_node, CONFIG["rep.type"], Literal(self.rep_type), None))
        for key, value in self.config_params.items():
            if isinstance(value, str):
                graph.add(Quad(sail_node, CONFIG[key], Literal(value), None))
            elif isinstance(value, int) and not isinstance(value, bool):
                graph.add(
                    Quad(
                        sail_node,
                        CONFIG[key],
                        Literal(value, datatype=XSD["integer"]),
                        None,
                    )
                )
            elif isinstance(value, float):
                graph.add(
                    Quad(
                        sail_node,
                        CONFIG[key],
                        Literal(value, datatype=XSD["double"]),
                        None,
                    )
                )
            elif isinstance(value, bool):
                graph.add(
                    Quad(
                        sail_node,
                        CONFIG[key],
                        Literal(str(value).lower(), datatype=XSD["boolean"]),
                        None,
                    )
                )
            elif isinstance(value, list):
                for item in value:
                    graph.add(Quad(sail_node, CONFIG[key], URIRef(item), None))
            elif isinstance(value, RepositoryImplConfig) or isinstance(
                value, SailConfig
            ):
                nested_node = value.add_to_graph(graph)
                graph.add(Quad(sail_node, CONFIG[key], nested_node, None))
            else:
                raise ValueError(f"Unsupported configuration value type: {type(value)}")
        return sail_node


class SPARQLRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for a SPARQLRepository.
    """

    TYPE = "openrdf:SPARQLRepository"

    def __init__(self, query_endpoint: str, update_endpoint: Optional[str] = None):
        """
        Initializes a new SPARQLRepositoryConfig instance.

        Args:
            query_endpoint (str): The SPARQL query endpoint URL.
            update_endpoint (Optional[str], optional): The SPARQL update endpoint URL. Defaults to None.
        """
        super().__init__(rep_type=SPARQLRepositoryConfig.TYPE)
        self.config_params["sparql.queryEndpoint"] = query_endpoint
        if update_endpoint:
            self.config_params["sparql.updateEndpoint"] = update_endpoint


class HTTPRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for an HTTPRepository.
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


class SailRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for a SailRepository.
    """

    TYPE = "openrdf:SailRepository"

    def __init__(self, sail_impl: "SailConfig"):
        super().__init__(rep_type=SailRepositoryConfig.TYPE)
        self.config_params["sail.impl"] = sail_impl

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the SailRepository configuration to the RDF graph.
        """
        return super().add_to_graph(graph)


class DatasetRepositoryConfig(RepositoryImplConfig):
    """
    Configuration for a DatasetRepository using RDF datasets.
    """

    TYPE = "openrdf:DatasetRepository"

    def __init__(self, delegate: "RepositoryImplConfig"):
        super().__init__(rep_type=DatasetRepositoryConfig.TYPE)
        self.config_params["delegate"] = delegate

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the DatasetRepository configuration to the RDF Graph
        """
        repo_node = super().add_to_graph(graph)
        return repo_node


class SailConfig:
    """
    Base class for SAIL configurations using RDF4J's Storage and Inference Layer.
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

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the SAIL configuration to the RDF graph.

        Returns:
            The RDF node representing this configuration.
        """
        sail_node = BNode()
        graph.add(Quad(sail_node, CONFIG["sail.type"], Literal(self.sail_type), None))
        for key, value in self.config_params.items():
            if isinstance(value, str):
                graph.add(Quad(sail_node, CONFIG[key], Literal(value), None))
            elif isinstance(value, bool):
                graph.add(
                    Quad(
                        sail_node,
                        CONFIG[key],
                        Literal(str(value).lower(), datatype=XSD["boolean"]),
                        None,
                    )
                )
            elif isinstance(value, int) and not isinstance(value, bool):
                graph.add(
                    Quad(
                        sail_node,
                        CONFIG[key],
                        Literal(str(value), datatype=XSD["integer"]),
                        None,
                    )
                )
            elif isinstance(value, float):
                graph.add(
                    Quad(
                        sail_node,
                        CONFIG[key],
                        Literal(str(value), datatype=XSD["double"]),
                        None,
                    )
                )
            elif isinstance(value, list):
                for item in value:
                    graph.add(Quad(sail_node, CONFIG[key], URIRef(item), None))
            elif isinstance(value, SailConfig) or isinstance(
                value, RepositoryImplConfig
            ):
                nested_node = value.add_to_graph(graph)
                graph.add(Quad(sail_node, CONFIG[key], nested_node, None))
            else:
                raise ValueError(f"Unsupported configuration value type: {type(value)}")
        return sail_node


class MemoryStoreConfig(SailConfig):
    """
    Configuration for a MemoryStore using in-memory RDF storage.
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


class NativeStoreConfig(SailConfig):
    """
    Configuration for a NativeStore using persistent file-based RDF storage.
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


class ElasticsearchStoreConfig(SailConfig):
    """
    Configuration for an ElasticsearchStore using Elasticsearch for RDF storage.
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


class SchemaCachingRDFSInferencerConfig(SailConfig):
    """
    Configuration for the RDF Schema inferencer using schema caching for performance.
    """

    TYPE = "rdf4j:SchemaCachingRDFSInferencer"

    def __init__(
        self,
        delegate: "SailConfig",
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        """
        Initializes a new SchemaCachingRDFSInferencerConfig.

        Args:
            delegate (SailConfig): The delegate configuration for the SchemaCachingRDFSInferencer.
            iteration_cache_sync_threshold (Optional[int]): The iteration cache sync threshold.
            default_query_evaluation_mode (Optional[str]): The default query evaluation mode.
        """
        super().__init__(
            sail_type=SchemaCachingRDFSInferencerConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        self.config_params["delegate"] = delegate

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the SchemaCachingRDFSInferencer configuration to the RDF graph.

        Args:
            graph (Graph): The RDF graph to add the configuration to.

        Returns:
            URIRef: The URIRef of the added configuration.
        """
        return super().add_to_graph(graph)


class DirectTypeHierarchyInferencerConfig(SailConfig):
    """
    Configuration for the Direct Type inferencer using type hierarchy inference.
    """

    TYPE = "openrdf:DirectTypeHierarchyInferencer"

    def __init__(
        self,
        delegate: "SailConfig",
        iteration_cache_sync_threshold: Optional[int] = None,
        default_query_evaluation_mode: Optional[str] = None,
    ):
        """
        Initializes a new DirectTypeHierarchyInferencerConfig.

        Args:
            delegate (SailConfig): The delegate configuration for the DirectTypeHierarchyInferencer.
            iteration_cache_sync_threshold (Optional[int]): The iteration cache sync threshold.
            default_query_evaluation_mode (Optional[str]): The default query evaluation mode.
        """
        super().__init__(
            sail_type=DirectTypeHierarchyInferencerConfig.TYPE,
            iteration_cache_sync_threshold=iteration_cache_sync_threshold,
            default_query_evaluation_mode=default_query_evaluation_mode,
        )
        self.config_params["delegate"] = delegate

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the DirectTypeHierarchyInferencerConfig to the graph

        Args:
            graph (Graph): The RDF graph to add the configuration to.

        Returns:
            URIRef: The URIRef of the added configuration.
        """
        return super().add_to_graph(graph)


class SHACLSailConfig(SailConfig):
    """
    Configuration for the SHACL Sail using SHACL constraint validation.
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
        """
        Initializes a new SHACLSailConfig.

        Args:
            delegate (SailConfig): The delegate configuration for the SHACL Sail.
            parallel_validation (Optional[bool]): Whether to enable parallel validation.
            undefined_target_validates_all_subjects (Optional[bool]): Whether to validate all subjects when undefined targets are encountered.
            log_validation_plans (Optional[bool]): Whether to log validation plans.
            log_validation_violations (Optional[bool]): Whether to log validation violations.
            ignore_no_shapes_loaded_exception (Optional[bool]): Whether to ignore exceptions when no shapes are loaded.
            validation_enabled (Optional[bool]): Whether to enable validation.
            cache_select_nodes (Optional[bool]): Whether to cache select nodes.
            global_log_validation_execution (Optional[bool]): Whether to log validation execution globally.
            rdfs_sub_class_reasoning (Optional[bool]): Whether to enable RDFS sub-class reasoning.
            performance_logging (Optional[bool]): Whether to enable performance logging.
            serializable_validation (Optional[bool]): Whether to enable serializable validation.
            eclipse_rdf4j_shacl_extensions (Optional[bool]): Whether to enable Eclipse RDF4J SHACL extensions.
            dash_data_shapes (Optional[bool]): Whether to enable Dash Data Shapes.
            validation_results_limit_total (Optional[int]): The total number of validation results to limit.
            validation_results_limit_per_constraint (Optional[int]): The number of validation results to limit per constraint.
            iteration_cache_sync_threshold (Optional[int]): The iteration cache sync threshold.
            default_query_evaluation_mode (Optional[str]): The default query evaluation mode.
        """
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

    def add_to_graph(self, graph: Dataset) -> URIRef:
        """
        Adds the SHACLSailConfig to the RDF graph.

        Args:
            graph (Graph): The RDF graph to add the configuration to.

        Returns:
            URIRef: The URIRef of the added configuration.
        """
        return super().add_to_graph(graph)
