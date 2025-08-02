"""Integration tests for repository configurations.

These tests verify that the repository configurations can be used to actually create
repositories in an RDF4J server and perform basic operations.
"""
import pytest
from random import randint

from rdf4j_python import AsyncRdf4j
from rdf4j_python.model.repository_config import (
    DatasetRepositoryConfig,
    DirectTypeHierarchyInferencerConfig,
    MemoryStoreConfig,
    NativeStoreConfig,
    RepositoryConfig,
    SailRepositoryConfig,
    SchemaCachingRDFSInferencerConfig,
    SHACLSailConfig,
    SPARQLRepositoryConfig,
)
from rdf4j_python.model.term import IRI, Literal


def random_repo_id() -> str:
    """Generate a random repository ID for testing."""
    return f"test_repo_{randint(1, 1000000)}"


@pytest.mark.asyncio
async def test_memory_store_repository_creation(rdf4j_service: str):
    """Test creating a repository with MemoryStoreConfig."""
    repo_id = random_repo_id()
    config = RepositoryConfig(
        repo_id=repo_id,
        title="Memory Store Test Repository",
        sail_impl=MemoryStoreConfig(persist=False)
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation - add a statement
            subject = IRI("http://example.com/subject")
            predicate = IRI("http://example.com/predicate")
            obj = Literal("test_value")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added
            statements = list(await repo.get_statements())
            assert len(statements) == 1
            assert statements[0].subject == subject
            assert statements[0].predicate == predicate
            assert statements[0].object == obj
            
        finally:
            # Clean up
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_native_store_repository_creation(rdf4j_service: str):
    """Test creating a repository with NativeStoreConfig."""
    repo_id = random_repo_id()
    config = RepositoryConfig(
        repo_id=repo_id,
        title="Native Store Test Repository",
        impl=SailRepositoryConfig(
            sail_impl=NativeStoreConfig(
                triple_indexes="spoc,posc",
                force_sync=False
            )
        )
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation
            subject = IRI("http://example.com/native")
            predicate = IRI("http://example.com/hasValue")
            obj = Literal("native_value")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added
            statements = list(await repo.get_statements())
            assert len(statements) == 1
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_dataset_repository_creation(rdf4j_service: str):
    """Test creating a repository with DatasetRepositoryConfig."""
    repo_id = random_repo_id()
    memory_config = MemoryStoreConfig(persist=False)
    sail_config = SailRepositoryConfig(sail_impl=memory_config)
    dataset_config = DatasetRepositoryConfig(delegate=sail_config)
    
    config = RepositoryConfig(
        repo_id=repo_id,
        title="Dataset Test Repository",
        impl=dataset_config
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation with named graph
            subject = IRI("http://example.com/dataset")
            predicate = IRI("http://example.com/inGraph")
            obj = Literal("graph_value")
            context = IRI("http://example.com/graph1")
            
            await repo.add_statement(subject, predicate, obj, context=context)
            
            # Verify the statement was added in the correct context
            statements = list(await repo.get_statements(contexts=[context]))
            assert len(statements) == 1
            assert statements[0].subject == subject
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_rdfs_inferencer_repository_creation(rdf4j_service: str):
    """Test creating a repository with SchemaCachingRDFSInferencerConfig."""
    repo_id = random_repo_id()
    memory_config = MemoryStoreConfig(persist=False)
    rdfs_config = SchemaCachingRDFSInferencerConfig(
        delegate=memory_config,
        iteration_cache_sync_threshold=8000
    )
    
    config = RepositoryConfig(
        repo_id=repo_id,
        title="RDFS Inferencer Test Repository",
        impl=SailRepositoryConfig(sail_impl=rdfs_config)
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation
            subject = IRI("http://example.com/rdfs")
            predicate = IRI("http://example.com/hasType")
            obj = IRI("http://example.com/TestClass")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added (RDFS inferencer adds many additional statements)
            statements = list(await repo.get_statements())
            assert len(statements) > 1  # Should have our statement plus many inferred statements
            
            # Check that our specific statement is in there
            our_statements = [s for s in statements if s.subject == subject and s.predicate == predicate and s.object == obj]
            assert len(our_statements) == 1
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_direct_type_hierarchy_inferencer_repository_creation(rdf4j_service: str):
    """Test creating a repository with DirectTypeHierarchyInferencerConfig."""
    repo_id = random_repo_id()
    memory_config = MemoryStoreConfig(persist=False)
    hierarchy_config = DirectTypeHierarchyInferencerConfig(
        delegate=memory_config,
        iteration_cache_sync_threshold=5000
    )
    
    config = RepositoryConfig(
        repo_id=repo_id,
        title="Direct Type Hierarchy Test Repository",
        impl=SailRepositoryConfig(sail_impl=hierarchy_config)
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation
            subject = IRI("http://example.com/hierarchy")
            predicate = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            obj = IRI("http://example.com/SomeType")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added (DirectTypeHierarchy inferencer may add additional statements)
            statements = list(await repo.get_statements())
            assert len(statements) >= 1  # Should have at least our statement
            
            # Check that our specific statement is in there
            our_statements = [s for s in statements if s.subject == subject and s.predicate == predicate and s.object == obj]
            assert len(our_statements) == 1
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_shacl_sail_repository_creation(rdf4j_service: str):
    """Test creating a repository with SHACLSailConfig."""
    repo_id = random_repo_id()
    memory_config = MemoryStoreConfig(persist=False)
    shacl_config = SHACLSailConfig(
        delegate=memory_config,
        parallel_validation=True,
        validation_enabled=True
    )
    
    config = RepositoryConfig(
        repo_id=repo_id,
        title="SHACL Sail Test Repository",
        impl=SailRepositoryConfig(sail_impl=shacl_config)
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation
            subject = IRI("http://example.com/shacl")
            predicate = IRI("http://example.com/validates")
            obj = Literal("shacl_test")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added
            statements = list(await repo.get_statements())
            assert len(statements) == 1
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_nested_inferencer_repository_creation(rdf4j_service: str):
    """Test creating a repository with nested inferencer configurations."""
    repo_id = random_repo_id()
    
    # Create a chain: Memory → RDFS Inferencer → Direct Type Hierarchy Inferencer
    memory_config = MemoryStoreConfig(persist=False)
    rdfs_config = SchemaCachingRDFSInferencerConfig(
        delegate=memory_config,
        iteration_cache_sync_threshold=8000
    )
    hierarchy_config = DirectTypeHierarchyInferencerConfig(
        delegate=rdfs_config,
        iteration_cache_sync_threshold=12000
    )
    
    config = RepositoryConfig(
        repo_id=repo_id,
        title="Nested Inferencer Test Repository",
        impl=SailRepositoryConfig(sail_impl=hierarchy_config)
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation
            subject = IRI("http://example.com/nested")
            predicate = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            obj = IRI("http://example.com/ComplexType")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added (nested inferencers add many additional statements)
            statements = list(await repo.get_statements())
            assert len(statements) > 1  # Should have our statement plus many inferred statements
            
            # Check that our specific statement is in there
            our_statements = [s for s in statements if s.subject == subject and s.predicate == predicate and s.object == obj]
            assert len(our_statements) == 1
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_repository_config_convenience_parameter(rdf4j_service: str):
    """Test creating a repository using the convenience sail_impl parameter."""
    repo_id = random_repo_id()
    
    # Use the convenience sail_impl parameter
    config = RepositoryConfig(
        repo_id=repo_id,
        title="Convenience Parameter Test Repository",
        sail_impl=MemoryStoreConfig(
            persist=True,
            sync_delay=2000
        )
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create the repository
            repo = await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
            # Test basic operation
            subject = IRI("http://example.com/convenience")
            predicate = IRI("http://example.com/usesConvenienceParam")
            obj = Literal("true")
            
            await repo.add_statement(subject, predicate, obj)
            
            # Verify the statement was added
            statements = list(await repo.get_statements())
            assert len(statements) == 1
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.skip(reason="Requires external SPARQL endpoint")
@pytest.mark.asyncio
async def test_sparql_repository_creation(rdf4j_service: str):
    """Test creating a repository with SPARQLRepositoryConfig.
    
    Note: This test is skipped because it requires an external SPARQL endpoint.
    In a real test environment, you would need to provide valid endpoints.
    """
    repo_id = random_repo_id()
    sparql_config = SPARQLRepositoryConfig(
        query_endpoint="http://example.com/sparql",
        update_endpoint="http://example.com/sparql/update"
    )
    
    config = RepositoryConfig(
        repo_id=repo_id,
        title="SPARQL Test Repository",
        impl=sparql_config
    )
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # This would create a SPARQL repository if endpoints were valid
            await db.create_repository(config=config)
            
            # Verify it was created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            assert repo_id in repo_ids
            
        finally:
            await db.delete_repository(repo_id)


@pytest.mark.asyncio
async def test_multiple_repositories_with_different_configs(rdf4j_service: str):
    """Test creating multiple repositories with different configurations simultaneously."""
    configs = [
        RepositoryConfig(
            repo_id=f"multi_memory_{randint(1, 1000000)}",
            title="Multi Test Memory Repository",
            sail_impl=MemoryStoreConfig(persist=False)
        ),
        RepositoryConfig(
            repo_id=f"multi_native_{randint(1, 1000000)}",
            title="Multi Test Native Repository", 
            impl=SailRepositoryConfig(
                sail_impl=NativeStoreConfig(triple_indexes="spoc")
            )
        ),
        RepositoryConfig(
            repo_id=f"multi_dataset_{randint(1, 1000000)}",
            title="Multi Test Dataset Repository",
            impl=DatasetRepositoryConfig(
                delegate=SailRepositoryConfig(
                    sail_impl=MemoryStoreConfig(persist=False)
                )
            )
        )
    ]
    
    async with AsyncRdf4j(rdf4j_service) as db:
        try:
            # Create all repositories
            for config in configs:
                await db.create_repository(config=config)
            
            # Verify all were created
            repos = await db.list_repositories()
            repo_ids = [r.id for r in repos]
            
            for config in configs:
                assert config.repo_id in repo_ids
                
                # Test basic operation on each
                repo = await db.get_repository(config.repo_id)
                subject = IRI(f"http://example.com/{config.repo_id}")
                predicate = IRI("http://example.com/testPredicate")
                obj = Literal(f"test_value_for_{config.repo_id}")
                
                await repo.add_statement(subject, predicate, obj)
                statements = list(await repo.get_statements())
                assert len(statements) == 1
                
        finally:
            # Clean up all repositories
            for config in configs:
                await db.delete_repository(config.repo_id)