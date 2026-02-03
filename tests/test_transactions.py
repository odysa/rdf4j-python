"""Tests for transaction support."""

import pytest
import pytest_asyncio

from rdf4j_python import (
    AsyncRdf4j,
    IsolationLevel,
    TransactionState,
    TransactionStateError,
)
from rdf4j_python.model.term import Literal, Quad
from rdf4j_python.model.vocabulary import EXAMPLE as ex
from rdf4j_python.model.vocabulary import RDF


@pytest_asyncio.fixture
async def txn_repo(rdf4j_service, random_mem_repo_config):
    """Create a fresh repository for transaction tests."""
    async with AsyncRdf4j(rdf4j_service) as db:
        repo = await db.create_repository(random_mem_repo_config)
        yield repo
        await db.delete_repository(random_mem_repo_config.repo_id)


class TestTransactionLifecycle:
    """Tests for transaction lifecycle (begin, commit, rollback)."""

    @pytest.mark.asyncio
    async def test_transaction_context_manager_commit(self, txn_repo):
        """Test that context manager commits on success."""
        quad = Quad(ex["s1"], RDF.type, ex["Thing"])

        async with txn_repo.transaction() as txn:
            assert txn.state == TransactionState.ACTIVE
            await txn.add_statements([quad])

        # Transaction should be committed
        assert txn.state == TransactionState.COMMITTED

        # Data should be persisted
        size = await txn_repo.size()
        assert size == 1

    @pytest.mark.asyncio
    async def test_transaction_context_manager_rollback_on_exception(self, txn_repo):
        """Test that context manager rolls back on exception."""
        quad = Quad(ex["s1"], RDF.type, ex["Thing"])

        with pytest.raises(ValueError):
            async with txn_repo.transaction() as txn:
                await txn.add_statements([quad])
                raise ValueError("Intentional error")

        # Transaction should be rolled back
        assert txn.state == TransactionState.ROLLED_BACK

        # Data should not be persisted
        size = await txn_repo.size()
        assert size == 0

    @pytest.mark.asyncio
    async def test_manual_transaction_commit(self, txn_repo):
        """Test manual transaction commit."""
        quad = Quad(ex["s1"], RDF.type, ex["Thing"])

        txn = txn_repo.transaction()
        assert txn.state == TransactionState.PENDING

        await txn.begin()
        assert txn.state == TransactionState.ACTIVE

        await txn.add_statements([quad])
        await txn.commit()
        assert txn.state == TransactionState.COMMITTED

        # Data should be persisted
        size = await txn_repo.size()
        assert size == 1

    @pytest.mark.asyncio
    async def test_manual_transaction_rollback(self, txn_repo):
        """Test manual transaction rollback."""
        quad = Quad(ex["s1"], RDF.type, ex["Thing"])

        txn = txn_repo.transaction()
        await txn.begin()
        await txn.add_statements([quad])
        await txn.rollback()
        assert txn.state == TransactionState.ROLLED_BACK

        # Data should not be persisted
        size = await txn_repo.size()
        assert size == 0


class TestTransactionOperations:
    """Tests for operations within transactions."""

    @pytest.mark.asyncio
    async def test_add_multiple_statements(self, txn_repo):
        """Test adding multiple statements in a transaction."""
        quads = [
            Quad(ex["s1"], RDF.type, ex["Person"]),
            Quad(ex["s1"], ex["name"], Literal("Alice")),
            Quad(ex["s2"], RDF.type, ex["Person"]),
            Quad(ex["s2"], ex["name"], Literal("Bob")),
        ]

        async with txn_repo.transaction() as txn:
            await txn.add_statements(quads)

        size = await txn_repo.size()
        assert size == 4

    @pytest.mark.asyncio
    async def test_add_statements_in_multiple_calls(self, txn_repo):
        """Test adding statements across multiple calls in same transaction."""
        quad1 = Quad(ex["s1"], RDF.type, ex["Thing"])
        quad2 = Quad(ex["s2"], RDF.type, ex["Thing"])

        async with txn_repo.transaction() as txn:
            await txn.add_statements([quad1])
            await txn.add_statements([quad2])

        size = await txn_repo.size()
        assert size == 2

    @pytest.mark.asyncio
    async def test_delete_statements(self, txn_repo):
        """Test deleting statements within a transaction."""
        quad1 = Quad(ex["s1"], RDF.type, ex["Thing"])
        quad2 = Quad(ex["s2"], RDF.type, ex["Thing"])

        # First add some data
        await txn_repo.add_statements([quad1, quad2])
        assert await txn_repo.size() == 2

        # Delete one in a transaction
        async with txn_repo.transaction() as txn:
            await txn.delete_statements([quad1])

        size = await txn_repo.size()
        assert size == 1

    @pytest.mark.asyncio
    async def test_sparql_update(self, txn_repo):
        """Test SPARQL UPDATE within a transaction."""
        # Add initial data
        quad = Quad(ex["s1"], ex["status"], Literal("draft"))
        await txn_repo.add_statements([quad])

        # Update via SPARQL in transaction
        async with txn_repo.transaction() as txn:
            await txn.update("""
                DELETE { ?s <http://example.org/status> "draft" }
                INSERT { ?s <http://example.org/status> "published" }
                WHERE { ?s <http://example.org/status> "draft" }
            """)

        # Verify the update
        results = await txn_repo.query(
            'SELECT ?status WHERE { <http://example.org/s1> <http://example.org/status> ?status }'
        )
        statuses = [str(row["status"]) for row in results]
        assert "published" in statuses[0]


class TestTransactionStateErrors:
    """Tests for transaction state error handling."""

    @pytest.mark.asyncio
    async def test_cannot_begin_twice(self, txn_repo):
        """Test that beginning a transaction twice raises error."""
        txn = txn_repo.transaction()
        await txn.begin()

        with pytest.raises(TransactionStateError):
            await txn.begin()

        await txn.rollback()

    @pytest.mark.asyncio
    async def test_cannot_commit_pending_transaction(self, txn_repo):
        """Test that committing a pending transaction raises error."""
        txn = txn_repo.transaction()

        with pytest.raises(TransactionStateError):
            await txn.commit()

    @pytest.mark.asyncio
    async def test_cannot_rollback_pending_transaction(self, txn_repo):
        """Test that rolling back a pending transaction raises error."""
        txn = txn_repo.transaction()

        with pytest.raises(TransactionStateError):
            await txn.rollback()

    @pytest.mark.asyncio
    async def test_cannot_commit_committed_transaction(self, txn_repo):
        """Test that committing an already committed transaction raises error."""
        async with txn_repo.transaction() as txn:
            pass  # Empty transaction, just commit

        with pytest.raises(TransactionStateError):
            await txn.commit()

    @pytest.mark.asyncio
    async def test_cannot_add_to_committed_transaction(self, txn_repo):
        """Test that adding to a committed transaction raises error."""
        async with txn_repo.transaction() as txn:
            pass

        with pytest.raises(TransactionStateError):
            await txn.add_statements([Quad(ex["s"], ex["p"], ex["o"])])

    @pytest.mark.asyncio
    async def test_cannot_add_to_rolled_back_transaction(self, txn_repo):
        """Test that adding to a rolled back transaction raises error."""
        txn = txn_repo.transaction()
        await txn.begin()
        await txn.rollback()

        with pytest.raises(TransactionStateError):
            await txn.add_statements([Quad(ex["s"], ex["p"], ex["o"])])


class TestTransactionAtomicity:
    """Tests for transaction atomicity guarantees."""

    @pytest.mark.asyncio
    async def test_rollback_discards_all_changes(self, txn_repo):
        """Test that rollback discards all changes made in the transaction."""
        quads = [
            Quad(ex["s1"], RDF.type, ex["Thing"]),
            Quad(ex["s2"], RDF.type, ex["Thing"]),
            Quad(ex["s3"], RDF.type, ex["Thing"]),
        ]

        txn = txn_repo.transaction()
        await txn.begin()
        await txn.add_statements(quads)
        await txn.rollback()

        # None of the data should be persisted
        size = await txn_repo.size()
        assert size == 0

    @pytest.mark.asyncio
    async def test_exception_rolls_back_all_changes(self, txn_repo):
        """Test that exception in context manager rolls back all changes."""
        quads_before_error = [
            Quad(ex["s1"], RDF.type, ex["Thing"]),
            Quad(ex["s2"], RDF.type, ex["Thing"]),
        ]

        with pytest.raises(RuntimeError):
            async with txn_repo.transaction() as txn:
                await txn.add_statements(quads_before_error)
                # Add more statements then error
                await txn.add_statements([Quad(ex["s3"], RDF.type, ex["Thing"])])
                raise RuntimeError("Simulated error")

        # All changes should be rolled back
        size = await txn_repo.size()
        assert size == 0


class TestTransactionIsolation:
    """Tests for transaction isolation levels."""

    @pytest.mark.asyncio
    async def test_transaction_with_isolation_level(self, txn_repo):
        """Test creating transaction with specific isolation level."""
        quad = Quad(ex["s1"], RDF.type, ex["Thing"])

        # Note: Not all stores support all isolation levels
        # This test just verifies the parameter is passed correctly
        async with txn_repo.transaction(IsolationLevel.SNAPSHOT) as txn:
            await txn.add_statements([quad])

        size = await txn_repo.size()
        assert size == 1

    @pytest.mark.asyncio
    async def test_transaction_default_isolation(self, txn_repo):
        """Test transaction with default isolation level."""
        quad = Quad(ex["s1"], RDF.type, ex["Thing"])

        async with txn_repo.transaction() as txn:
            await txn.add_statements([quad])

        size = await txn_repo.size()
        assert size == 1


class TestTransactionProperties:
    """Tests for transaction property accessors."""

    @pytest.mark.asyncio
    async def test_state_property(self, txn_repo):
        """Test state property reflects correct transaction state."""
        txn = txn_repo.transaction()
        assert txn.state == TransactionState.PENDING

        await txn.begin()
        assert txn.state == TransactionState.ACTIVE

        await txn.commit()
        assert txn.state == TransactionState.COMMITTED

    @pytest.mark.asyncio
    async def test_is_active_property(self, txn_repo):
        """Test is_active property."""
        txn = txn_repo.transaction()
        assert txn.is_active is False

        await txn.begin()
        assert txn.is_active is True

        await txn.commit()
        assert txn.is_active is False
