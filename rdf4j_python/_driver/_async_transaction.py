"""Async transaction support for RDF4J repositories."""

from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Iterable, Optional, Self

import httpx

from rdf4j_python.exception import TransactionError, TransactionStateError
from rdf4j_python.model.term import Quad, Triple
from rdf4j_python.utils.const import Rdf4jContentType
from rdf4j_python.utils.helpers import serialize_statements

if TYPE_CHECKING:
    from rdf4j_python._client import AsyncApiClient


class TransactionState(Enum):
    """Represents the state of a transaction."""

    PENDING = "pending"  # Transaction not yet started
    ACTIVE = "active"  # Transaction is active
    COMMITTED = "committed"  # Transaction has been committed
    ROLLED_BACK = "rolled_back"  # Transaction has been rolled back


class IsolationLevel(Enum):
    """Transaction isolation levels supported by RDF4J.

    Note: Not all RDF4J store implementations support all isolation levels.
    If an unsupported level is requested, the store's default will be used.
    """

    NONE = "NONE"
    READ_UNCOMMITTED = "READ_UNCOMMITTED"
    READ_COMMITTED = "READ_COMMITTED"
    SNAPSHOT_READ = "SNAPSHOT_READ"
    SNAPSHOT = "SNAPSHOT"
    SERIALIZABLE = "SERIALIZABLE"


class AsyncTransaction:
    """Async context manager for transactional operations on an RDF4J repository.

    Transactions allow grouping multiple operations (add, delete, update) into
    a single atomic unit. Either all operations succeed (commit) or none of them
    take effect (rollback).

    Usage as context manager (recommended):
        ```python
        async with repo.transaction() as txn:
            await txn.add_statements([quad1, quad2])
            await txn.add_statements([quad3])
            # Auto-commits on success, auto-rollbacks on exception
        ```

    Manual usage:
        ```python
        txn = repo.transaction()
        await txn.begin()
        try:
            await txn.add_statements([quad1, quad2])
            await txn.commit()
        except Exception:
            await txn.rollback()
            raise
        ```

    Attributes:
        state: Current state of the transaction (PENDING, ACTIVE, COMMITTED, ROLLED_BACK)
    """

    _client: "AsyncApiClient"
    _repository_id: str
    _transaction_id: Optional[str]
    _isolation_level: Optional[IsolationLevel]
    _state: TransactionState

    def __init__(
        self,
        client: "AsyncApiClient",
        repository_id: str,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        """Initialize a transaction.

        Args:
            client: The async API client.
            repository_id: The repository ID.
            isolation_level: Optional isolation level for the transaction.
        """
        self._client = client
        self._repository_id = repository_id
        self._transaction_id = None
        self._isolation_level = isolation_level
        self._state = TransactionState.PENDING

    @property
    def state(self) -> TransactionState:
        """Returns the current state of the transaction."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns True if the transaction is active."""
        return self._state == TransactionState.ACTIVE

    async def __aenter__(self) -> Self:
        """Start the transaction when entering the context."""
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Commit or rollback the transaction when exiting the context.

        If an exception occurred, the transaction is rolled back.
        Otherwise, it is committed.
        """
        if self._state != TransactionState.ACTIVE:
            # Transaction already ended (manually committed/rolled back)
            return

        if exc_type is not None:
            # An exception occurred, rollback
            await self.rollback()
        else:
            # No exception, commit
            await self.commit()

    async def begin(self) -> None:
        """Start the transaction.

        Raises:
            TransactionStateError: If the transaction has already been started.
            TransactionError: If the server fails to create the transaction.
        """
        if self._state != TransactionState.PENDING:
            raise TransactionStateError(
                f"Cannot begin transaction: already in state {self._state.value}"
            )

        path = f"/repositories/{self._repository_id}/transactions"
        params = {}
        if self._isolation_level is not None:
            params["isolation-level"] = self._isolation_level.value

        response = await self._client.post(path, headers={}, params=params)

        if response.status_code != httpx.codes.CREATED:
            raise TransactionError(
                f"Failed to start transaction: {response.status_code} - {response.text}"
            )

        # Extract transaction ID from Location header
        location = response.headers.get("Location")
        if not location:
            raise TransactionError(
                "Server did not return transaction ID in Location header"
            )

        # Location is like: /rdf4j-server/repositories/{id}/transactions/{txn-id}
        self._transaction_id = location.rstrip("/").split("/")[-1]
        self._state = TransactionState.ACTIVE

    async def commit(self) -> None:
        """Commit the transaction, making all changes permanent.

        Raises:
            TransactionStateError: If the transaction is not active.
            TransactionError: If the commit fails.
        """
        if self._state != TransactionState.ACTIVE:
            raise TransactionStateError(
                f"Cannot commit transaction: in state {self._state.value}"
            )

        path = f"/repositories/{self._repository_id}/transactions/{self._transaction_id}"
        params = {"action": "COMMIT"}

        response = await self._client.put(path, params=params)

        if response.status_code not in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            raise TransactionError(
                f"Failed to commit transaction: {response.status_code} - {response.text}"
            )

        self._state = TransactionState.COMMITTED

    async def rollback(self) -> None:
        """Rollback the transaction, discarding all changes.

        Raises:
            TransactionStateError: If the transaction is not active.
            TransactionError: If the rollback fails.
        """
        if self._state != TransactionState.ACTIVE:
            raise TransactionStateError(
                f"Cannot rollback transaction: in state {self._state.value}"
            )

        path = f"/repositories/{self._repository_id}/transactions/{self._transaction_id}"

        response = await self._client.delete(path)

        if response.status_code not in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            raise TransactionError(
                f"Failed to rollback transaction: {response.status_code} - {response.text}"
            )

        self._state = TransactionState.ROLLED_BACK

    def _ensure_active(self) -> None:
        """Ensure the transaction is active before performing operations."""
        if self._state != TransactionState.ACTIVE:
            raise TransactionStateError(
                f"Cannot perform operation: transaction in state {self._state.value}"
            )

    async def add_statements(
        self, statements: Iterable[Quad] | Iterable[Triple]
    ) -> None:
        """Add statements to the repository within this transaction.

        Args:
            statements: The RDF statements to add.

        Raises:
            TransactionStateError: If the transaction is not active.
            TransactionError: If the operation fails.
        """
        self._ensure_active()

        path = f"/repositories/{self._repository_id}/transactions/{self._transaction_id}"
        params = {"action": "ADD"}
        headers = {"Content-Type": Rdf4jContentType.NQUADS}

        response = await self._client.put(
            path,
            content=serialize_statements(statements),
            params=params,
            headers=headers,
        )

        if response.status_code not in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            raise TransactionError(
                f"Failed to add statements: {response.status_code} - {response.text}"
            )

    async def delete_statements(
        self, statements: Iterable[Quad] | Iterable[Triple]
    ) -> None:
        """Delete specific statements from the repository within this transaction.

        Args:
            statements: The RDF statements to delete.

        Raises:
            TransactionStateError: If the transaction is not active.
            TransactionError: If the operation fails.
        """
        self._ensure_active()

        path = f"/repositories/{self._repository_id}/transactions/{self._transaction_id}"
        params = {"action": "DELETE"}
        headers = {"Content-Type": Rdf4jContentType.NQUADS}

        response = await self._client.put(
            path,
            content=serialize_statements(statements),
            params=params,
            headers=headers,
        )

        if response.status_code not in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            raise TransactionError(
                f"Failed to delete statements: {response.status_code} - {response.text}"
            )

    async def update(self, sparql_update: str) -> None:
        """Execute a SPARQL UPDATE within this transaction.

        Args:
            sparql_update: The SPARQL UPDATE query string.

        Raises:
            TransactionStateError: If the transaction is not active.
            TransactionError: If the operation fails.
        """
        self._ensure_active()

        path = f"/repositories/{self._repository_id}/transactions/{self._transaction_id}"
        params = {"action": "UPDATE"}
        headers = {"Content-Type": Rdf4jContentType.SPARQL_UPDATE}

        response = await self._client.put(
            path,
            content=sparql_update,
            params=params,
            headers=headers,
        )

        if response.status_code not in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            raise TransactionError(
                f"Failed to execute update: {response.status_code} - {response.text}"
            )
