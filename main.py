"""
Implementation of an in-memory non thread-safe database with transactions.

The database is a dictionary of key-value pairs. The key is a string and the value is an integer.
The database is initialized with an empty dictionary.
The database supports transactions. A transaction begins with a BEGIN command and ends with a COMMIT or ROLLBACK command.
A transaction can be nested. A nested transaction is a transaction that is started while another transaction is active.
The database supports the following commands:
    BEGIN - starts a new transaction. If there is already a transaction active, it starts a nested transaction.
    ROLLBACK - rolls back the most recent transaction. If there is no transaction active, it returns an error.
    COMMIT - commits all of the active transactions. If there is no transaction active, it returns an error.
    SET name value - sets the variable name to the its integer value.
    GET name - returns the value of the variable name, or None if that variable is not set.
    DELETE name - unsets the variable name, making it just like that variable was never set.
    COUNT value - returns the number of variables that are currently set to value. If no variables equal that value, returns 0.

"""

from collections import defaultdict
from typing import Optional


class DBClient:
    def get(self, key: str) -> Optional[int]:
        raise NotImplementedError

    def set(self, key: str, value: int) -> None:
        raise NotImplementedError

    def delete(self, key: str) -> None:
        raise NotImplementedError

    def count(self, value: int) -> int:
        raise NotImplementedError


class InMemoryDBClient(DBClient):
    def __init__(self) -> None:
        self.data = {}
        self.counts = defaultdict(int)

    def get(self, key: str) -> Optional[int]:
        if key in self.data:
            return self.data[key]

        return None

    def set(self, key: str, value: int) -> None:
        if key in self.data:
            self.counts[self.data[key]] -= 1

        self.data[key] = value
        self.counts[value] += 1

    def delete(self, key: str) -> None:
        if key not in self.data:
            raise Exception(f"Key {key} not found")

        self.counts[self.data[key]] -= 1
        del self.data[key]

    def count(self, value: int) -> int:
        return self.counts[value]


class TransactionLogEntry:
    def __init__(self, key: str, old_value: Optional[int], new_value: Optional[int]) -> None:
        self.key = key
        self.old_value = old_value
        self.new_value = new_value

    def is_create_operation(self) -> bool:
        return self.old_value is None and self.new_value is not None


class TransactionsHandler:

    def __init__(self, client: DBClient) -> None:
        self.client = client
        self.transaction_log = []
        self.transaction_marker = "BEGIN"

    def begin(self) -> None:
        self.transaction_log.append(self.transaction_marker)

    def commit(self) -> None:
        if not self.transaction_active():
            raise Exception("No transaction to commit")

        self.transaction_log = []

    def rollback(self) -> None:
        if not self.transaction_active():
            raise Exception("No transaction to rollback")

        self.__apply_rollback__()
        self.transaction_log.pop()  # removes the transaction marker

    def begin(self) -> None:
        self.transaction_log.append(self.transaction_marker)

    def get(self, key: str) -> Optional[int]:
        return self.client.get(key)

    def set(self, key: str, value: int) -> None:
        if not self.transaction_active():
            self.client.set(key, value)
            return

        old_value = None if key not in self.client.data else self.client.data[key]
        self.transaction_log.append(TransactionLogEntry(key, old_value, value))
        self.client.set(key, value)

    def delete(self, key: str) -> None:
        if key not in self.client.data:
            raise Exception(f"Key {key} not found")

        if not self.transaction_active():
            self.client.delete(key)
            return

        old_value = self.client.data[key]
        self.transaction_log.append(TransactionLogEntry(key, old_value, None))
        self.client.delete(key)

    def count(self, value: int) -> int:
        return self.client.count(value)

    def transaction_active(self) -> bool:
        # truthy when transaction_log is not empty
        return self.transaction_log

    def __apply_rollback__(self) -> None:
        while self.transaction_log[-1] != self.transaction_marker:
            entry = self.transaction_log.pop()
            if entry.is_create_operation():
                self.client.delete(entry.key)
            else:
                self.client.set(entry.key, entry.old_value)
