from main import TransactionsHandler
from main import InMemoryDBClient
import pytest

def test_without_rollback():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.set("a", 1)
    handler.set("b", 2)
    handler.set("c", 3)
    assert client.data == {"a": 1, "b": 2, "c": 3}
    handler.delete("b")
    assert client.data == {"a": 1, "c": 3}
    handler.set("a", 4)
    assert client.data == {"a": 4, "c": 3}
    assert handler.count(4) == 1
    assert handler.count(6) == 0
    handler.set("c", 4)
    assert handler.count(4) == 2


def test_with_rollback():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.set("a", 1)
    handler.set("b", 2)
    handler.set("c", 3)
    assert client.data == {"a": 1, "b": 2, "c": 3}
    handler.begin()
    handler.set("b", 4)
    assert handler.get("b") == 4
    handler.set("c", 5)
    handler.rollback()
    assert client.data == {"a": 1, "b": 2, "c": 3}
    assert handler.count(4) == 0
    assert handler.count(5) == 0


def test_with_commit():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.set("a", 1)
    handler.set("b", 2)
    handler.set("c", 3)
    assert client.data == {"a": 1, "b": 2, "c": 3}
    handler.begin()
    handler.set("b", 4)
    assert handler.get("b") == 4
    handler.set("c", 5)
    handler.commit()
    assert client.data == {"a": 1, "b": 4, "c": 5}
    assert handler.count(4) == 1
    assert handler.count(5) == 1


def test_with_nested_transactions():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.set("a", 1)
    handler.set("b", 2)
    handler.set("c", 3)
    assert client.data == {"a": 1, "b": 2, "c": 3}
    handler.begin()
    handler.set("b", 4)
    assert handler.get("b") == 4
    handler.set("c", 5)
    handler.begin()
    handler.set("c", 6)
    handler.rollback()
    assert client.data == {"a": 1, "b": 4, "c": 5}
    assert handler.count(4) == 1
    assert handler.count(5) == 1
    assert handler.count(6) == 0
    handler.commit()
    assert client.data == {"a": 1, "b": 4, "c": 5}
    assert handler.count(4) == 1
    assert handler.count(5) == 1
    assert handler.count(6) == 0

def test_delete_unknown_key_raises_exception():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    with pytest.raises(Exception):
        handler.delete("a")

def test_rollback_without_transaction_raises_exception():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    with pytest.raises(Exception):
        handler.rollback()

def test_commit_without_transaction_raises_exception():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    with pytest.raises(Exception):
        handler.commit()

def test_rollback_with_empty_transaction_does_nothing():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.begin()
    handler.rollback()
    assert client.data == {}

def test_commit_with_empty_transaction_does_nothing():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.begin()
    handler.commit()
    assert client.data == {}

def test_rollback_with_multiple_transactions():
    client = InMemoryDBClient()
    handler = TransactionsHandler(client)
    handler.begin()
    handler.set("a", 1)
    handler.set("b", 2)
    handler.set("c", 3)
    handler.begin()
    handler.set("b", 4)
    handler.set("c", 5)
    handler.rollback()
    assert client.data == {"a": 1, "b": 2, "c": 3}
    handler.rollback()
    assert client.data == {}
