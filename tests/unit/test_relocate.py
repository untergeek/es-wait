"""Unit tests for Relocate"""

# pylint: disable=W0621
from unittest.mock import MagicMock
import pytest
from elasticsearch8 import TransportError
from es_wait.relocate import Relocate
from es_wait.exceptions import ExceptionCount


@pytest.fixture
def relocate():
    """Create a Relocate instance with a mocked client."""
    client = MagicMock()
    return Relocate(client, name="test_index", pause=0.5, timeout=2.0, max_exceptions=2)


def test_relocate_init(relocate):
    """Test Relocate initialization."""
    assert relocate.name == "test_index"
    assert relocate.pause == 0.5
    assert relocate.timeout == 2.0
    assert relocate.max_exceptions == 2


def test_routing_table_success(relocate):
    """Test routing_table retrieves shard data correctly."""
    relocate.client.cluster.state.return_value = {
        "routing_table": {
            "indices": {
                "test_index": {
                    "shards": {"0": [{"state": "STARTED"}], "1": [{"state": "STARTED"}]}
                }
            }
        }
    }
    assert relocate.routing_table() == {
        "0": [{"state": "STARTED"}],
        "1": [{"state": "STARTED"}],
    }


def test_routing_table_transport_error(relocate):
    """Test routing_table handles TransportError."""
    relocate.client.cluster.state.side_effect = TransportError("Connection failed")
    relocate.routing_table()
    assert relocate.exceptions_raised == 1


def test_finished_state_true(relocate):
    """Test finished_state returns True when all shards are STARTED."""
    _ = {
        "0": [{"state": "STARTED"}],
        "1": [{"state": "STARTED"}],
    }
    relocate.routing_table = MagicMock(return_value=_)
    assert relocate.finished_state


def test_finished_state_false(relocate):
    """Test finished_state returns False when shards are not STARTED."""
    _ = {
        "0": [{"state": "STARTED"}],
        "1": [{"state": "INITIALIZING"}],
    }
    relocate.routing_table = MagicMock(return_value=_)
    assert not relocate.finished_state


def test_check_success(relocate):
    """Test check returns True when relocation is finished."""
    _ = {
        "0": [{"state": "STARTED"}],
        "1": [{"state": "STARTED"}],
    }
    relocate.routing_table = MagicMock(return_value=_)
    assert relocate.check() is True


def test_check_exception_count(relocate):
    """Test check increments exceptions_raised and raises ExceptionCount."""
    relocate.client.cluster.state.side_effect = TransportError("Error")
    with pytest.raises(ExceptionCount) as exc_info:
        for _ in range(3):  # Exceed max_exceptions (2)
            relocate.check()
    assert relocate.exceptions_raised == 2
    assert exc_info.value.count == 2


def test_routing_table_key_error(relocate):
    """Test routing_table handles KeyError"""
    relocate.client.cluster.state.return_value = {
        "routing_table": {
            "indices": {
                "other_index": {
                    "shards": {
                        "0": [{"state": "STARTED"}],
                        "1": [{"state": "STARTED"}],
                    }
                }
            }
        }
    }
    relocate.routing_table()
    assert relocate.exceptions_raised == 1


def test_finished_state_key_error(relocate):
    """Test finished_state handles KeyError"""
    relocate.routing_table = MagicMock(return_value={"0": [{"state": "STARTED"}]})
    relocate.routing_table.return_value["0"][0].pop("state")
    assert not relocate.finished_state
    assert relocate.exceptions_raised == 1


def test_check_key_error(relocate):
    """Test check handles KeyError"""
    relocate.routing_table = MagicMock(return_value={"0": [{"state": "STARTED"}]})
    relocate.routing_table.return_value["0"][0].pop("state")
    assert not relocate.check()
    assert relocate.exceptions_raised == 1


def test_check_transport_error(relocate):
    """Test check handles TransportError"""
    relocate.routing_table = MagicMock(side_effect=TransportError("Connection failed"))
    assert not relocate.check()
    assert relocate.exceptions_raised == 1
