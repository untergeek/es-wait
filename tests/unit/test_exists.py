"""Unit tests for Exists"""

import pytest
from elasticsearch8.exceptions import TransportError
from es_wait.exists import Exists


# Test initialization
# - Test default initialization
def test_initialization_defaults(exists_client):
    """
    Test that Exists initializes with default kind='index' and correct attributes.
    """
    exists = Exists(exists_client, name="test_index")
    assert exists.kind == "index"
    assert exists.name == "test_index"
    assert exists.waitstr == 'for index "test_index" to exist'


# - Test invalid kind raises ValueError
def test_initialization_invalid_kind(exists_client):
    """Test that an invalid kind raises ValueError with a descriptive message."""
    with pytest.raises(
        ValueError,
        match=(
            'kind must be one of index, data_stream, index_template, '
            'component_template'
        ),
    ):
        Exists(exists_client, name="test", kind="invalid_kind")


# - Test name=None raises ValueError
def test_initialization_name_none(exists_client):
    """Test that name=None raises ValueError via _ensure_not_none."""
    with pytest.raises(ValueError, match="Keyword arg name cannot be None"):
        Exists(exists_client, name=None)


# Test func_map method
@pytest.mark.parametrize(
    "kind, expected_func_name, expected_kwargs",
    [
        ("index", "exists", {"index": "test_index"}),
        ("data_stream", "exists", {"index": "test_data_stream"}),
        ("index_template", "exists_index_template", {"name": "test_template"}),
        (
            "component_template",
            "exists_component_template",
            {"name": "test_component"},
        ),
    ],
)
def test_func_map(exists_client, kind, expected_func_name, expected_kwargs):
    """Test that func_map returns the correct function and kwargs for each kind."""
    exists = Exists(
        exists_client,
        name=expected_kwargs.get("index", expected_kwargs.get("name")),
        kind=kind,
    )
    func, kwargs = exists.func_map()
    # str(func) returns a string representation of the MagicMock object
    # >>> s = "<MagicMock name='mock.cluster.exists_component_template' id='1234'>"
    # >>> s.split("'")[1]
    # 'mock.cluster.exists_component_template'
    # >>> s.split("'")[1].split('.')[2]
    # 'exists_component_template'
    assert str(func).split("'")[1].split('.')[2] == expected_func_name
    assert kwargs == expected_kwargs


# Test check method
# - Test successful existence check
@pytest.mark.parametrize(
    "kind", ["index", "data_stream", "index_template", "component_template"]
)
def test_check_success(exists_client, kind):
    """Test that check returns True when the entity exists for each kind."""
    exists = Exists(exists_client, name="test", kind=kind)
    assert exists.check() is True


# - Test TransportError handling
def test_check_transport_error(exists_client, caplog):
    """Test that check handles TransportError, logs an error, and returns False."""
    exists_client.indices.exists.side_effect = TransportError("connection error")
    exists = Exists(exists_client, name="test_index", kind="index")
    assert exists.check() is False
    assert "Error checking for index \"test_index\"" in caplog.text


# - Test when entity does not exist
def test_check_entity_not_exist(exists_client):
    """Test that check returns False when the entity does not exist."""
    exists_client.indices.exists.return_value = False
    exists = Exists(exists_client, name="non_existent_index", kind="index")
    assert exists.check() is False
