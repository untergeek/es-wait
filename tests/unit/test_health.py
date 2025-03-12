"""Unit tests for Health"""

# pylint: disable=W0621
from elasticsearch8.exceptions import TransportError
from es_wait.health import Health


# Test initialization with default values
def test_initialization_defaults(health_client):
    """Test that Health initializes with default parameters correctly."""
    health = Health(health_client)
    assert health.check_type == "status"
    assert health.waitstr == "for cluster health to show green status"
    assert health.indices == "*"
    assert health.check_for == {"status": "green"}
    assert health.do_health_report is True


# Test initialization with 'relocation' check_type
def test_initialization_relocation(health_client):
    """Test initialization with 'relocation' check_type."""
    health = Health(health_client, check_type="relocation")
    assert health.check_type == "relocation"
    assert health.waitstr == "for cluster health to show zero relocating shards"
    assert health.check_for == {"relocating_shards": 0}


# Test initialization with custom check_for
def test_initialization_custom_check_for(health_client):
    """Test that a custom check_for overrides the default."""
    custom_check = {"status": "yellow"}
    health = Health(health_client, check_type="status", check_for=custom_check)
    assert health.check_for == custom_check
    assert health.waitstr == "for cluster health to show green status"  # Unchanged


# Test initialization with indices as a list
def test_initialization_indices_list(health_client):
    """Test that a list of indices is joined correctly."""
    health = Health(health_client, indices=["index1", "index2"])
    assert health.indices == "index1,index2"


# Test initialization with indices as a string
def test_initialization_indices_string(health_client):
    """Test that a string index is set correctly."""
    health = Health(health_client, indices="index1")
    assert health.indices == "index1"


# Test initialization with 'cluster_routing' and indices, expecting a warning
def test_initialization_cluster_routing_with_indices(health_client, caplog):
    """Test that 'cluster_routing' ignores indices and logs a warning."""
    health = Health(health_client, check_type="cluster_routing", indices=["index1"])
    assert health.check_type == "cluster_routing"
    assert health.indices == "index1"  # Set but ignored in check
    assert (
        "For 'cluster_routing', 'indices' is ignored. Checking all indices."
        in caplog.text
    )


# Test check method for 'status' with green status
def test_check_status_green(health_client):
    """Test that check returns True when status is green."""
    health = Health(health_client, check_type="status")
    assert health.check() is True
    health_client.cluster.health.assert_called_with(index="*", filter_path="status")


# Test check method for 'relocation' with zero relocating shards
def test_check_relocation_zero(health_client):
    """Test that check returns True when relocating_shards is 0."""
    health_client.cluster.health.return_value = {"relocating_shards": 0}
    health = Health(health_client, check_type="relocation", indices="index1")
    assert health.check() is True
    health_client.cluster.health.assert_called_with(
        index="index1", filter_path="relocating_shards"
    )


# Test check method for 'cluster_routing', ignoring indices
def test_check_cluster_routing(health_client):
    """Test that check uses '*' for cluster_routing despite indices."""
    health_client.cluster.health.return_value = {"relocating_shards": 0}
    health = Health(health_client, check_type="cluster_routing", indices="index1")
    assert health.check() is True
    health_client.cluster.health.assert_called_with(
        index="*", filter_path="relocating_shards"
    )


# Test check method when health check fails
def test_check_failure(health_client):
    """Test that check returns False when condition is not met."""
    health_client.cluster.health.return_value = {"status": "yellow"}
    health = Health(health_client, check_type="status")
    assert health.check() is False


# Test check method handling TransportError
def test_check_transport_error(health_client, caplog):
    """Test that check handles TransportError and logs an error."""
    health_client.cluster.health.side_effect = TransportError("connection error")
    health = Health(health_client)
    assert health.check() is False
    assert "Error checking health:" in caplog.text


# Test filter_path property with multiple keys
def test_filter_path_multiple_keys(health_client):
    """Test that filter_path is correctly generated from check_for keys."""
    health = Health(
        health_client, check_for={"status": "green", "relocating_shards": 0}
    )
    assert health.filter_path == "status,relocating_shards"


# Test check method with multiple keys, all conditions met
def test_check_multiple_keys_success(health_client):
    """Test that check returns True when all conditions are met."""
    health_client.cluster.health.return_value = {
        "status": "green",
        "relocating_shards": 0,
    }
    health = Health(
        health_client, check_for={"status": "green", "relocating_shards": 0}
    )
    assert health.check() is True
    health_client.cluster.health.assert_called_with(
        index="*", filter_path="status,relocating_shards"
    )


# Test check method with multiple keys, one condition fails
def test_check_multiple_keys_failure(health_client):
    """Test that check returns False when one condition fails."""
    health_client.cluster.health.return_value = {
        "status": "green",
        "relocating_shards": 1,
    }
    health = Health(
        health_client, check_for={"status": "green", "relocating_shards": 0}
    )
    assert health.check() is False
