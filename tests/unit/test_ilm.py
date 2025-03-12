"""Unit tests for IndexLifecycle"""

from unittest.mock import MagicMock, patch
import pytest
from elasticsearch8.exceptions import NotFoundError
from es_wait.ilm import IndexLifecycle, IlmPhase, IlmStep
from es_wait.exceptions import IlmWaitError

# --- IndexLifecycle Tests ---


def test_indexlifecycle_init():
    """Test that IndexLifecycle initializes with the correct name."""
    client = MagicMock()
    ilm = IndexLifecycle(client, name="test_index")
    assert ilm.name == "test_index"


def test_indexlifecycle_init_none_name():
    """Test that initializing with name=None raises ValueError."""
    client = MagicMock()
    with pytest.raises(ValueError, match="Keyword arg name cannot be None"):
        IndexLifecycle(client, name=None)


def test_get_explain_data_success():
    """Test successful retrieval of ILM explain data."""
    client = MagicMock()
    client.ilm.explain_lifecycle.return_value = {
        "indices": {"test_index": {"phase": "hot"}}
    }
    ilm = IndexLifecycle(client, name="test_index")
    data = ilm.get_explain_data()
    assert data == {"phase": "hot"}


def test_get_explain_data_not_found(meta404):
    """Test handling of NotFoundError when the index doesn't exist."""
    client = MagicMock()
    client.ilm.explain_lifecycle.side_effect = NotFoundError(
        "not found", meta=meta404, body="not found"
    )
    ilm = IndexLifecycle(client, name="test_index")
    with pytest.raises(NotFoundError):
        ilm.get_explain_data()


def test_get_explain_data_generic_error():
    """Test handling of a generic exception, raising IlmWaitError."""
    client = MagicMock()
    client.ilm.explain_lifecycle.side_effect = Exception("generic error")
    ilm = IndexLifecycle(client, name="test_index")
    with pytest.raises(IlmWaitError):
        ilm.get_explain_data()


# --- IlmPhase Tests ---


def test_ilmphase_init():
    """Test that IlmPhase initializes with the correct phase and waitstr."""
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="hot")
    assert ilm.phase == "hot"
    assert ilm.waitstr == 'for "test_index" to complete ILM transition to phase "hot"'


def test_ilmphase_init_none_phase():
    """Test that initializing with phase=None raises ValueError."""
    client = MagicMock()
    with pytest.raises(ValueError, match="Keyword arg phase cannot be None"):
        IlmPhase(client, name="test_index", phase=None)


def test_ilmphase_check_match():
    """Test check returns True when the current phase matches the target phase."""
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="hot")
    with patch.object(ilm, 'get_explain_data', return_value={"phase": "hot"}):
        assert ilm.check() is True


def test_ilmphase_check_new_and_higher():
    """
    Test check returns True when target phase is 'new' and current phase is
    higher.
    """
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="new")
    with patch.object(ilm, 'get_explain_data', return_value={"phase": "hot"}):
        assert ilm.check() is True


def test_ilmphase_check_no_match():
    """
    Test check returns False when the current phase does not match the target
    phase.
    """
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="warm")
    with patch.object(ilm, 'get_explain_data', return_value={"phase": "hot"}):
        assert ilm.check() is False


def test_ilmphase_check_no_data():
    """Test check returns False when no ILM explain data is found."""
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="hot")
    with patch.object(ilm, 'get_explain_data', return_value=None):
        assert ilm.check() is False


def test_ilmphase_check_no_phase_key():
    """Test check returns False when the 'phase' key is missing in the explain data."""
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="hot")
    with patch.object(ilm, 'get_explain_data', return_value={"foo": "bar"}):
        assert ilm.check() is False


def test_phase_by_num():
    """Test phase_by_num maps known phases to numbers and unknowns to 0."""
    ilm = IlmPhase(MagicMock(), name="test", phase="hot")
    assert ilm.phase_by_num("new") == 1
    assert ilm.phase_by_num("hot") == 2
    assert ilm.phase_by_num("unknown") == 0


def test_phase_by_name():
    """Test phase_by_name maps known numbers to phases and unknowns to 'undef'."""
    ilm = IlmPhase(MagicMock(), name="test", phase="hot")
    assert ilm.phase_by_name(1) == "new"
    assert ilm.phase_by_name(2) == "hot"
    assert ilm.phase_by_name(0) == "undef"


# --- IlmStep Tests ---


def test_ilmstep_init():
    """Test that IlmStep initializes with the correct waitstr."""
    client = MagicMock()
    ilm = IlmStep(client, name="test_index")
    assert ilm.waitstr == 'for "test_index" to complete the current ILM step'


def test_ilmstep_check_complete():
    """Test check returns True when both action and step are 'complete'."""
    client = MagicMock()
    ilm = IlmStep(client, name="test_index")
    with patch.object(
        ilm, 'get_explain_data', return_value={"action": "complete", "step": "complete"}
    ):
        assert ilm.check() is True


def test_ilmstep_check_not_complete():
    """Test check returns False when action or step is not 'complete'."""
    client = MagicMock()
    ilm = IlmStep(client, name="test_index")
    with patch.object(
        ilm,
        'get_explain_data',
        return_value={"action": "in_progress", "step": "in_progress"},
    ):
        assert ilm.check() is False


def test_ilmstep_check_not_found_index_exists(meta404):
    """Test check returns False when NotFoundError is raised but the index exists."""
    client = MagicMock()
    client.indices.exists.return_value = True
    ilm = IlmStep(client, name="test_index")
    with patch.object(
        ilm,
        'get_explain_data',
        side_effect=NotFoundError("not found", meta=meta404, body="not found"),
    ):
        assert ilm.check() is False


def test_ilmstep_check_not_found_index_not_exist(meta404):
    """Test check raises NotFoundError when the index does not exist."""
    client = MagicMock()
    client.indices.exists.return_value = False
    ilm = IlmStep(client, name="test_index")
    with patch.object(
        ilm,
        'get_explain_data',
        side_effect=NotFoundError("not found", meta=meta404, body="not found"),
    ):
        with pytest.raises(NotFoundError):
            ilm.check()
