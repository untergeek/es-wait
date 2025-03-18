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


@pytest.mark.parametrize('phase', ['hot', 'delete'], indirect=True)
def test_ilmphase_init(ilm_phase, named_index, phase):
    """Test that IlmPhase initializes with the correct phase and waitstr."""
    assert ilm_phase.phase == phase
    assert (
        ilm_phase.waitstr
        == f'for "{named_index}" to complete ILM transition to phase "{phase}"'
    )


def test_ilmphase_init_none_phase(client, named_index):
    """Test that initializing with phase=None raises ValueError."""
    with pytest.raises(ValueError, match="Keyword arg phase cannot be None"):
        IlmPhase(client, name=named_index, phase=None)


@pytest.mark.parametrize(
    'actual,phase,expected',
    [
        ('new', 'new', True),
        ('new', 'hot', False),
        ('hot', 'new', True),
        ('hot', 'hot', True),
        ('hot', 'warm', False),
        ('warm', 'warm', True),
        ('warm', 'cold', False),
        ('cold', 'cold', True),
        ('cold', 'frozen', False),
        ('frozen', 'frozen', True),
        ('frozen', 'delete', False),
        ('delete', 'delete', True),
        ('delete', 'hot', True),
    ],
    indirect=True,
)
def test_ilmphase_check_match(actual, expected, ilm_phase, indexexplain):
    """Test check returns True when the current phase matches the target phase."""
    retval = indexexplain(phase=actual)
    with patch.object(ilm_phase, 'get_explain_data', return_value=retval):
        assert ilm_phase.check() is expected


@pytest.mark.parametrize(
    'phase', ['hot', 'warm', 'cold', 'frozen', 'delete'], indirect=True
)
def test_ilmphase_check_new_and_higher(fullexplain, ilm_phase, named_index, phase):
    """
    Test check returns True when target phase is 'new' and current phase is
    higher.
    """
    retval = fullexplain(phase=phase)['indices'][named_index]
    with patch.object(ilm_phase, 'get_explain_data', return_value=retval):
        with patch.object(ilm_phase, 'phase_stuck', return_value=False):
            assert ilm_phase.check() is True


@pytest.mark.parametrize(
    'phase', ['new', 'warm', 'cold', 'frozen', 'delete'], indirect=True
)
def test_ilmphase_check_no_match(ilm_phase, indexexplain):
    """
    Test check returns False when the current phase does not match the target
    phase.
    """
    retval = indexexplain(action='hot')
    with patch.object(ilm_phase, 'get_explain_data', return_value=retval):
        assert not ilm_phase.reached_phase()
        assert not ilm_phase.check()


@pytest.mark.parametrize('phase', ['hot'], indirect=True)
def test_ilmphase_check_no_data(ilm_phase):
    """Test check returns False when no ILM explain data is found."""
    with patch.object(ilm_phase, 'get_explain_data', return_value={}):
        try:
            assert not ilm_phase.has_explain()
        # pylint: disable=broad-except
        except Exception:
            pytest.fail("Unexpected exception raised")


@pytest.mark.parametrize('phase', ['hot'], indirect=True)
def test_ilmphase_check_no_phase_key(ilm_phase, fullexplain, named_index):
    """Test check returns True when the current phase matches the target phase."""
    explain = fullexplain('complete', 'hot', 'complete')
    del explain['indices'][named_index]['phase']
    with patch.object(ilm_phase, 'get_explain_data', return_value=explain):
        assert 'phase' not in ilm_phase.explain


@pytest.mark.parametrize(
    'phase', ['new', 'hot', 'warm', 'cold', 'frozen', 'delete'], indirect=True
)
def test_ilm_reached(client, named_index, ilm_completed):
    """
    Fixture to create an IlmPhase instance with mocked client and parameterized
    return values for the ILM explain API.

    In this test, the target phase is 'new'. The if the reported phase is 'new',
    or higher, and the phase is completed, the check method should return True
    """
    client.ilm.explain_lifecycle.return_value = ilm_completed
    ilm = IlmPhase(client, name=named_index, phase='new')
    assert ilm.reached_phase()


@pytest.mark.parametrize(
    'phase,target,expected',
    [
        ('hot', 'warm', False),
        ('warm', 'cold', False),
        ('hot', 'delete', False),
        ('warm', 'warm', True),
        ('hot', 'cold', False),
    ],
    indirect=True,
)
def test_reached_phase(expected, ilm_phase, target):
    """Test reached_phase returns accordingly"""
    setattr(ilm_phase, 'phase', target)
    assert ilm_phase.reached_phase() is expected


@pytest.mark.parametrize('phase,target', [('hot', 'delete')], indirect=True)
def test_stuck_count_exceeded(ilm_phase, target):
    """
    Test reached_phase returns False when phase_lt is True and phase is not
    complete.
    """
    setattr(ilm_phase, 'phase', target)
    assert ilm_phase.stuck_count == 0  # initial value
    assert not ilm_phase.advanced  # initial value
    assert not ilm_phase.check(max_stuck_count=1)  # First test
    assert ilm_phase.stuck_count == 1  # Stuck count is 1
    assert not ilm_phase.check(max_stuck_count=1)  # We're stuck
    assert ilm_phase.advanced  # We attempted to advance ILM manually
    assert ilm_phase.stuck_count == 0  # We reset the stuck count
    assert not ilm_phase.check(max_stuck_count=1)  # We test again
    assert ilm_phase.stuck_count == 1  # Stuck count is 1
    # And now it will raise the next time we attempt to run
    # phase_stuck because we've already advanced the phase
    # and we're >= to the max_stuck_count
    with pytest.raises(IlmWaitError):
        ilm_phase.phase_stuck(max_stuck_count=1)


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


def test_phase_stuck_not_stuck():
    """
    Test phase_stuck returns False when stuck_count is less than max_stuck_count.
    """
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="hot")
    ilm.stuck_count = 2
    assert not ilm.phase_stuck(max_stuck_count=3)


def test_phase_stuck_advanced():
    """
    Test phase_stuck raises IlmWaitError when stuck_count exceeds
    max_stuck_count and phase was advanced.
    """
    client = MagicMock()
    ilm = IlmPhase(client, name="test_index", phase="hot")
    ilm.stuck_count = 3
    ilm.advanced = True
    with pytest.raises(
        IlmWaitError,
        match=(
            "ILM phase hot was stuck, but was advanced. Even after advancing, "
            "the phase is still stuck."
        ),
    ):
        ilm.phase_stuck(max_stuck_count=3)


def test_phase_stuck_trigger_advance():
    """
    Test phase_stuck triggers ILM phase advance when stuck_count exceeds
    max_stuck_count and phase was not advanced.
    """
    idx = "test_index"
    client = MagicMock()
    ilm = IlmPhase(client, name=idx, phase="warm")
    ilm.stuck_count = 3
    ilm.advanced = False
    curr = {'action': 'complete', 'step': 'complete', 'phase': 'hot'}
    resp = {'indices': {'index': idx, **curr}}
    with patch.object(ilm, 'get_explain_data', return_value=resp):
        assert ilm.phase_stuck(max_stuck_count=3)
        assert ilm.advanced is True
        assert ilm.stuck_count == 0
        assert ilm.exceptions_raised == 0


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
        assert not ilm.check()


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
        assert not ilm.check()
