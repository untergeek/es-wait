"""Unit test level conftest.py"""

# pylint: disable=missing-function-docstring,redefined-outer-name,R0913

from unittest.mock import MagicMock, patch
import datetime
from random import randrange
from dotmap import DotMap
import pytest
from elastic_transport import ApiResponseMeta
from elasticsearch8.exceptions import NotFoundError
from es_wait import IlmPhase, IlmStep, Relocate, Restore

CLUSTER_HEALTH = {
    "cluster_name": "unit_test",
    "status": "green",
    "timed_out": False,
    "number_of_nodes": 7,
    "number_of_data_nodes": 3,
    "active_primary_shards": 235,
    "active_shards": 471,
    "relocating_shards": 0,
    "initializing_shards": 0,
    "unassigned_shards": 0,
    "delayed_unassigned_shards": 0,
    "number_of_pending_tasks": 0,
    "task_max_waiting_in_queue_millis": 0,
    "active_shards_percent_as_number": 100,
}
FAKE_FAIL = Exception('Simulated Failure')
FAKE_TIME = datetime.datetime(2020, 12, 25, 17, 5, 55)
GENERIC_TASK = {'task': 'I0ekFjMhSPCQz7FUs1zJOg:54510686'}
INDEX_HEALTH = [{"health": "green"}]
INDEX_NAME = 'index_name'
INDEX_RESOLVE = {'indices': [{'name': INDEX_NAME}], 'aliases': [], 'data_streams': []}
NAMED_INDICES = ["index-2015.01.01", "index-2015.02.01"]
SNAP_NAME = 'snap_name'
REPO_NAME = 'fake_repo'
RESTORE = {'shards': [{'stage': 'VALUE'}]}
PROTO_TASK = {
    'node': 'I0ekFjMhSPCQz7FUs1zJOg',
    'description': 'UNIT TEST',
    'running_time_in_nanos': 1637039537721,
    'action': 'indices:data/write/reindex',
    'id': 54510686,
    'start_time_in_millis': 1489695981997,
}


@pytest.fixture(scope='function')
def chunky_list():
    def _chunky_list(stage):
        chunkme = []
        retval = {}
        for i in range(1, 300):
            name = f'longish-indexname-00000{i}'
            chunkme.append(name)
            retval[name] = {'shards': [{'stage': stage}]}
        return chunkme, retval

    return _chunky_list


@pytest.fixture(scope='function')
def client():
    return MagicMock()


@pytest.fixture(scope='function')
def cluster_health():
    return CLUSTER_HEALTH


@pytest.fixture(scope='function')
def cluster_state(named_index, shardinator):
    def _cluster_state(state, count):
        shards = shardinator(state, count)
        return {'routing_table': {'indices': {named_index: shards}}}

    return _cluster_state


@pytest.fixture(scope='function')
def existschk(client):
    def _existschk(kind, retval):
        if kind in ['index', 'data_stream']:
            client.indices.exists.return_value = retval
        elif kind in ['template', 'index_template']:
            client.indices.exists_index_template.return_value = retval
        elif kind in ['component', 'component_template']:
            client.cluster.exists_component_template.return_value = retval

    return _existschk


@pytest.fixture(scope='function')
def fake_fail():
    return FAKE_FAIL


@pytest.fixture(scope='function')
def meta404():
    return ApiResponseMeta(404, '1.1', {}, 0.01, None)


@pytest.fixture(scope='function')
def fake_notfound():
    # 5 positional args for meta: status, http_version, headers, duration, node
    meta = ApiResponseMeta(404, '1.1', {}, 0.01, None)
    body = 'simulated error'
    msg = 'simulated error'
    # 3 positional args for NotFoundError: message, meta, body
    yield NotFoundError(msg, meta, body)


@pytest.fixture(scope='function')
def generic_task():
    return GENERIC_TASK['task']


@pytest.fixture(scope='function')
def healthchk(client, cluster_health):
    def _healthchk(retval=cluster_health):
        client.cluster.health.return_value = retval

    return _healthchk


@pytest.fixture
def expected(request):
    return request.param


@pytest.fixture(scope='function')
def phase(request):
    return request.param


@pytest.fixture(scope='function')
def actual(request):
    return request.param


@pytest.fixture(scope='function')
def target(request):
    return request.param


@pytest.fixture(scope="function")
def action(request):
    if request.param == "complete":
        return "complete"
    return "in_progress"


@pytest.fixture(scope="function")
def step(request):
    if request.param == "complete":
        return "complete"
    return "in_progress"


@pytest.fixture(scope='function')
def explainretval(client, fullexplain):
    def _explainretval(phase=None, action='completed', step='completed'):
        retval = fullexplain(phase=phase, action=action, step=step)
        client.ilm.explain_lifecycle.return_value = retval

    return _explainretval


@pytest.fixture(scope='function')
def fullexplain(indexexplain, named_index):
    def _fullexplain(phase=None, action='completed', step='completed'):
        retval = indexexplain(phase=phase, action=action, step=step)
        return {'indices': {named_index: retval}}

    return _fullexplain


@pytest.fixture(scope='function')
def indexexplain():
    def _indexexplain(phase=None, action='completed', step='completed'):
        return {
            'phase': phase,
            'action': action,
            'step': step,
        }

    return _indexexplain


@pytest.fixture(scope='function')
def ilm_phase(client, named_index, explainretval, phase):
    """Fixture to create an IlmPhase instance with mocked client."""
    explainretval(action='complete', phase=phase, step='complete')
    ilm_phase = IlmPhase(client, name=named_index, phase=phase)
    return ilm_phase


@pytest.fixture
def ilm_completed(fullexplain, phase):
    return fullexplain(phase=phase, action='complete', step='complete')


@pytest.fixture(scope='function')
def ilm_test(client, named_index):
    def _ilm_test(phase=None, result: bool = False):
        ic = IlmStep(client, name=named_index)
        if phase is not None:
            ic = IlmPhase(client, name=named_index, phase=phase)

        return bool(ic.check() is result)

    return _ilm_test


@pytest.fixture(scope='function')
def index_health():
    return INDEX_HEALTH


@pytest.fixture(scope='function')
def idx():
    return INDEX_NAME


@pytest.fixture(scope='function')
def idx_resolve():
    return INDEX_RESOLVE


@pytest.fixture(scope='function')
def indexhc(client, index_health, idx_resolve):
    def _indexhc(retval=index_health, resolve=idx_resolve):
        client.cat.indices.return_value = retval
        client.indices.resolve_index.return_value = resolve

    return _indexhc


@pytest.fixture(scope='function')
def named_index():
    return NAMED_INDICES[0]


@pytest.fixture(scope='function')
def named_indices():
    return NAMED_INDICES


@pytest.fixture(scope='function')
def proto_task():
    return PROTO_TASK


@pytest.fixture(scope='function')
def relocate_test(client, named_index, relocatechk):
    def _relocate_test(state: str = None, count: int = 1, result: bool = False):
        relocatechk(state, count)
        rc = Relocate(client, name=named_index)
        return bool(rc.check() is result)

    return _relocate_test


@pytest.fixture(scope='function')
def relocatechk(client, cluster_state):
    def _relocatechk(state, count):
        client.cluster.state.return_value = cluster_state(state, count)

    return _relocatechk


@pytest.fixture(scope='function')
def repo():
    return REPO_NAME


@pytest.fixture(scope='function')
def restorechk(client, restorevals):
    def _restorechk(retval=restorevals('DONE')):
        client.indices.recovery.return_value = retval

    return _restorechk


@pytest.fixture(scope='function')
def restore_state():
    def _restore_state(state):
        retval = DotMap(RESTORE)
        retval.shards[0].stage = state
        return retval

    return _restore_state


@pytest.fixture(scope='function')
def restore_test(client, chunky_list, named_indices, restorechk, restorevals):
    def _restore_test(val, boolval, chunktest=False):
        if chunktest:
            biglist, retval = chunky_list(val)
            restorechk(retval)
            rc = Restore(client, index_list=biglist)
        else:
            restorechk(restorevals(val))
            rc = Restore(client, index_list=named_indices)
        return bool(rc.check() is boolval)

    return _restore_test


@pytest.fixture(scope='function')
def restorevals(restore_state):
    def _restorevals(state):
        if state == {}:
            return state
        retval = DotMap()
        state = restore_state(state)
        for idx in NAMED_INDICES:
            retval[idx] = state
        return retval

    return _restorevals


@pytest.fixture(scope='function')
def shardinator():
    """Generate shard states to mimic cluster.state output"""

    def _shardinator(state, count):
        states = ['INITIALIZING', 'RELOCATING', 'STARTED', 'UNASSIGNED']
        retval = {}
        for i in range(0, count):
            prirep = []
            for _ in ['primary', 'replica']:
                value = states[randrange(3)] if state.lower() == 'random' else state
                prirep.append({'state': value})
            retval[str(i)] = prirep
        return {'shards': retval}

    return _shardinator


@pytest.fixture(scope='function')
def snap():
    return SNAP_NAME


@pytest.fixture(scope='function')
def snap_resp(snap, named_indices):
    def _snap_resp(state=None, snapshot=snap, indices=named_indices):
        retval = {
            'snapshots': [
                {
                    'state': state,
                    'snapshot': snapshot,
                    'indices': indices,
                }
            ]
        }
        return retval

    return _snap_resp


@pytest.fixture(scope='function')
def snapbundle(client, snap, repo):
    return client, {'snapshot': snap, 'repository': repo}


@pytest.fixture(scope='function')
def snapchk(client):
    def _snapchk(retval):
        client.snapshot.get.return_value = retval

    return _snapchk


@pytest.fixture(scope='function')
def taskchk(client):
    def _taskchk(retval):
        client.tasks.get.return_value = retval

    return _taskchk


@pytest.fixture(scope='function')
def taskmaster(proto_task):
    def _taskmaster(completed=False, task=proto_task, failures=None):
        failures = [] if failures is None else failures
        test_task = {
            'completed': completed,
            'task': task,
            'response': {'failures': failures},
        }
        return test_task

    return _taskmaster


@pytest.fixture(scope='function')
def base_client():
    """
    Create a mocked Elasticsearch client.
    """
    client = MagicMock()
    return client


@pytest.fixture(scope='function')
def health_client():
    """
    Create a mocked Elasticsearch client for 'Health' with default the default
    response set to {'status': 'green'}.
    """
    client = MagicMock()
    client.cluster.health.return_value = {'status': 'green'}
    return client


@pytest.fixture(scope='function')
def exists_client():
    """
    Create a mocked Elasticsearch client for 'Exists' with default responses
    set to True.
    """
    client = MagicMock()
    client.indices.exists.return_value = True
    client.indices.exists_index_template.return_value = True
    client.cluster.exists_component_template.return_value = True
    return client


# Mock datetime for consistent timing
@pytest.fixture
def mock_datetime():
    with patch('es_wait._base.datetime') as mock_dt:
        yield mock_dt
