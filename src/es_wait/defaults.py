"""Default values used in multiple places in the package"""

import typing as t


class HealthCheckDict(t.TypedDict, total=False):
    """
    This is a type hint for the dictionary that is used to check the health of the
    cluster. It is used in the :py:class:`Health` class to determine if the health
    check is successful.
    """

    cluster_name: str
    status: str
    timed_out: bool
    number_of_nodes: int
    number_of_data_nodes: int
    active_primary_shards: int
    active_shards: int
    relocating_shards: int
    initializing_shards: int
    unassigned_shards: int
    unassigned_primary_shards: int
    delayed_unassigned_shards: int
    number_of_pending_tasks: int
    number_of_in_flight_fetch: int
    task_max_waiting_in_queue_millis: int
    active_shards_percent_as_number: int


status: HealthCheckDict = {'status': 'green'}
"""The default value for the HealthCheckDict when type is `status`"""

relocation: HealthCheckDict = {'relocating_shards': 0}
"""The default value for the HealthCheckDict when type is `relocation`"""

CHECK_TYPES = {
    'status': status,
    'relocation': relocation,
    'cluster_routing': relocation,
}
"""The types of health checks that can be performed"""

DEFAULT_PAUSE = 9.0
"""The default pause time between checks"""

DEFAULT_MAX_EXCEPTIONS = 10
"""The default maximum number of exceptions to allow"""

BASE = {
    'pause': DEFAULT_PAUSE,
    'timeout': 15.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the base Waiter class"""

ExistsTypes = t.Literal[
    'index',
    'data_stream',
    'index_template',
    'component_template',
]
"""The acceptable values for types of entities that can be checked for existence"""

EXISTS_TYPES: t.List[ExistsTypes] = list(t.get_args(ExistsTypes))
"""A list of the types of entities that can be checked for existence"""

EXISTS = {
    'kind': ExistsTypes,
    'types': EXISTS_TYPES,
    'pause': 1.5,
    'timeout': 15.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the Exists class"""

HealthTypes = t.Literal['status', 'relocation', 'cluster_routing']
"""The acceptable values for types of health checks"""

HEALTH_TYPES = {
    'status': status,
    'relocation': relocation,
    'cluster_routing': relocation,
}
"""The types of health checks that can be performed"""

HEALTH = {
    'types': HEALTH_TYPES,
    'pause': 1.5,
    'timeout': 15.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the Health class"""

ILM = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the ILM class"""

RELOCATE = {
    'pause': DEFAULT_PAUSE,
    'timeout': 3600.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the Relocate class"""

RESTORE = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the Restore class"""

SNAPSHOT = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the Snapshot class"""

TASK = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""The default values for the Task class"""
