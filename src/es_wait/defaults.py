"""Default configuration values for the es-wait package."""

import typing as t


class HealthCheckDict(t.TypedDict, total=False):
    """Type hint for health check conditions.

    Defines the structure of dictionaries used to specify expected health check
    conditions in the :py:class:`es_wait.health.Health` class.

    Attributes:
        cluster_name (str): Name of the cluster.
        status (str): Cluster status (e.g., 'green', 'yellow', 'red').
        timed_out (bool): If the health check timed out.
        number_of_nodes (int): Total number of nodes.
        number_of_data_nodes (int): Number of data nodes.
        active_primary_shards (int): Number of active primary shards.
        active_shards (int): Total number of active shards.
        relocating_shards (int): Number of relocating shards.
        initializing_shards (int): Number of initializing shards.
        unassigned_shards (int): Number of unassigned shards.
        unassigned_primary_shards (int): Number of unassigned primary shards.
        delayed_unassigned_shards (int): Number of delayed unassigned shards.
        number_of_pending_tasks (int): Number of pending tasks.
        number_of_in_flight_fetch (int): Number of in-flight fetch operations.
        task_max_waiting_in_queue_millis (int): Max task wait time in milliseconds.
        active_shards_percent_as_number (int): Percentage of active shards.

    Example:
        >>> check = HealthCheckDict(status="green")
        >>> check
        {'status': 'green'}
    """


status: HealthCheckDict = {'status': 'green'}
"""Default health check conditions for status checks.

Used in :py:class:`es_wait.health.Health` when check_type is 'status'.
Specifies that the cluster status should be 'green'.
"""

relocation: HealthCheckDict = {'relocating_shards': 0}
"""Default health check conditions for relocation checks.

Used in :py:class:`es_wait.health.Health` when check_type is 'relocation' or
'cluster_routing'. Specifies that there should be no relocating shards.
"""

CHECK_TYPES = {
    'status': status,
    'relocation': relocation,
    'cluster_routing': relocation,
}
"""Mapping of health check types to default conditions.

Keys are valid check_type values for :py:class:`es_wait.health.Health`.
Values are corresponding :py:class:`HealthCheckDict` instances.
"""

DEFAULT_PAUSE = 9.0
"""Default pause time between checks (in seconds).

Used as the default pause value for most waiter classes.
"""

DEFAULT_MAX_EXCEPTIONS = 10
"""Default maximum number of exceptions allowed.

Used as the default max_exceptions value for most waiter classes.
"""

BASE = {
    'pause': DEFAULT_PAUSE,
    'timeout': 15.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait._base.Waiter` class.

Contains pause, timeout, and max_exceptions values.
"""

ExistsTypes = t.Literal[
    'index',
    'data_stream',
    'index_template',
    'component_template',
]
"""Type literal for valid entity types in :py:class:`es_wait.exists.Exists`.

Defines acceptable values for the kind parameter.
"""

EXISTS_TYPES: t.List[ExistsTypes] = list(t.get_args(ExistsTypes))
"""List of valid entity types for :py:class:`es_wait.exists.Exists`.

Derived from ExistsTypes for runtime use.
"""

EXISTS = {
    'kind': ExistsTypes,
    'types': EXISTS_TYPES,
    'pause': 1.5,
    'timeout': 10.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait.exists.Exists` class.

Contains kind, types, pause, timeout, and max_exceptions values.
"""

HealthTypes = t.Literal['status', 'relocation', 'cluster_routing']
"""Type literal for valid health check types in :py:class:`es_wait.health.Health`.

Defines acceptable values for the check_type parameter.
"""

HEALTH_TYPES = {
    'status': status,
    'relocation': relocation,
    'cluster_routing': relocation,
}
"""Mapping of health check types to default conditions.

Identical to CHECK_TYPES, used in :py:class:`es_wait.health.Health`.
"""

HEALTH = {
    'types': HEALTH_TYPES,
    'pause': 1.5,
    'timeout': 15.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait.health.Health` class.

Contains types, pause, timeout, and max_exceptions values.
"""

ILM = {
    'pause': DEFAULT_PAUSE,
    'timeout': 630.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the ILM classes.

Used in :py:class:`es_wait.ilm.IndexLifecycle`, :py:class:`es_wait.ilm.IlmPhase`,
and :py:class:`es_wait.ilm.IlmStep`.
"""

RELOCATE = {
    'pause': DEFAULT_PAUSE,
    'timeout': 3600.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait.relocate.Relocate` class.

Contains pause, timeout, and max_exceptions values.
"""

RESTORE = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait.restore.Restore` class.

Contains pause, timeout, and max_exceptions values.
"""

SNAPSHOT = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait.snapshot.Snapshot` class.

Contains pause, timeout, and max_exceptions values.
"""

TASK = {
    'pause': DEFAULT_PAUSE,
    'timeout': 7200.0,
    'max_exceptions': DEFAULT_MAX_EXCEPTIONS,
}
"""Default configuration for the :py:class:`es_wait.task.Task` class.

Contains pause, timeout, and max_exceptions values.
"""
