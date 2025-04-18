"""Utilities for waiting on Elasticsearch operations.

The es-wait package provides classes to wait for completion of Elasticsearch
tasks, such as index relocation, snapshot creation, and ILM phase transitions.
Each waiter polls the cluster state, handles timeouts, and logs progress.

Example:
    >>> from es_wait import Health
    >>> from elasticsearch8 import Elasticsearch
    >>> client = Elasticsearch()
    >>> waiter = Health(client, check_type="status")
    >>> waiter.wait()
"""

__version__ = "0.15.0"
from .exists import Exists
from .health import Health
from .ilm import IlmPhase, IlmStep
from .relocate import Relocate
from .restore import Restore
from .snapshot import Snapshot
from .task import Task

__all__ = [
    'Exists',
    'Health',
    'IlmPhase',
    'IlmStep',
    'Relocate',
    'Restore',
    'Snapshot',
    'Task',
]
