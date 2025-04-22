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

__version__ = "0.15.1"
from datetime import datetime
from .exists import Exists
from .health import Health
from .ilm import IlmPhase, IlmStep
from .relocate import Relocate
from .restore import Restore
from .snapshot import Snapshot
from .task import Task

FIRST_YEAR = 2024
now = datetime.now()
if now.year == FIRST_YEAR:
    COPYRIGHT_YEARS = "2025"
else:
    COPYRIGHT_YEARS = f"2025-{now.year}"


__author__ = "Aaron Mildenstein"
__copyright__ = f"{COPYRIGHT_YEARS}, {__author__}"
__license__ = "Apache 2.0"
__status__ = "Development"
__description__ = "Utilities for waiting on Elasticsearch operations"
__url__ = "https://github.com/untergeek/es-wait"
__email__ = "aaron@mildensteins.com"
__maintainer__ = "Aaron Mildenstein"
__maintainer_email__ = f"{__email__}"
__keywords__ = ["elasticsearch", "tools", "wait"]
__classifiers__ = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
]

__all__ = [
    'Exists',
    'Health',
    'IlmPhase',
    'IlmStep',
    'Relocate',
    'Restore',
    'Snapshot',
    'Task',
    "__author__",
    "__copyright__",
    "__version__",
]
