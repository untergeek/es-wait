"""Snapshot Check"""

import typing as t
import logging
from elasticsearch8 import Elasticsearch
from .base import Waiter

# pylint: disable=missing-docstring,too-many-arguments


class Snapshot(Waiter):
    ACTIONS: t.Optional[str] = None

    def __init__(
        self,
        client: Elasticsearch,
        action: t.Optional[str] = None,
        pause: float = 9,
        timeout: float = -1,
        snapshot: str = None,
        repository: str = None,
    ) -> None:
        super().__init__(client=client, action=action, pause=pause, timeout=timeout)
        self.logger = logging.getLogger('es_wait.Snapshot')
        self.snapshot = snapshot
        self.repository = repository
        self.empty_check('snapshot')
        self.empty_check('repository')
        self.checkid = f'check for snapshot {self.snapshot} completion'

    @property
    def check(self) -> bool:
        """
        This function calls `client.snapshot.`
        :py:meth:`~.elasticsearch.client.SnapshotClient.get` and tests to see whether
        the snapshot is complete, and if so, with what status. It will log errors
        according to the result. If the snapshot is still ``IN_PROGRESS``, it will
        return ``False``. ``SUCCESS`` will be an ``INFO`` level message, ``PARTIAL``
        nets a ``WARNING`` message, ``FAILED`` is an ``ERROR``, message, and all others
        will be a ``WARNING`` level message.
        """
        state = self.snapstate['snapshots'][0]['state']
        retval = True
        if state == 'IN_PROGRESS':
            retval = False
        if retval:
            self.log_completion(state)
        return retval

    @property
    def snapstate(self) -> t.Dict:
        result = {}
        try:
            result = self.client.snapshot.get(
                repository=self.repository, snapshot=self.snapshot
            )
        except Exception as err:
            raise ValueError(
                f'Unable to obtain information for snapshot "{self.snapshot}" in '
                f'repository "{self.repository}". Error: {err}'
            ) from err
        return result

    def log_completion(self, state: str) -> None:
        if state == 'SUCCESS':
            self.logger.info('Snapshot %s successfully completed.', self.snapshot)
        elif state == 'PARTIAL':
            self.logger.warning(
                'Snapshot %s completed with state PARTIAL.', self.snapshot
            )
        elif state == 'FAILED':
            self.logger.error('Snapshot %s completed with state FAILED.', self.snapshot)
        else:
            self.logger.warning(
                'Snapshot %s completed with state: %s', self.snapshot, state
            )
