"""Snapshot Completion Waiter"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Snapshot(Waiter):
    """Wait for a snapshot to complete"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,
        timeout: float = -1,
        snapshot: str = None,
        repository: str = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        #: The snapshot name
        self.snapshot = snapshot
        #: The repository name
        self.repository = repository
        self.empty_check('snapshot')
        self.empty_check('repository')
        self.waitstr = f'for snapshot "{self.snapshot}" to complete'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def check(self) -> bool:
        """
        Get the state of the snapshot from :py:meth:`snapstate` to determine if the
        snapshot is complete, and if so, with what status.

        If the state is ``IN_PROGRESS``, this method will return ``False``.

        For all other states, it calls :py:meth:`log_completion` to log the final
        result. It then returns ``True``.

        :getter: Returns if the check was complete
        :type: bool
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
        """
        This function calls
        :py:meth:`snapshot.get() <elasticsearch.client.SnapshotClient.get>` to get the
        current state of the snapshot.

        :getter: Returns the state of the snapshot
        :type: bool
        """
        result = {}
        try:
            result = self.client.snapshot.get(
                repository=self.repository, snapshot=self.snapshot
            )
        except Exception as err:
            raise ValueError(
                f'Unable to obtain information for snapshot "{self.snapshot}" in '
                f'repository "{self.repository}". Error: {self.prettystr(err)}'
            ) from err
        return result

    def log_completion(self, state: str) -> None:
        """
        Log completion based on ``state``

        If the snapshot state is:

        .. list-table:: Snapshot States & Logs
           :widths: 15 15 70
           :header-rows: 1

           * - State
             - Log Level
             - Message
           * - ``SUCCESS``
             - ``INFO``
             - Snapshot [`name`] successfully completed.
           * - ``PARTIAL``
             - ``WARNING``
             - Snapshot [`name`] completed with state PARTIAL.
           * - ``FAILED``
             - ``ERROR``
             - Snapshot [`name`] completed with state FAILED.
           * - [`other`]
             - ``WARNING``
             - Snapshot [`name`] completed with state: [`other`]

        :param state: The snapshot state
        """
        if state == 'SUCCESS':
            logger.info('Snapshot %s successfully completed.', self.snapshot)
        elif state == 'PARTIAL':
            logger.warning('Snapshot %s completed with state PARTIAL.', self.snapshot)
        elif state == 'FAILED':
            logger.error('Snapshot %s completed with state FAILED.', self.snapshot)
        else:
            logger.warning('Snapshot %s completed with state: %s', self.snapshot, state)
