"""Snapshot Completion Waiter"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
import tiered_debug as debug
from ._base import Waiter
from .defaults import SNAPSHOT
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Snapshot(Waiter):
    """Wait for a snapshot to complete"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = SNAPSHOT.get('pause', 9.0),
        timeout: float = SNAPSHOT.get('timeout', 7200.0),
        max_exceptions: int = SNAPSHOT.get('max_exceptions', 10),
        snapshot: str = '',
        repository: str = '',
    ) -> None:
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Starting method...')
        #: The snapshot name
        self.snapshot = snapshot
        #: The repository name
        self.repository = repository
        self._ensure_not_none('snapshot')
        self._ensure_not_none('repository')
        self.waitstr = f'for snapshot "{self.snapshot}" to complete'
        self.announce()
        debug.lv3('Snapshot object initialized')

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
            debug.lv4('TRY: snapshot.get()')
            result = dict(
                self.client.snapshot.get(
                    repository=self.repository, snapshot=self.snapshot
                )
            )
            debug.lv5(f'snapshot.get result: {result}')
        except Exception as err:
            raise ValueError(
                f'Unable to obtain information for snapshot "{self.snapshot}" in '
                f'repository "{self.repository}". Error: {prettystr(err)}'
            ) from err
        return result

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
        debug.lv2('Starting method...')
        self.too_many_exceptions()
        try:
            debug.lv4('TRY: Getting snapshot state')
            state = self.snapstate['snapshots'][0]['state']
            debug.lv5(f'snapshot state: {state}')
            retval = True
        except ValueError as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            state = 'UNDEFINED'
            logger.error(err)
            retval = False
        if state == 'IN_PROGRESS':
            retval = False
        if retval:
            self.log_completion(state)
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

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
             - Snapshot [`name`] completed with state SUCCESS.
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
        debug.lv2('Starting method...')
        msg = f'Snapshot {self.snapshot} completed with state: {state}'
        statemap = {'SUCCESS': logger.info, 'FAILED': logger.error}
        logfunc = statemap.get(state, logger.warning)
        logfunc(msg)
        debug.lv3('Exiting method')
