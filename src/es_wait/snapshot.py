"""Snapshot Completion Waiter."""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from ._base import Waiter
from .debug import debug, begin_end
from .defaults import SNAPSHOT
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Snapshot(Waiter):
    """Wait for a snapshot to complete.

    Polls the snapshot state to check if it has completed, logging the final
    status (e.g., SUCCESS, FAILED).

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 7200.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        snapshot (str): Snapshot name (default: '').
        repository (str): Repository name (default: '').

    Attributes:
        snapshot (str): Snapshot name.
        repository (str): Repository name.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `snapshot` or `repository` is empty.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo")
        >>> snap.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = SNAPSHOT.get('pause', 9.0),
        timeout: float = SNAPSHOT.get('timeout', 7200.0),
        max_exceptions: int = SNAPSHOT.get('max_exceptions', 10),
        snapshot: str = '',
        repository: str = '',
    ) -> None:
        """Initialize the Snapshot waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 7200.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            snapshot (str): Snapshot name (default: '').
            repository (str): Repository name (default: '').

        Raises:
            ValueError: If `snapshot` or `repository` is empty.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo")
            >>> snap.snapshot
            'my-snapshot'
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Snapshot object...')
        self.snapshot = snapshot
        self.repository = repository
        self._ensure_not_none('snapshot')
        self._ensure_not_none('repository')
        self.waitstr = f'for snapshot "{self.snapshot}" to complete'
        self.announce()
        debug.lv3('Snapshot object initialized')

    def __repr__(self) -> str:
        """Return a string representation of the Snapshot instance.

        Returns:
            str: String representation including snapshot, repository, waitstr,
                and pause.

        Example:
            >>> snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo",
            ...                 pause=5.0)
            >>> repr(snap)
            'Snapshot(snapshot="my-snapshot", repository="my-repo", waitstr="for
            snapshot \"my-snapshot\" to complete", pause=5.0)'
        """
        parts = [
            f"snapshot={self.snapshot!r}",
            f"repository={self.repository!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @property
    def snapstate(self) -> t.Dict:
        """Get the current state of the snapshot.

        Calls the snapshot.get API to retrieve the snapshot status.

        Returns:
            Dict: Snapshot state data.

        Raises:
            ValueError: If the snapshot or repository information cannot be obtained.

        Example:
            >>> snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo")
            >>> state = snap.snapstate  # Retrieves snapshot state
            >>> isinstance(state, dict)
            True
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

    @begin_end()
    def check(self) -> bool:
        """Check if the snapshot is complete.

        Returns True if the snapshot is not IN_PROGRESS, False otherwise. Logs
        the final state.

        Returns:
            bool: True if snapshot is complete, False otherwise.

        Raises:
            ValueError: If snapshot state cannot be retrieved.

        Example:
            >>> snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo")
            >>> snap.check()  # Returns True if snapshot complete
            False
        """
        self.too_many_exceptions()
        try:
            debug.lv4('TRY: Getting snapshot state')
            state = self.snapstate['snapshots'][0]['state']
            debug.lv5(f'snapshot state: {state}')
            retval = True
        except ValueError as err:
            self.exceptions_raised += 1
            self.add_exception(err)
            state = 'UNDEFINED'
            logger.error(err)
            retval = False
        if state == 'IN_PROGRESS':
            retval = False
        if retval:
            self.log_completion(state)
        debug.lv5(f'Return value = {retval}')
        return retval

    @begin_end()
    def log_completion(self, state: str) -> None:
        """Log the completion status of the snapshot.

        Logs the snapshot state with appropriate log levels: INFO for SUCCESS,
        ERROR for FAILED, WARNING for others.

        Args:
            state (str): Snapshot state (e.g., SUCCESS, FAILED, PARTIAL).

        Example:
            >>> snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo")
            >>> snap.log_completion("SUCCESS")
            ... # Logs: Snapshot my-snapshot completed with state: SUCCESS
        """
        msg = f'Snapshot {self.snapshot} completed with state: {state}'
        statemap = {'SUCCESS': logger.info, 'FAILED': logger.error}
        logfunc = statemap.get(state, logger.warning)
        logfunc(msg)
