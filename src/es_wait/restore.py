"""Snapshot Restore Waiter."""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .debug import debug, begin_end
from .defaults import RESTORE
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Restore(Waiter):
    """Wait for a snapshot to restore.

    Polls the indices.recovery API to check if all shards for the specified
    indices are in the DONE stage.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 7200.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        index_list (Sequence[str], optional): List of indices (default: None).

    Attributes:
        index_list (Sequence[str]): List of indices being restored.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `index_list` is None.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> restore = Restore(client, index_list=["index1", "index2"])
        >>> restore.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = RESTORE.get('pause', 9.0),
        timeout: float = RESTORE.get('timeout', 7200.0),
        max_exceptions: int = RESTORE.get('max_exceptions', 10),
        index_list: t.Optional[t.Sequence[str]] = None,
    ) -> None:
        """Initialize the Restore waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 7200.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            index_list (Sequence[str], optional): List of indices (default: None).

        Raises:
            ValueError: If `index_list` is None.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> restore = Restore(client, index_list=["index1"])
            >>> restore.index_list
            ['index1']
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Restore object...')
        self.index_list = index_list
        self._ensure_not_none('index_list')
        self.waitstr = 'for indices in index_list to be restored from snapshot'
        self.announce()
        debug.lv3('Restore object initialized')

    def __repr__(self) -> str:
        """Return a string representation of the Restore instance.

        Returns:
            str: String representation including index_list, waitstr, and pause.

        Example:
            >>> restore = Restore(client, index_list=["index1"], pause=9.0)
            >>> repr(restore)
            'Restore(index_list=["index1"], waitstr="for indices in index_list
            to be restored from snapshot", pause=9.0)'
        """
        parts = [
            f"index_list={self.index_list!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @property
    @begin_end()
    def index_list_chunks(self) -> t.Sequence[t.Sequence[str]]:
        """Split index list into 3KB chunks.

        Chunks large index lists to avoid API limits, measured as CSV strings.

        Returns:
            Sequence[Sequence[str]]: List of index list chunks.

        Example:
            >>> restore = Restore(client, index_list=["index1", "index2"])
            >>> chunks = restore.index_list_chunks
            >>> isinstance(chunks, list) and all(isinstance(c, list) for c in chunks)
            True
        """
        chunks = []
        chunk = ""
        for index in self.index_list:
            if len(chunk) < 3072:
                if not chunk:
                    chunk = index
                else:
                    chunk += "," + index
            else:
                chunks.append(chunk.split(','))
                chunk = index
        chunks.append(chunk.split(','))
        debug.lv5(f'Return value = {chunks}')
        return chunks

    @begin_end()
    def check(self) -> bool:
        """Check if the snapshot restoration is complete.

        Verifies if all shards for all indices are in the DONE stage.

        Returns:
            bool: True if restoration is complete, False otherwise.

        Raises:
            ValueError: If recovery information cannot be obtained.

        Example:
            >>> restore = Restore(client, index_list=["index1"])
            >>> restore.check()  # Returns True if restoration complete
            False
        """
        response = {}
        for chunk in self.index_list_chunks:
            try:
                debug.lv4('Calling get_recovery')
                chunk_response = self.get_recovery(chunk)
                debug.lv5(f'get_recovery response: {chunk_response}')
            except ValueError as err:
                self.exceptions_raised += 1
                self.add_exception(err)
                logger.error(err)
                return False
            if not chunk_response:
                debug.lv1('_recovery API returned an empty response. Trying again.')
                self.exceptions_raised += 1
                debug.lv5('Return value = False')
                return False
            response.update(chunk_response)
        debug.lv3(f'Provided indices: {prettystr(self.index_list)}')
        debug.lv3(f'Found indices: {prettystr(list(response.keys()))}')
        for index, data in response.items():
            for shard in data['shards']:
                stage = shard['stage']
                if stage != 'DONE':
                    debug.lv1(f'Index {index} is still in stage {stage}')
                    return False
        debug.lv5('Return value = True')
        return True

    @begin_end()
    def get_recovery(self, chunk: t.Sequence[str]) -> t.Dict:
        """Get recovery information for a chunk of indices.

        Calls the indices.recovery API to check restoration progress.

        Args:
            chunk (Sequence[str]): List of index names.

        Returns:
            Dict: Recovery information for the chunk.

        Raises:
            ValueError: If recovery information cannot be obtained.

        Example:
            >>> restore = Restore(client, index_list=["index1"])
            >>> recovery = restore.get_recovery(["index1"])
            >>> isinstance(recovery, dict)
            True
        """
        chunk_response = {}
        try:
            debug.lv4('TRY: Getting index recovery information')
            chunk_response = dict(self.client.indices.recovery(index=chunk, human=True))
            debug.lv5(f'indices.recovery response: {chunk_response}')
        except TransportError as err:
            msg = (
                f'Restore.get_recovery: Unable to obtain recovery information for '
                f'specified indices {chunk}. Elasticsearch TransportError: '
                f'{prettystr(err)}'
            )
            logger.warning(msg)
            self.add_exception(err)
        except Exception as err:
            msg = (
                f'Restore.get_recovery: Unable to obtain recovery information for '
                f'specified indices {chunk}. Error: {prettystr(err)}'
            )
            logger.warning(msg)
            self.add_exception(err)
        debug.lv5(f'Return value = {chunk_response}')
        return chunk_response
