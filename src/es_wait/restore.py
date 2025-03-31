"""Snapshot Restore Waiter"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
import tiered_debug as debug
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .defaults import RESTORE
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Restore(Waiter):
    """Wait for a snapshot to restore"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = RESTORE.get('pause', 9.0),
        timeout: float = RESTORE.get('timeout', 7200.0),
        max_exceptions: int = RESTORE.get('max_exceptions', 10),
        index_list: t.Optional[t.Sequence[str]] = None,
    ) -> None:
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Restore object...')
        #: The list of indices being restored
        self.index_list = index_list
        self._ensure_not_none('index_list')
        self.waitstr = 'for indices in index_list to be restored from snapshot'
        self.announce()
        debug.lv3('Restore object initialized')

    @property
    def index_list_chunks(self) -> t.Sequence[t.Sequence[str]]:
        """
        This utility chunks very large index lists into 3KB chunks.
        It measures the size as a csv string, then converts back into a list for the
        return value.

        Pulls this data from :py:attr:`index_list`

        :getter: Returns a list of smaller chunks of :py:attr:`index_list` in lists
        :type: bool
        """
        debug.lv2('Starting method...')
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
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {chunks}')
        return chunks

    def check(self) -> bool:
        """
        Iterates over a list of indices in batched chunks, and calls
        :py:meth:`get_recovery` for each batch, updating a local ``response`` dict with
        each successive result.

        For each entry in ``response`` it will evaluate the shards from each index for
        which stage they are.

        The method will return ``True`` if all shards for all indices in
        :py:attr:`index_list` are at stage ``DONE``, and ``False`` otherwise.

        This check is designed to fail fast: if a single shard is encountered that is
        still recovering (not in ``DONE`` stage), it will immediately return ``False``,
        rather than complete iterating over the rest of the response.

        :getter: Returns if the check was complete
        :type: bool
        """
        debug.lv2('Starting method...')
        response = {}
        for chunk in self.index_list_chunks:
            try:
                debug.lv4('Calling get_recovery')
                chunk_response = self.get_recovery(chunk)
                debug.lv5(f'get_recovery response: {chunk_response}')
            except ValueError as err:
                self.exceptions_raised += 1
                self.add_exception(err)  # Append the error to self._exceptions
                logger.error(err)
                return False
            if not chunk_response:
                debug.lv1('_recovery API returned an empty response. Trying again.')
                self.exceptions_raised += 1  # Repeated empties as exceptions
                debug.lv3('Exiting method, returning value')
                debug.lv5('Value = False')
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

        # If we've gotten here, all of the indices have recovered
        debug.lv3('Exiting method, returning value')
        debug.lv5('Value = True')
        return True

    def get_recovery(self, chunk: t.Sequence[str]) -> t.Dict:
        """
        Calls :py:meth:`indices.recovery()
        <elasticsearch.client.IndicesClient.recovery>` with a list of indices to check
        for complete recovery.

        Returns the response, or raises a :py:exc:`ValueError` if it is unable to get a
        response.

        :param chunk: A list of index names
        :type chunk: list

        :raises ValueError: If the indices are not recoverable

        :returns: The response from the recovery API for the provided chunk
        :rtype: dict
        """
        debug.lv2('Starting method...')
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
            self.add_exception(err)  # Append the error to self._exceptions
        except Exception as err:
            msg = (
                f'Restore.get_recovery: Unable to obtain recovery information for '
                f'specified indices {chunk}. Error: {prettystr(err)}'
            )
            logger.warning(msg)
            self.add_exception(err)  # Append the error to self._exceptions
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {chunk_response}')
        return chunk_response
