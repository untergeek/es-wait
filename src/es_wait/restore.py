"""Snapshot Restore Waiter"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Restore(Waiter):
    """Wait for a snapshot to restore"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,
        timeout: float = -1,
        index_list: t.Sequence[str] = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        #: The list of indices being restored
        self.index_list = index_list
        self.empty_check('index_list')
        self.waitstr = 'for indices in index_list to be restored from snapshot'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def index_list_chunks(self) -> t.Sequence[t.Sequence[t.AnyStr]]:
        """
        This utility chunks very large index lists into 3KB chunks.
        It measures the size as a csv string, then converts back into a list for the
        return value.

        Pulls this data from :py:attr:`index_list`

        :getter: Returns a list of smaller chunks of :py:attr:`index_list` in lists
        :type: bool
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
        return chunks

    @property
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
        response = {}
        for chunk in self.index_list_chunks:
            chunk_response = self.get_recovery(chunk)
            if chunk_response == {}:
                logger.debug('_recovery API returned an empty response. Trying again.')
                return False
            response.update(chunk_response)
        logger.debug('Provided indices: %s', self.prettystr(self.index_list))
        logger.debug('Found indices: %s', self.prettystr(list(response.keys())))
        for index, data in response.items():
            for shard in data['shards']:
                stage = shard['stage']
                if stage != 'DONE':
                    logger.debug('Index %s is still in stage %s', index, stage)
                    return False

        # If we've gotten here, all of the indices have recovered
        return True

    def get_recovery(self, chunk: t.Sequence[str]) -> t.Dict:
        """
        Calls :py:meth:`indices.recovery()
        <elasticsearch.client.IndicesClient.recovery>` with a list of indices to check
        for complete recovery.

        Returns the response, or raises a :py:exc:`ValueError` if it is unable to get a
        response.

        :param chunk: A list of index names
        """
        try:
            chunk_response = self.client.indices.recovery(index=chunk, human=True)
        except Exception as err:
            msg = (
                f'Unable to obtain recovery information for specified indices. Error: '
                f'{self.prettystr(err)}'
            )
            raise ValueError(msg) from err
        return chunk_response
