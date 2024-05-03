"""Snapshot Restore Check"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Restore')

# pylint: disable=missing-docstring,too-many-arguments


class Restore(Waiter):
    """Restore Waiter class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,
        timeout: float = -1,
        index_list: t.Sequence[str] = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        self.index_list = index_list
        self.empty_check('index_list')
        self.waitstr = 'for indices in index_list to be restored from snapshot'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def check(self) -> bool:
        """
        Calls `client.indices.`
        :py:meth:`~.elasticsearch.client.IndicesClient.recovery` with a list of indices
        to check for complete recovery.  It will return ``True`` if recovery of those
        indices is complete, and ``False`` otherwise.  It is designed to fail fast: if
        a single shard is encountered that is still recovering (not in ``DONE`` stage),
        it will immediately return ``False``, rather than complete iterating over the
        rest of the response.
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

    @property
    def index_list_chunks(self) -> t.Sequence[t.Sequence[t.AnyStr]]:
        """
        This utility chunks very large index lists into 3KB chunks.
        It measures the size as a csv string, then converts back into a list for the
        return value.
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

    def get_recovery(self, chunk: t.Sequence[str]) -> t.Dict:
        """Get recovery information from Elasticsearch"""
        try:
            chunk_response = self.client.indices.recovery(index=chunk, human=True)
        except Exception as err:
            msg = (
                f'Unable to obtain recovery information for specified indices. Error: '
                f'{self.prettystr(err)}'
            )
            raise ValueError(msg) from err
        return chunk_response
