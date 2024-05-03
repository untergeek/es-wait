"""Index Relocation Waiter"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Relocate(Waiter):
    """Wait for an index to relocate"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,
        timeout: float = -1,
        name: str = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        #: The index name
        self.name = name
        self.empty_check('name')
        self.waitstr = f'for index "{self.name}" to finish relocating'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def check(self) -> bool:
        """
        This method gets the value from property :py:meth:`finished_state` and returns
        that value.

        :getter: Returns if the check was complete
        :type: bool
        """
        finished = self.finished_state
        if finished:
            logger.debug('Relocate Check for index: "%s" has passed.', self.name)
        return finished

    @property
    def finished_state(self) -> bool:
        """
        Return the boolean state of whether all shards in the index are 'STARTED'

        The :py:func:`all` function returns True if all items in an iterable are true,
        otherwise it returns False. We use it twice here, nested.

        Gets this from property :py:meth:`routing_table`

        :getter: Returns whether the shards are all ``STARTED``
        :type: bool
        """
        return all(
            all(shard['state'] == "STARTED" for shard in shards)
            for shards in self.routing_table.values()
        )

    @property
    def routing_table(self) -> t.Dict:
        """
        This method calls :py:meth:`cluster.state()
        <elasticsearch.client.ClusterClient.state>` to get the shard routing table. As
        the cluster state API result can be quite large, it uses a ``filter_path`` to
        drastically reduce the result size. This path is:

          .. code-block:: python

             f'routing_table.indices.{self.name}'

        It will raise a :py:exc:`ValueError` on an exception to this API call.

        It will then try to return

          .. code-block:: python

             return result['routing_table']['indices'][self.name]['shards']

        And will raise a :py:exc:`KeyError` if one of those keys is not found.

        :getter: Returns the shard routing table
        :type: bool
        """
        msg = f'Unable to get routing table data from cluster state for {self.name}'
        fpath = f'routing_table.indices.{self.name}'
        try:
            result = self.client.cluster.state(index=self.name, filter_path=fpath)
            # {
            #     "routing_table": {
            #         "indices": {
            #         "SELF.NAME": {
            #             "shards": {
            #             "0": [
            #                   {
            #                    "state": "SHARD_STATE",
        except Exception as exc:
            logger.critical(msg)
            raise ValueError(msg) from exc

        # Actually return the result
        try:
            return result['routing_table']['indices'][self.name]['shards']
        except KeyError as err:
            logger.critical(msg)
            raise KeyError(msg) from err
