"""Relocate Check"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Relocate')

# pylint: disable=missing-docstring,too-many-arguments


class Relocate(Waiter):

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,
        timeout: float = -1,
        name: str = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        self.name = name
        self.empty_check('name')
        self.waitstr = f'for index "{self.name}" to finish relocating'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def check(self) -> bool:
        """
        This function calls `client.cluster.`
        :py:meth:`~.elasticsearch.client.ClusterClient.state` with a given index to
        check if all of the shards for that index are in the ``STARTED`` state. It will
        return ``True`` if all primary and replica shards are in the ``STARTED`` state,
        and it will return ``False`` if any shard is in a different state.
        """
        finished = self.finished_state
        if finished:
            logger.debug('Relocate Check for index: "%s" has passed.', self.name)
        return finished

    @property
    def finished_state(self) -> bool:
        """
        Return the boolean state of whether all shards in the index are 'STARTED'

        The all() function returns True if all items in an iterable are true,
        otherwise it returns False. We use it twice here, nested.
        """
        return all(
            all(shard['state'] == "STARTED" for shard in shards)
            for shards in self.routing_table.values()
        )

    @property
    def routing_table(self) -> t.Dict:
        msg = f'Unable to get routing table data from cluster state for {self.name}'
        # Using filter_path drastically reduces the result size
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
        try:
            return result['routing_table']['indices'][self.name]['shards']
        except KeyError as err:
            logger.critical(msg)
            raise KeyError(msg) from err
