"""Relocate Check"""
import typing as t
import logging
from elasticsearch8 import Elasticsearch
from .base import Waiter

# pylint: disable=missing-docstring,too-many-arguments

class Relocate(Waiter):
    ACTIONS: t.Optional[str] = None
    def __init__(
            self,
            client: Elasticsearch,
            action: t.Optional[str] = None,
            pause: float = 9,
            timeout: float = -1,
            name: str = None,
        ) -> None:
        super().__init__(client=client, action=action, pause=pause, timeout=timeout)
        self.logger = logging.getLogger('es_wait.Health')
        self.name = name
        self.empty_check('name')
        self.checkid = f'check for the {self.name} index relocation process to complete'

    @property
    def check(self) -> bool:
        """
        This function calls `client.cluster.` :py:meth:`~.elasticsearch.client.ClusterClient.state`
        with a given index to check if all of the shards for that index are in the ``STARTED``
        state. It will return ``True`` if all primary and replica shards are in the ``STARTED``
        state, and it will return ``False`` if any shard is in a different state.
        """
        finished = self.finished_state
        if finished:
            self.logger.debug('Relocate Check for index: "%s" has passed.', self.name)
        return finished

    @property
    def finished_state(self) -> bool:
        return (
            all(
                all(shard['state'] == "STARTED" for shard in shards)
                for shards in self.routing_table.values()
            )
        )

    @property
    def routing_table(self) -> t.Dict:
        msg = f'Unable to get routing table data from cluster state for {self.name}'
        try:
            result = self.client.cluster.state(index=self.name)
        except Exception as exc:
            self.logger.critical(msg)
            raise ValueError(msg) from exc
        try:
            return result['routing_table']['indices'][self.name]['shards']
        except KeyError as err:
            self.logger.critical(msg)
            raise KeyError(msg) from err
