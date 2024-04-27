"""Health Check"""

import typing as t
import logging
from elasticsearch8 import Elasticsearch
from .base import Waiter

# pylint: disable=missing-docstring,too-many-arguments


class Health(Waiter):
    ACTIONS = ['allocation', 'cluster_routing', 'mount', 'replicas', 'shrink']
    RELO_ACTIONS = ['allocation', 'cluster_routing']
    STATUS_ACTIONS = ['mount', 'replicas', 'shrink']
    RELO_ARGS = {'relocating_shards': 0}
    STATUS_ARGS = {'status': 'green'}

    def __init__(
        self,
        client: Elasticsearch,
        action: t.Literal[
            'allocation', 'cluster_routing', 'mount', 'replicas', 'shrink'
        ] = None,
        pause: float = 1.5,
        timeout: float = 15,
    ) -> None:
        super().__init__(client=client, action=action, pause=pause, timeout=timeout)
        self.logger = logging.getLogger('es_wait.Health')
        self.empty_check('action')
        self.checkid = self.getcheckid

    @property
    def argmap(self) -> t.Union[t.Dict[str, int], t.Dict[str, str]]:
        """This is a little way to ensure many possibilities come down to one"""
        if self.action in self.RELO_ACTIONS:
            return self.RELO_ARGS
        if self.action in self.STATUS_ACTIONS:
            return self.STATUS_ARGS
        msg = f'{self.action} is not an acceptable value for action'
        self.logger.error(msg)
        raise ValueError(msg)

    @property
    def check(self) -> bool:
        """
        This function calls `client.cluster.`
        :py:meth:`~.elasticsearch.client.ClusterClient.health` and, based on the
        contents of self.argmap, will return ``True`` or ``False`` depending on whether
        that particular keyword appears in the output, and has the expected value.

        If multiple keys are provided, all must match for a ``True`` response.
        """
        output = self.client.cluster.health()
        check = True
        args = self.argmap
        for key, value in args.items():
            # First, verify that the key is in output
            if key not in output:
                raise KeyError(f'Key "{key}" not in cluster health output')
            # Verify that the output matches the expected value
            if output[key] != value:
                msg = (
                    f'NO MATCH: Value for key "{value}", health check output: '
                    f'{output[key]}'
                )
                self.logger.debug(msg)
                check = False  # We do not match
            else:
                msg = (
                    f'MATCH: Value for key "{value}", health check output: '
                    f'{output[key]}'
                )
                self.logger.debug(msg)
        if check:
            self.logger.debug('Health check for action %s passed.', self.action)
        return check

    @property
    def getcheckid(self) -> t.AnyStr:
        if self.action in self.RELO_ACTIONS:
            retval = 'check for cluster health to show zero relocating shards'
        if self.action in self.STATUS_ACTIONS:
            retval = 'check for cluster health to show green status'
        return retval
