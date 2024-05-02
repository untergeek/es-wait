"""Health Check"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Health')

# pylint: disable=missing-docstring,too-many-arguments


class Health(Waiter):
    ACTIONS = ['allocation', 'cluster_routing', 'mount', 'replicas', 'shrink']
    RELO_ACTIONS = ['allocation', 'cluster_routing']
    STATUS_ACTIONS = ['mount', 'replicas', 'shrink']
    RELO_ARGS = {'relocating_shards': 0}
    STATUS_ARGS = {'status': 'green'}

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1.5,
        timeout: float = 15,
        action: t.Literal[
            'allocation', 'cluster_routing', 'mount', 'replicas', 'shrink'
        ] = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        self.action = action
        self.empty_check('action')
        self.waitstr = self.getwaitstr

    def argmap(self) -> t.Union[t.Dict[str, int], t.Dict[str, str]]:
        """This is a little way to ensure many possibilities come down to one"""
        if self.action in self.RELO_ACTIONS:
            return self.RELO_ARGS
        if self.action in self.STATUS_ACTIONS:
            return self.STATUS_ARGS
        msg = f'{self.action} is not an acceptable value for action'
        logger.error(msg)
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
        args = self.argmap()
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
                logger.debug(msg)
                check = False  # We do not match
            else:
                msg = (
                    f'MATCH: Value for key "{value}", health check output: '
                    f'{output[key]}'
                )
                logger.debug(msg)
        if check:
            logger.debug('Health check for action %s passed.', self.action)
        return check

    @property
    def getwaitstr(self) -> t.AnyStr:
        retval = None
        if self.action in self.RELO_ACTIONS:
            retval = 'for cluster health to show zero relocating shards'
        if self.action in self.STATUS_ACTIONS:
            retval = 'for cluster health to show green status'
        return retval
