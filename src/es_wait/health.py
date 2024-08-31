"""Health Check Waiter"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Health(Waiter):
    """Wait for health check result to be the expected state"""

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
        #: The action determines the kind of response we look for in the health check
        self.action = action
        self.empty_check('action')
        self.waitstr = self.getwaitstr
        self.do_health_report = True
        logger.debug('Waiting %s...', self.waitstr)

    def argmap(self) -> t.Union[t.Dict[str, int], t.Dict[str, str]]:
        """
        This method ensures that we are processing the correct arguments based on
        :py:attr:`action`
        """
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
        This function calls :py:meth:`cluster.health()
        <elasticsearch.client.ClusterClient.health>` and, based on the
        return value from :py:meth:`argmap`, will return ``True`` or ``False``
        depending on whether that particular keyword appears in the output, and has the
        expected value.

        If multiple keys are provided, all must match for a ``True`` response.

        :getter: Returns if the check was complete
        :type: bool
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
        """
        Define the waitstr based on :py:attr:`action`

        :getter: Returns the proper waitstr
        :type: str
        """
        retval = None
        if self.action in self.RELO_ACTIONS:
            retval = 'for cluster health to show zero relocating shards'
        if self.action in self.STATUS_ACTIONS:
            retval = 'for cluster health to show green status'
        return retval
