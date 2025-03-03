"""Health Check Waiter"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Index(Waiter):
    """Wait for index health check to have an expected value"""

    ACTIONS = ['allocation', 'cluster_routing', 'mount', 'replicas', 'shrink']
    HEALTH_ACTIONS = ['health', 'mount', 'replicas', 'shrink']
    HEALTH_ARGS = {'status': 'green'}

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1.5,
        timeout: float = 15.0,
        action: t.Literal['health', 'mount', 'replicas', 'shrink', 'undef'] = 'undef',
        index: str = '',
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        #: The action determines the kind of response we look for in the health check
        self.action = action
        if action == 'undef':
            msg = 'action must be one of health, mount, replicas, or shrink'
            logger.error(msg)
            raise ValueError(msg)
        self.index = index
        self._ensure_not_none('index')
        self.resolve_index()
        self.waitstr = self.getwaitstr
        self.do_health_report = True
        logger.debug('Waiting %s...', self.waitstr)

    def argmap(self) -> t.Union[t.Dict[str, int], t.Dict[str, str]]:
        """
        This method ensures that we are processing the correct arguments based on
        :py:attr:`action`
        """
        if self.action in self.HEALTH_ACTIONS:
            return self.HEALTH_ARGS
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
        output = dict(
            self.client.cluster.health(index=self.index, filter_path='status')
        )
        logger.debug('output = %s', output)
        check = True
        args = self.argmap()
        for key, value in args.items():
            # First, verify that the key is in output
            if key not in output:
                raise KeyError(f'Key "{key}" not in index health output')
            # Verify that the output matches the expected value
            if output[key] != value:
                msg = (
                    f'NO MATCH: Value for key "{value}", index health check output: '
                    f'{output[key]}'
                )
                logger.debug(msg)
                check = False  # We do not match
            else:
                msg = (
                    f'MATCH: Value for key "{value}", index health check output: '
                    f'{output[key]}'
                )
                logger.debug(msg)
        if check:
            logger.debug('Index health check for action %s passed.', self.action)
        return check

    def resolve_index(self) -> None:
        """
        Resolve whether the value of :py:attr:`index` is an index of the same
        name, or something else.
        """
        resp = self.client.indices.resolve_index(name=self.index)
        if len(resp['indices']) == 0:  # This is bad, it should be one
            raise ValueError(f'{self.index} resolves to zero indices: {resp}')
        if len(resp['indices']) > 1:  # This is bad, it should only be one
            raise ValueError(f'{self.index} resolves to more than one index: {resp}')
        if resp['indices'][0]['name'] != self.index:  # An alias?
            raise ValueError(f'{self.index} does not resolve to itself: {resp}')

    @property
    def getwaitstr(self) -> str:
        """
        Define the waitstr based on :py:attr:`action`

        :getter: Returns the proper waitstr
        :type: str
        """
        retval = ''
        if self.action in self.HEALTH_ACTIONS:
            retval = 'for index health to show green status'
        return retval
