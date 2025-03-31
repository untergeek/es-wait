"""Health Check Waiter"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
import tiered_debug as debug
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .defaults import HealthCheckDict, HealthTypes, HEALTH
from .utils import healthchk_result, prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

STATUS_CHK: HealthCheckDict = {'status': 'green'}
RELO_CHK: HealthCheckDict = {'relocating_shards': 0}
WAITSTR_MAP = {
    'status': 'for cluster health to show green status',
    'relocation': 'for cluster health to show zero relocating shards',
    'cluster_routing': (
        'for cluster health to show zero relocating shards across all indices'
    ),
}


class Health(Waiter):
    """Wait for health check result to be the expected state"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = HEALTH.get('pause', 1.5),
        timeout: float = HEALTH.get('timeout', 15.0),
        max_exceptions: int = HEALTH.get('max_exceptions', 10),
        check_type: HealthTypes = 'status',
        indices: t.Optional[t.Union[str, t.List[str]]] = None,
        check_for: t.Optional[HealthCheckDict] = None,
    ) -> None:
        """
        Initializes the Health waiter.

        :py:meth:`do_health_report` is set manually to ``True`` for this class
        so the parent class can log the health report results in the event
        something goes wrong.

        :param check_type: The type of health check to perform:
            One of:
            - 'status': Wait for the cluster status to be 'green'.
            - 'relocation': Wait for no relocating shards in specified indices.
            - 'cluster_routing': Wait for no relocating shards across all indices
            (ignores 'indices').
        :param indices: Index or list of indices to check (ignored for
            'cluster_routing')
        :param check_for: Custom conditions to check in the health response.
            Default values:
            - For 'check_type' = 'status', {'status': 'green'}
            - For 'check_type' = 'relocation' or 'cluster_routing',
            {'relocating_shards': 0}.
            Providing a value here overrides defaults.
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Health object...')
        #: The entity name
        self.check_type = check_type
        if self.check_type == 'cluster_routing' and indices is not None:
            logger.warning(
                "For 'cluster_routing', 'indices' is ignored. Checking all indices."
            )
        self.do_health_report = True
        self.indices = (
            ','.join(indices) if isinstance(indices, list) else indices or '*'
        )
        self.check_for = check_for or HEALTH['types'][check_type]
        self.waitstr = WAITSTR_MAP[check_type]
        self.announce()
        debug.lv3('Health object initialized')

    @property
    def filter_path(self) -> str:
        """
        Define the filter_path based on :py:attr:`check_for`

        The health check response has only root-level keys, so there is no need
        to worry about nested keys in either :py:attr:`check_for` or in the
        filter_path.

        :getter: Returns the proper filter_path
        :type: str
        """
        return ','.join(self.check_for.keys())

    def check(self) -> bool:
        """
        This function calls :py:meth:`cluster.health()
        <elasticsearch.client.ClusterClient.health>` and, based on the value
        returned by :py:func:`healthchk_result <es_wait.utils.healthchk_result>`,
        will return ``True`` or ``False``.

        For 'cluster_routing', 'indices' is ignored, and all indices ('*') are
        checked.

        :returns: True if the condition in 'check_for' is met, False otherwise.
        :rtype: bool
        """
        debug.lv2('Starting method...')
        self.too_many_exceptions()
        target = self.indices if self.check_type != 'cluster_routing' else '*'
        if self.check_type == 'cluster_routing':
            logger.info('Doing cluster_routing health check. Checking all indices.')
        try:
            debug.lv4('TRY: Getting health check response')
            response = self.client.cluster.health(
                index=target, filter_path=self.filter_path
            )
            debug.lv5(f'cluster.health response: {response}')
            result = healthchk_result(response, self.check_for)
            if result:
                retval = result
            else:
                retval = False
        except TransportError as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            logger.error(f'Error checking health: {prettystr(err)}')
            retval = False
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval
