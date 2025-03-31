"""Index Relocation Waiter"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
import tiered_debug as debug
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .defaults import RELOCATE

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Relocate(Waiter):
    """Wait for an index to relocate"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = RELOCATE.get('pause', 9.0),
        timeout: float = RELOCATE.get('timeout', 3600.0),
        max_exceptions: int = RELOCATE.get('max_exceptions', 10),
        name: t.Optional[str] = None,
    ) -> None:
        """
        The :py:class:`Relocate` class is a subclass of :py:class:`Waiter` and is
        used to wait for an index to finish relocating.

        :note: See defaults.py for default values.

        :param client: The Elasticsearch client
        :param pause: The pause time between checks (default is
            defaults.RELOCATE_PAUSE seconds)
        :param timeout: The time to wait before giving up (default is
            defaults.RELOCATE_TIMEOUT seconds)
        :param max_exceptions: The maximum number of exceptions to allow (default
            is defaults.MAX_EXCEPTIONS)
        :param name: The index name

        :type client: Elasticsearch client
        :type pause: float
        :type timeout: float
        :type max_exceptions: int
        :type name: str

        :raises ValueError: If the index name is not provided
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Relocate object...')
        #: The index name
        self.name = name
        self._ensure_not_none('name')
        self.waitstr = f'for index "{self.name}" to finish relocating'
        self.announce()
        debug.lv3('Relocate object initialized')

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
        debug.lv2('Starting method...')
        _ = self.routing_table()
        if not _:
            logger.warning(f'No routing table data for index "{self.name}"')
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = False')
            return False
        try:
            debug.lv4('TRY: Getting shard state data')
            retval = all(
                all(shard['state'] == "STARTED" for shard in shards)
                for shards in _.values()
            )
            debug.lv3('Exiting method, returning value')
            debug.lv5(f'Value = {retval}')
            return retval
        except KeyError as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            logger.error(f'KeyError in finished_state for index "{self.name}"')
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = False')
            return False

    def routing_table(self) -> t.Dict[str, t.List[t.Dict[str, str]]]:
        """
        This method calls :py:meth:`cluster.state()
        <elasticsearch.client.ClusterClient.state>` to get the shard routing
        table. As the cluster state API result can be quite large, it uses a
        ``filter_path`` to drastically reduce the result size. This path is:

          .. code-block:: python

             f'routing_table.indices.{self.name}'

        It will raise a :py:exc:`ValueError` on an exception to this API call.

        It will then try to return

          .. code-block:: python

             return result['routing_table']['indices'][self.name]['shards']

        :getter: Returns the shard routing table
        :type: t.Dict[str, t.List[t.Dict[str, str]]]
        """
        debug.lv2('Starting method...')
        msg = f'Unable to get routing table data from cluster state for {self.name}'
        fpath = f'routing_table.indices.{self.name}'
        try:
            debug.lv4('TRY: Getting cluster state response')
            result = self.client.cluster.state(index=self.name, filter_path=fpath)
            # {
            #     "routing_table": {
            #         "indices": {
            #         "SELF.NAME": {
            #             "shards": {
            #             "0": [
            #                   {
            #                    "state": "SHARD_STATE",
            debug.lv5(f'cluster.state response: {result}')
        except TransportError as exc:
            self.exceptions_raised += 1
            self.add_exception(exc)  # Append the error to self._exceptions
            logger.critical(f'{msg} because of {exc}')
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = {}')
            return {}

        # Actually return the result
        try:
            debug.lv4('TRY: Getting shard routing table data')
            retval = result['routing_table']['indices'][self.name]['shards']
            debug.lv3('Exiting method, returning value')
            debug.lv5(f'Value = {retval}')
            return retval
        except KeyError as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            logger.error(f'{msg} because of {err}')
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = {}')
            return {}

    def check(self) -> bool:
        """
        This method gets the value from property :py:meth:`finished_state` and returns
        that value.

        :returns: Returns if the check was complete
        :rtype: bool
        """
        debug.lv2('Starting method...')
        self.too_many_exceptions()
        try:
            debug.lv4('TRY: Getting finished_state value')
            finished = self.finished_state
        except (TransportError, KeyError) as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            logger.error(f'Error checking for index "{self.name}": {err}')
            finished = False
        if finished:
            debug.lv1(f'Relocate Check for index: "{self.name}" has passed.')
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {finished}')
        return finished
