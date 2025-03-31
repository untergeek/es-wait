"""Task Completion Waiter"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
import warnings
from time import localtime, strftime
from dotmap import DotMap  # type: ignore
import tiered_debug as debug
from elasticsearch8.exceptions import GeneralAvailabilityWarning
from ._base import Waiter
from .defaults import TASK
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Task(Waiter):
    """Wait for a task to complete"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = TASK.get('pause', 9.0),
        timeout: float = TASK.get('timeout', 7200.0),
        max_exceptions: int = TASK.get('max_exceptions', 10),
        action: t.Literal['forcemerge', 'reindex', 'update_by_query'] = 'reindex',
        task_id: str = '',
    ) -> None:
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Task object...')
        #: The action to wait for
        self.action = action
        if action not in ['forcemerge', 'reindex', 'update_by_query']:
            msg = 'The action must be one of forcemerge, reindex, or update_by_query'
            logger.error(msg)
            raise ValueError(msg)
        #: The task identification string
        self.task_id = task_id
        self._ensure_not_none('task_id')
        #: The :py:meth:`tasks.get() <elasticsearch.client.TasksClient.get>` results
        self.task_data = None
        #: The contents of :py:attr:`task_data['task'] <task_data>`
        self.task = None
        self.failure_count = 0
        self.waitstr = f'for the "{self.action}" task to complete'
        self.announce()
        debug.lv3('Task object initialized')

    @property
    def task_complete(self) -> bool:
        """
        Process :py:attr:`task` and :py:attr:`task_data` to see if the task has
        completed, or is still running.

        If :py:attr:`task_data` contains ``'completed': True``, then it will
        return ``True``. If the task is not completed, it will log some information
        about the task and return ``False``
        """
        debug.lv2('Starting method...')
        running_time = 0.000000001 * self.task.running_time_in_nanos  # type: ignore
        debug.lv3(f'Running time: {running_time} seconds')
        if self.task_data.completed:  # type: ignore
            completion_time = running_time * 1000
            completion_time += self.task['start_time_in_millis']  # type: ignore
            time_string = strftime(
                '%Y-%m-%dT%H:%M:%S', localtime(completion_time / 1000)
            )
            msg = (
                f'Task "{self.task.description}" with task_id '  # type: ignore
                f'"{self.task_id}" completed at {time_string}'
            )
            debug.lv3(msg)
            retval = True
        else:
            # Log the task status here.
            _ = self.task_data.toDict()  # type: ignore
            debug.lv5(f'Full Task Data: {prettystr(_)}')
            msg = (
                f'Task "{self.task.description}" with task_id '  # type: ignore
                f'"{self.task_id}" has been running for {running_time} seconds'
            )
            debug.lv3(msg)
            retval = False
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def check(self) -> bool:
        """
        This function calls :py:meth:`tasks.get()
        <elasticsearch.client.TasksClient.get>` with the provided ``task_id`` and sets
        the values for :py:attr:`task_data` and :py:attr:`task` as part of its
        execution pipeline.

        It then calls :py:meth:`reindex_check` to see if it is a reindex operation.

        Finally, it returns whatever :py:meth:`task_complete` returns.

        :getter: Returns if the check was complete
        :type: bool
        """
        debug.lv2('Starting method...')
        # The properties for task_data
        # TASK_DATA
        # self.task_data.response = {}
        # self.task_data.completed = False
        # self.task_data.task = {} -> Becomes TASK
        # TASK
        # self.task.action = str
        # self.task.description = str
        # self.task.running_time_in_nanos = 0
        self.too_many_exceptions()
        response = {}
        try:
            # The Tasks API is not yet GA. We need to suppress the warning for now.
            # This is required after elasticsearch8>=8.16.0 as the warning is raised
            # from that release onward.
            debug.lv4('TRY: Getting task information')
            warnings.filterwarnings("ignore", category=GeneralAvailabilityWarning)
            response = dict(self.client.tasks.get(task_id=self.task_id))
            debug.lv5(f'tasks.get response: {response}')
        except Exception as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            msg = (
                f'Unable to obtain task information for task_id "{self.task_id}". '
                f'Response: {prettystr(response)} -- '
                f'Exception: {prettystr(err)}'
            )
            logger.error(msg)
            return False
        self.task_data = DotMap(response)
        self.task = self.task_data.task  # type: ignore
        try:
            debug.lv4('TRY: Checking for reindex task')
            self.reindex_check()
        except ValueError as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            logger.error(f'Error in reindex_check: {prettystr(err)}')
            return False
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {self.task_complete}')
        return self.task_complete

    def reindex_check(self) -> None:
        """
        Check to see if the task is a reindex operation. The task may be "complete" but
        had one or more failures. Raise a :py:exc:`ValueError` exception if errors were
        encountered.

        Gets data from :py:attr:`task` and :py:attr:`task_data`.
        """
        debug.lv2('Starting method...')
        if self.task.action == 'indices:data/write/reindex':  # type: ignore
            debug.lv5("It's a REINDEX task")
            debug.lv5(f'TASK_DATA: {prettystr(self.task_data.toDict())}')
            debug.lv5(
                f'TASK_DATA keys: ' f'{prettystr(list(self.task_data.toDict().keys()))}'
            )
            if self.task_data.response.failures:  # type: ignore
                if len(self.task_data.response.failures) > 0:  # type: ignore
                    _ = self.task_data.response.failures  # type: ignore
                    msg = (
                        f'Failures found in the {self.action} response: '
                        f'{prettystr(_)}'
                    )
                    logger.error(msg)
                    debug.lv3('Exiting method, raising ValueError')
                    raise ValueError(msg)
            debug.lv3('Exiting method')
