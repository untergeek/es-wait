"""Task Completion Waiter"""

import typing as t
import logging
import warnings
from time import localtime, strftime
from dotmap import DotMap  # type: ignore
from elasticsearch8.exceptions import GeneralAvailabilityWarning
from ._base import Waiter
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Task(Waiter):
    """Wait for a task to complete"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9.0,
        timeout: float = -1.0,
        action: t.Literal[
            'forcemerge', 'reindex', 'update_by_query', 'undef'
        ] = 'undef',
        task_id: str = '',
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        #: The action to wait for
        self.action = action
        if action == 'undef':
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
        logger.debug('Waiting %s...', self.waitstr)

    @property
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
        # The properties for task_data
        # TASK_DATA
        # self.task_data.response = {}
        # self.task_data.completed = False
        # self.task_data.task = {} -> Becomes TASK
        # TASK
        # self.task.action = str
        # self.task.description = str
        # self.task.running_time_in_nanos = 0

        response = {}
        try:
            # The Tasks API is not yet GA. We need to suppress the warning for now.
            # This is required after elasticsearch8>=8.16.0 as the warning is raised
            # from that release onward.
            warnings.filterwarnings("ignore", category=GeneralAvailabilityWarning)
            response = dict(self.client.tasks.get(task_id=self.task_id))
        except Exception as err:
            msg = (
                f'Unable to obtain task information for task_id "{self.task_id}". '
                f'Response: {prettystr(response)} -- '
                f'Exception: {prettystr(err)}'
            )
            logger.error(msg)
            raise ValueError(msg) from err
        self.task_data = DotMap(response)
        self.task = self.task_data.task  # type: ignore
        self.reindex_check()
        return self.task_complete

    def reindex_check(self) -> None:
        """
        Check to see if the task is a reindex operation. The task may be "complete" but
        had one or more failures. Raise a :py:exc:`ValueError` exception if errors were
        encountered.

        Gets data from :py:attr:`task` and :py:attr:`task_data`.
        """
        if self.task.action == 'indices:data/write/reindex':  # type: ignore
            # logger.debug("It's a REINDEX task")
            # logger.debug('TASK_DATA: %s', prettystr(self.task_data.toDict()))
            # logger.debug(
            #     'TASK_DATA keys: %s',
            #     prettystr(list(self.task_data.toDict().keys())),
            # )
            if self.task_data.response.failures:  # type: ignore
                if len(self.task_data.response.failures) > 0:  # type: ignore
                    _ = self.task_data.response.failures  # type: ignore
                    msg = (
                        f'Failures found in the {self.action} response: '
                        f'{prettystr(_)}'
                    )
                    raise ValueError(msg)

    @property
    def task_complete(self) -> bool:
        """
        Process :py:attr:`task` and :py:attr:`task_data` to see if the task has
        completed, or is still running.

        If :py:attr:`task_data` contains ``'completed': True``, then it will
        return ``True``. If the task is not completed, it will log some information
        about the task and return ``False``
        """
        running_time = 0.000000001 * self.task.running_time_in_nanos  # type: ignore
        logger.debug('Running time: %s seconds', running_time)
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
            logger.debug(msg)
            retval = True
        else:
            # Log the task status here.
            _ = self.task_data.toDict()  # type: ignore
            logger.debug('Full Task Data: %s', prettystr(_))
            msg = (
                f'Task "{self.task.description}" with task_id '  # type: ignore
                f'"{self.task_id}" has been running for {running_time} seconds'
            )
            logger.debug(msg)
            retval = False
        return retval
