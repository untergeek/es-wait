"""Task Completion Waiter"""

import typing as t
import logging
from time import localtime, strftime
from dotmap import DotMap
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Task(Waiter):
    """Wait for a task to complete"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,
        timeout: float = -1,
        action: t.Literal['forcemerge', 'reindex', 'update_by_query'] = None,
        task_id: str = None,
    ) -> None:
        super().__init__(client=client, pause=pause, timeout=timeout)
        self.action = action
        #: The task identification string
        self.task_id = task_id
        self.empty_check('action')
        self.empty_check('task_id')
        #: The :py:meth:`tasks.get() <elasticsearch.client.TasksClient.get>` results
        self.task_data = None
        #: The contents of :py:attr:`task_data['task'] <task_data>`
        self.task = None
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
            response = dict(self.client.tasks.get(task_id=self.task_id))
        except Exception as err:
            msg = (
                f'Unable to obtain task information for task_id "{self.task_id}". '
                f'Response: {self.prettystr(response)} -- '
                f'Exception: {self.prettystr(err)}'
            )
            raise ValueError(msg) from err
        self.task_data = DotMap(response)
        self.task = self.task_data.task
        self.reindex_check()
        return self.task_complete

    def reindex_check(self) -> None:
        """
        Check to see if the task is a reindex operation. The task may be "complete" but
        had one or more failures. Raise a :py:exc:`ValueError` exception if errors were
        encountered.

        Gets data from :py:attr:`task` and :py:attr:`task_data`.
        """
        if self.task.action == 'indices:data/write/reindex':
            # logger.debug("It's a REINDEX task")
            # logger.debug('TASK_DATA: %s', self.prettystr(self.task_data.toDict()))
            # logger.debug(
            #     'TASK_DATA keys: %s',
            #     self.prettystr(list(self.task_data.toDict().keys())),
            # )
            if self.task_data.response.failures:
                if len(self.task_data.response.failures) > 0:
                    msg = (
                        f'Failures found in the {self.action} response: '
                        f'{self.prettystr(self.task_data.response["failures"])}'
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
        running_time = 0.000000001 * self.task.running_time_in_nanos
        logger.debug('Running time: %s seconds', running_time)
        if self.task_data.completed:
            completion_time = (running_time * 1000) + self.task['start_time_in_millis']
            time_string = strftime(
                '%Y-%m-%dT%H:%M:%S', localtime(completion_time / 1000)
            )
            logger.debug(
                'Task "%s" completed at %s.', self.task.description, time_string
            )
            retval = True
        else:
            # Log the task status here.
            logger.debug('Full Task Data: %s', self.prettystr(self.task_data.toDict()))
            msg = (
                f'Task "{self.task.description}" with task_id "{self.task_id}" has '
                f'been running for {running_time} seconds'
            )
            logger.debug(msg)
            retval = False
        return retval
