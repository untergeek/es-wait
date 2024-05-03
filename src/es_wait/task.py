"""Task Check"""

import typing as t
import logging
from time import localtime, strftime
from dotmap import DotMap
from ._base import Waiter

# from .args import TaskArgs

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Task')

# pylint: disable=missing-docstring,too-many-arguments


class Task(Waiter):
    ACTIONS = ['forcemerge', 'reindex', 'update_by_query']

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
        self.task_id = task_id
        self.empty_check('action')
        self.empty_check('task_id')
        self.task_data = None
        self.task = None
        self.waitstr = f'for the "{self.action}" task to complete'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def check(self) -> bool:
        """
        This function calls `client.tasks.`
        :py:meth:`~.elasticsearch.client.TasksClient.get` with the provided
        ``task_id``.  If the task data contains ``'completed': True``, then it will
        return ``True``. If the task is not completed, it will log some information
        about the task and return ``False``
        """
        # The properties for TaskArgs
        # TASK_DATA
        # self.task_data.response = {}
        # self.task_data.completed = False
        # self.task_data.task = {} -> Becomes TASK
        # TASK
        # self.task.action = str
        # self.task.description = str
        # self.task.running_time_in_nanos = 0

        try:
            # self.task_data = TaskArgs(
            #     settings=self.client.tasks.get(task_id=self.task_id)
            # )
            self.task_data = DotMap(self.client.tasks.get(task_id=self.task_id))
        except Exception as err:
            msg = (
                f'Unable to obtain task information for task_id "{self.task_id}". '
                f'Exception {self.prettystr(err)}'
            )
            raise ValueError(msg) from err
        self.task = self.task_data.task
        self.reindex_check()
        return self.task_complete

    def reindex_check(self) -> None:
        if self.task.action == 'indices:data/write/reindex':
            logger.debug("It's a REINDEX task")
            logger.debug('TASK_DATA: %s', self.prettystr(self.task_data.toDict()))
            logger.debug(
                'TASK_DATA keys: %s',
                self.prettystr(list(self.task_data.toDict().keys())),
            )
            if self.task_data.response.failures:
                if len(self.task_data.response.failures) > 0:
                    msg = (
                        f'Failures found in the {self.action} response: '
                        f'{self.prettystr(self.task_data.response["failures"])}'
                    )
                    raise ValueError(msg)

    @property
    def task_complete(self) -> bool:
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
