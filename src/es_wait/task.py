"""Task Check"""
import typing as t
import logging
from time import localtime, strftime
from elasticsearch8 import Elasticsearch
from .base import Waiter
from .args import TaskArgs

# pylint: disable=missing-docstring,too-many-arguments

class Task(Waiter):
    ACTIONS = ['forcemerge', 'reindex', 'update_by_query']
    def __init__(
            self,
            client: Elasticsearch,
            action: t.Literal['forcemerge', 'reindex', 'update_by_query'] = None,
            pause: float = 9,
            timeout: float = -1,
            task_id: str = None,
        ) -> None:
        super().__init__(client=client, action=action, pause=pause, timeout=timeout)
        self.logger = logging.getLogger('es_wait.Health')
        self.task_id = task_id
        self.empty_check(task_id)
        self.task_data = None
        self.task = None
        self.checkid = f'check for the {self.action} task to complete'

    @property
    def check(self) -> bool:
        """
        This function calls `client.tasks.` :py:meth:`~.elasticsearch.client.TasksClient.get` with
        the provided ``task_id``.  If the task data contains ``'completed': True``, then it will
        return ``True``. If the task is not completed, it will log some information about the task
        and return ``False``
        """
        try:
            self.task_data = TaskArgs(settings=self.client.tasks.get(task_id=self.task_id))
        except Exception as err:
            msg = f'Unable to obtain task information for task_id "{self.task_id}". Exception {err}'
            raise ValueError(msg) from err
        self.task = TaskArgs(settings=self.task_data.task)
        self.reindex_check()
        return self.task_complete

    def reindex_check(self) -> None:
        if self.task.action == 'indices:data/write/reindex':
            self.logger.debug("It's a REINDEX task")
            self.logger.debug('TASK_DATA: %s', self.task_data.asdict)
            self.logger.debug('TASK_DATA keys: %s', list(self.task_data.asdict.keys()))
            if self.task_data.response:
                if 'failures' in self.task_data.response:
                    msg = (
                        f'Failures found in reindex response: {self.task_data.response["failures"]}'
                    )
                    raise ValueError(msg)

    @property
    def task_complete(self) -> bool:
        running_time = 0.000000001 * self.task.running_time_in_nanos
        self.logger.debug('Running time: %s seconds', running_time)
        if self.task_data.completed:
            completion_time = (running_time * 1000) + self.task['start_time_in_millis']
            time_string = strftime('%Y-%m-%dT%H:%M:%S', localtime(completion_time/1000))
            self.logger.debug('Task "%s" completed at %s.', self.task.description, time_string)
            retval = True
        else:
            # Log the task status here.
            self.logger.debug('Full Task Data: %s', self.task_data.asdict)
            msg = (
                f'Task "{self.task.description}" with task_id "{self.task_id}" has been running '
                f'for {running_time} seconds'
            )
            self.logger.debug(msg)
            retval = False
        return retval
