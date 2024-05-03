"""Unit tests for Task"""

import pytest
from es_wait import Task


class TestTask:
    """Test Task class"""

    def test_bad_task_id(self, client, fake_fail):
        """
        Should raise ``ValueError`` if a bad value for ``task_id`` is passed
        """
        client.tasks.get.side_effect = fake_fail
        tc = Task(client, action='foo', task_id='bar')
        with pytest.raises(ValueError, match=r'Unable to obtain task information'):
            # pylint: disable=W0104
            tc.check

    def test_incomplete_task(self, client, generic_task, taskchk, taskmaster):
        """Should return ``False`` if task is incomplete"""
        taskchk(taskmaster())
        tc = Task(client, action='reindex', task_id=generic_task)
        assert not tc.check

    def test_complete_task(self, client, generic_task, taskchk, taskmaster):
        """Should return ``True`` if task is complete"""
        taskchk(taskmaster(completed=True))
        tc = Task(client, action='update_by_query', task_id=generic_task)
        assert tc.check

    def test_has_failures(self, client, generic_task, taskchk, taskmaster):
        """Should raise ValueError if errors are encountered"""
        action = 'forcemerge'
        taskchk(taskmaster(completed=True, failures=['fail1', 'fail2']))
        tc = Task(client, action=action, task_id=generic_task)
        matchstr = f'Failures found in the {action} response'
        with pytest.raises(ValueError, match=matchstr):
            # pylint: disable=W0104
            tc.check

    def test_wait_success(self, client, generic_task, taskchk, taskmaster):
        """Should raise a TimeoutError if task does not complete on time"""
        taskchk(taskmaster(completed=True))
        tc = Task(
            client,
            action='update_by_query',
            task_id=generic_task,
            pause=0.05,
            timeout=1,
        )
        assert tc.wait() is None

    def test_wait_timeout(self, client, generic_task, taskchk, taskmaster):
        """Should raise a TimeoutError if task does not complete on time"""
        taskchk(taskmaster())
        tc = Task(
            client, action='reindex', task_id=generic_task, pause=0.5, timeout=5.5
        )
        with pytest.raises(TimeoutError):
            tc.wait()
