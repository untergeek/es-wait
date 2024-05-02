"""Unit tests for Waiter"""

import pytest
from es_wait._base import Waiter


# class TestWaitForIt(TestCase):
#     """TestWaitForIt

#     Test helpers.waiters.wait_for_it functionality
#     """

#     # pylint: disable=line-too-long
#     def test_bad_action(self):
#         """test_bad_action

#         Should raise a ``ConfigurationError`` exception if ``action`` is invalid
#         """
#         client = Mock()
#         # self.assertRaises(ConfigurationError, wait_for_it, client, 'foo')
#         with pytest.raises(ConfigurationError, match=r'"action" must be one of'):
#             wait_for_it(client, 'foo')

#     def test_reindex_action_no_task_id(self):
#         """test_reindex_action_no_task_id

#         Should raise a ``MissingArgument`` exception if ``task_id`` is missing for
#         ``reindex``
#         """
#         client = Mock()
#         # self.assertRaises(MissingArgument, wait_for_it, client, 'reindex')
#         with pytest.raises(MissingArgument, match=r'A task_id must accompany "action"'):
#             wait_for_it(client, 'reindex')

#     def test_snapshot_action_no_snapshot(self):
#         """test_snapshot_action_no_snapshot

#         Should raise a ``MissingArgument`` exception if ``snapshot`` is missing for
#         ``snapshot``
#         """
#         client = Mock()
#         # self.assertRaises(MissingArgument, wait_for_it, client,
#         #   'snapshot', repository='foo')
#         with pytest.raises(
#             MissingArgument, match=r'A snapshot and repository must accompany "action"'
#         ):
#             wait_for_it(client, 'snapshot', repository='foo')

#     def test_snapshot_action_no_repository(self):
#         """test_snapshot_action_no_repository

#         Should raise a ``MissingArgument`` exception if ``repository`` is missing for
#         ``snapshot``
#         """
#         client = Mock()
#         # self.assertRaises(MissingArgument, wait_for_it, client,
#         #   'snapshot', snapshot='foo')
#         with pytest.raises(
#             MissingArgument, match=r'A snapshot and repository must accompany "action"'
#         ):
#             wait_for_it(client, 'snapshot', snapshot='foo')

#     def test_restore_action_no_indexlist(self):
#         """test_restore_action_no_indexlist

#         Should raise a ``MissingArgument`` exception if ``index_list`` is missing for
#         ``restore``
#         """
#         client = Mock()
#         # self.assertRaises(MissingArgument, wait_for_it, client, 'restore')
#         with pytest.raises(
#             MissingArgument, match=r'An index_list must accompany "action"'
#         ):
#             wait_for_it(client, 'restore')

#     def test_reindex_action_bad_task_id(self):
#         """test_reindex_action_bad_task_id

#         Should raise a ``CuratorException`` exception if there's a bad task_id

#         This is kind of a fake fail, even in the code.
#         """
#         client = Mock()
#         client.tasks.get.return_value = {'a': 'b'}
#         client.tasks.get.side_effect = FAKE_FAIL
#         # self.assertRaises(CuratorException, wait_for_it,
#         #       client, 'reindex', task_id='foo')
#         with pytest.raises(CuratorException, match=r'Unable to find task_id'):
#             wait_for_it(client, 'reindex', task_id='foo')

#     def test_reached_max_wait(self):
#         """test_reached_max_wait

#         Should raise a ``ActionTimeout`` exception if we've waited past the defined
#         timeout period
#         """
#         client = Mock()
#         client.cluster.health.return_value = {'status': 'red'}
#         # self.assertRaises(ActionTimeout, wait_for_it, client, 'replicas',
#         # wait_interval=1, max_wait=1)
#         with pytest.raises(
#             ActionTimeout, match=r'failed to complete in the max_wait period'
#         ):
#             wait_for_it(client, 'replicas', wait_interval=1, max_wait=1)
