"""Unit tests for Restore"""

import unittest
from unittest.mock import MagicMock
from es_wait.restore import Restore


class TestRestore(unittest.TestCase):
    """Test Restore class"""

    def setUp(self):
        self.client = MagicMock()
        self.restore = Restore(client=self.client, index_list=['index1', 'index2'])

    def test_index_list_chunks(self):
        """Test that index_list_chunks returns a list of lists"""
        self.restore.index_list = ['index' + str(i) for i in range(100)]
        chunks = self.restore.index_list_chunks
        self.assertTrue(all(len(chunk) <= 3072 for chunk in chunks))

    def test_check_all_done(self):
        """Test that check returns True when all indices are done"""
        self.restore.get_recovery = MagicMock(
            return_value={
                'index1': {'shards': [{'stage': 'DONE'}]},
                'index2': {'shards': [{'stage': 'DONE'}]},
            }
        )
        self.assertTrue(self.restore.check())

    def test_check_not_done(self):
        """Test that check returns False when not all indices are done"""
        self.restore.get_recovery = MagicMock(
            return_value={
                'index1': {'shards': [{'stage': 'DONE'}]},
                'index2': {'shards': [{'stage': 'INIT'}]},
            }
        )
        self.assertFalse(self.restore.check())

    def test_get_recovery_success(self):
        """Test that get_recovery returns a dict of indices"""
        self.client.indices.recovery.return_value = {
            'index1': {'shards': [{'stage': 'DONE'}]}
        }
        response = self.restore.get_recovery(['index1'])
        self.assertIn('index1', response)

    def test_get_recovery_failure(self):
        """Test that get_recovery raises an exception"""
        self.client.indices.recovery.side_effect = Exception('Test error')
        with self.assertRaises(ValueError):
            self.restore.get_recovery(['index1'])


class TestRestoreMore:
    """Test Restore class"""

    def test_fail_to_get_recovery(self, client, fake_fail, named_indices):
        """
        Should raise ``ValueError`` when an upstream Exception is encountered
        """
        client.indices.recovery.side_effect = fake_fail
        rc = Restore(client, index_list=named_indices)
        assert not rc.check()

    def test_incomplete_recovery(self, restore_test):
        """Should return ``False`` when recovery is incomplete"""
        assert restore_test('INDEX', False)

    def test_completed_recovery(self, restore_test):
        """Should return ``True`` when recovery is complete"""
        assert restore_test('DONE', True)

    def test_empty_recovery(self, restore_test):
        """Should return ``False`` when an empty response comes back"""
        assert restore_test({}, False)

    def test_chunker(self, restore_test):
        """Ensure that very long lists of indices are properly chunked"""
        assert restore_test('DONE', True, chunktest=True)


# @pytest.fixture
# def client():
#     return MagicMock()


# def test_restore_init_defaults(client):
#     """Test Restore initialization with default parameters"""
#     restore = Restore(client=client)
#     assert restore.client == client
#     assert restore.pause == 9.0
#     assert restore.timeout == 7200.0
#     assert restore.max_exceptions == 10
#     assert restore.index_list is None
#     assert restore.waitstr == 'for indices in index_list to be restored from snapshot'


# def test_restore_init_custom(client):
#     """Test Restore initialization with custom parameters"""
#     index_list = ['index1', 'index2']
#     restore = Restore(
#         client=client,
#         pause=5.0,
#         timeout=3600.0,
#         max_exceptions=5,
#         index_list=index_list,
#     )
#     assert restore.client == client
#     assert restore.pause == 5.0
#     assert restore.timeout == 3600.0
#     assert restore.max_exceptions == 5
#     assert restore.index_list == index_list
#     assert restore.waitstr == 'for indices in index_list to be restored from snapshot'


# def test_restore_init_ensure_not_none(client):
#     """Test Restore initialization ensures index_list is not None"""
#     with pytest.raises(ValueError):
#         Restore(client=client, index_list=None)
