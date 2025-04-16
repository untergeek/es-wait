"""Unit tests for Restore"""

import logging
import unittest
from unittest.mock import MagicMock
from elasticsearch8 import TransportError
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
        assert not self.restore.check()

    def test_check_value_error(self):
        """Test that check returns False and logs error when ValueError is raised"""
        self.restore.get_recovery = MagicMock(side_effect=ValueError('Test error'))
        with self.assertLogs('es_wait.restore', level='ERROR') as log:
            self.assertFalse(self.restore.check())
            self.assertIn('ERROR:es_wait.restore:Test error', log.output)

    def test_check_complete_recovery(self):
        """Test that check returns True when all indices are recovered"""
        self.restore.get_recovery = MagicMock(
            return_value={
                'index1': {'shards': [{'stage': 'DONE'}]},
                'index2': {'shards': [{'stage': 'DONE'}]},
            }
        )
        self.assertTrue(self.restore.check())


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

    def test_get_recovery_transport_error(self, client, caplog):
        """
        Test that get_recovery logs a warning and adds exception on TransportError
        """
        caplog.set_level(logging.WARNING)
        client.indices.recovery.side_effect = TransportError('Test TransportError')
        rc = Restore(client, index_list=['index1'])
        assert not rc.check()
        assert 'Restore.get_recovery: Unable to obtain recovery' in caplog.text
        assert 'information for specified indices' in caplog.text
        assert 'index1' in caplog.text
        assert "TransportError('Test TransportError')" in caplog.text
        assert rc.exceptions_raised == 1

    def test_get_recovery_general_exception(self, client, caplog):
        """
        Test that get_recovery logs a warning and adds exception on general Exception
        """
        caplog.set_level(logging.WARNING)
        client.indices.recovery.side_effect = Exception('Test Exception')
        rc = Restore(client, index_list=['index1'])
        assert not rc.check()
        assert 'Restore.get_recovery: Unable to obtain recovery' in caplog.text
        assert 'information for specified indices' in caplog.text
        assert 'index1' in caplog.text
        assert "Exception('Test Exception')" in caplog.text
        assert rc.exceptions_raised == 1


def test_check_empty_response(caplog):
    """Test that check returns False when get_recovery returns an empty response"""
    caplog.set_level(logging.DEBUG)
    client = MagicMock()
    restore = Restore(client=client, index_list=['index1', 'index2'])
    restore.get_recovery = MagicMock(return_value={})
    assert not restore.check()
    assert '_recovery API returned an empty response. Trying again.' in caplog.text


def test_check_partial_recovery(caplog):
    """Test that check returns False when not all indices are recovered"""
    caplog.set_level(logging.DEBUG)
    client = MagicMock()
    restore = Restore(client=client, index_list=['index1', 'index2'])
    restore.get_recovery = MagicMock(
        return_value={
            'index1': {'shards': [{'stage': 'DONE'}]},
            'index2': {'shards': [{'stage': 'INIT'}]},
        }
    )
    assert not restore.check()
    assert 'Index index2 is still in stage INIT' in caplog.text
