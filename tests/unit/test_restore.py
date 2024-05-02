"""Unit tests for Restore"""

import pytest
from es_wait import Restore


class TestRestore:
    """Test Restore class"""

    def test_fail_to_get_recovery(self, client, fake_fail, named_indices):
        """
        Should raise ``ValueError`` when an upstream Exception is encountered
        """
        client.indices.recovery.side_effect = fake_fail
        rc = Restore(client, index_list=named_indices)
        with pytest.raises(ValueError, match=r'Unable to obtain recovery information'):
            # pylint: disable=W0104
            rc.check

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
