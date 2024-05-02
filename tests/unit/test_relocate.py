"""Unit tests for Relocate"""

import pytest
from es_wait import Relocate


class TestRelocate:
    """Test Relocate class"""

    def test_fail_to_get_state(self, client, fake_fail):
        """
        Should raise ``ValueError`` when an upstream Exception is encountered
        """
        client.cluster.state.side_effect = fake_fail
        rc = Relocate(client, name='arbitrary')
        with pytest.raises(ValueError, match=r'Unable to get routing table data'):
            # pylint: disable=W0104
            rc.check

    def test_key_error(self, client):
        """
        Should raise ``KeyError`` when the result does not contain expected keys
        """
        found = 'found'
        expected = 'expected'
        missing = {'routing_table': {'indices': {found: {'shards': {}}}}}
        client.cluster.state.return_value = missing
        rc = Relocate(client, name=expected)
        with pytest.raises(KeyError):
            # pylint: disable=W0104
            rc.check

    def test_relocate_success(self, relocate_test):
        """Should return ``True`` when all shards are started"""
        assert relocate_test(state='STARTED', result=True)

    def test_relocate_incomplete(self, relocate_test):
        """Should return ``False`` when some shards are not 'STARTED'"""
        assert relocate_test(state='RELOCATING', result=False)

    def test_relocate_random(self, relocate_test):
        """
        Should return ``False`` when not all shards are not 'STARTED'
        Testing on large count of shards, randomized results
        """
        assert relocate_test(state='random', count=20, result=False)

    # def test_empty_recovery(self, relocate_test):
    #     """Should return ``False`` when an empty response comes back"""
    #     assert relocate_test({}, False)

    # def test_chunker(self, relocate_test):
    #     """Ensure that very long lists of indices are properly chunked"""
    #     assert relocate_test('DONE', True, chunktest=True)
