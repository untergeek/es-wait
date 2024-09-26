"""Unit tests for Index"""

import pytest
from es_wait import Index

FAKE_FAIL = Exception('Simulated Failure')

# pylint: disable=W0104


class TestIndex:
    """TestIndex

    Test Index class
    """

    def test_key_value_match(self, client, indexhc, idx):
        """test_key_value_match
        Should return ``True`` when matching keyword args are passed.
        """
        indexhc()
        hc = Index(client, action='health', index=idx)
        assert hc.check

    def test_key_value_negative(self, client, indexhc, idx):
        """test_key_value_negative
        Should return ``False`` when a negative response value is found
        """
        indexhc([{"health": "red"}])
        hc = Index(client, action='health', index=idx)
        assert not hc.check

    def test_resolve_negative(self, client, indexhc, idx):
        """test_resolve_negative
        Should raise ``ValueError` when the index does not resolve
        """
        tval = {'indices': [{'name': 'nomatch'}], 'aliases': [], 'data_streams': []}
        indexhc([{"health": "green"}], tval)
        with pytest.raises(ValueError, match=r'does not resolve to itself'):
            _ = Index(client, action='health', index=idx)

    def test_resolve_no_indices(self, client, indexhc, idx):
        """test_resolve_no_indices
        Should raise ``ValueError`` when there are no indices in response
        """
        tval = {'indices': [], 'aliases': [{'name': idx, 'indices': []}]}
        indexhc([{"health": "green"}], tval)
        with pytest.raises(ValueError, match=r'resolves to zero indices'):
            _ = Index(client, action='health', index=idx)

    def test_resolve_multiple(self, client, indexhc, idx):
        """test_resolve_multiple
        Should raise ``ValueError` when there are no indices in response
        """
        tval = {'indices': [{'name': 'nomatch1'}, {'name': 'nomatch2'}]}
        indexhc([{"health": "green"}], tval)
        with pytest.raises(ValueError, match=r'resolves to more than one index'):
            _ = Index(client, action='health', index=idx)

    def test_unacceptable_action(self, client, indexhc, idx):
        """test_unacceptable_action
        Should raise ``ValueError`` when an incorrect action is passed
        """
        indexhc()
        hc = Index(client, action='NOTFOUND', index=idx)
        with pytest.raises(ValueError, match=r'is not an acceptable value for action'):
            hc.argmap()

    def test_key_not_found(self, client, indexhc, idx):
        """test_key_not_found
        Should raise KeyError when key is not in client.cluster.health output
        """
        indexhc([{'not': 'found'}])
        hc = Index(client, action='health', index=idx)
        with pytest.raises(KeyError, match=r'not in index health output'):
            hc.check
