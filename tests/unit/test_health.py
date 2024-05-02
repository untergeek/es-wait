"""Unit tests for Health"""

import pytest
from es_wait import Health

FAKE_FAIL = Exception('Simulated Failure')


class TestHealth:
    """TestHealth

    Test Health class
    """

    def test_key_value_match(self, client, healthchk):
        """test_key_value_match
        Should return ``True`` when matching keyword args are passed.
        """
        healthchk()
        hc = Health(client, action='allocation')
        assert hc.check

    def test_key_value_negative(self, client, healthchk):
        """test_key_value_negative
        Should return ``False`` when a negative response value is found
        """
        healthchk({"status": "red"})
        hc = Health(client, action='replicas')
        assert not hc.check

    def test_unacceptable_action(self, client, healthchk):
        """test_unacceptable_action
        Should raise ``ValueError`` when an incorrect action is passed
        """
        healthchk()
        hc = Health(client, action='NOTFOUND')
        with pytest.raises(ValueError, match=r'is not an acceptable value for action'):
            hc.argmap()

    def test_key_not_found(self, client, healthchk):
        """test_key_not_found
        Should raise KeyError when key is not in client.cluster.health output
        """
        healthchk({'not': 'found'})
        hc = Health(client, action='shrink')
        with pytest.raises(KeyError, match=r'not in cluster health output'):
            # pylint: disable=W0104
            hc.check
